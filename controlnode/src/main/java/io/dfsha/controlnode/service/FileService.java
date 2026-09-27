package io.dfsha.controlnode.service;

import io.dfsha.controlnode.api.DfshaException;
import io.dfsha.controlnode.config.ControlNodeProperties;
import io.dfsha.controlnode.metadata.InMemoryMetadataStore;
import io.dfsha.controlnode.metadata.MetadataStore;
import io.dfsha.controlnode.model.BlockMeta;
import io.dfsha.controlnode.model.DataNodeInfo;
import io.dfsha.controlnode.model.FileVersion;
import io.dfsha.controlnode.model.INode;
import io.dfsha.controlnode.model.Lease;
import io.dfsha.controlnode.security.BlockTokenService;
import io.dfsha.controlnode.security.Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Ciclo de vida de un archivo: create -> allocateBlock* -> commit (o abort),
 * y open para lectura. Cada escritura produce una versión nueva que sólo se
 * hace visible al confirmarla (copy-on-write + publicación atómica).
 */
@Service
public class FileService {

    private static final Logger log = LoggerFactory.getLogger(FileService.class);
    private static final int VERSIONS_TO_KEEP = 2;

    private final MetadataStore store;
    private final NamespaceService namespace;
    private final BlockManager blocks;
    private final BlockTokenService tokens;
    private final ControlNodeProperties props;

    public FileService(MetadataStore store, NamespaceService namespace, BlockManager blocks,
                       BlockTokenService tokens, ControlNodeProperties props) {
        this.store = store;
        this.namespace = namespace;
        this.blocks = blocks;
        this.tokens = tokens;
        this.props = props;
    }

    // ------------------------------------------------------------------ DTOs

    public record NodeRef(String nodeId, String address, String zone) {}

    public record CreateResult(String fileId, String versionId, String leaseId,
                               int blockSize, String path) {}

    public record AllocatedBlock(String blockId, int index, List<NodeRef> pipeline, String token) {}

    public record CommittedBlock(int index, String blockId, long size, String sha256) {}

    public record CommitResult(String path, String versionId, long size, int blockCount) {}

    public record OpenBlock(int index, String blockId, long size, String sha256,
                            List<NodeRef> replicas, String token) {}

    public record OpenResult(String path, String fileId, String versionId, long size,
                             int blockSize, List<OpenBlock> blocks) {}

    // ---------------------------------------------------------------- crear

    public CreateResult create(String path, boolean overwrite, Principal p) {
        String target = InMemoryMetadataStore.normalize(path);
        return store.write(() -> {
            INode existing = store.resolve(target);
            INode file;
            if (existing != null) {
                if (existing.isDir()) throw DfshaException.conflict("IS_A_DIRECTORY", target + " es un directorio");
                if (!overwrite) throw DfshaException.conflict("ALREADY_EXISTS", "Ya existe: " + target);
                NamespaceService.requireWrite(existing, p);
                // Un solo escritor por archivo: cualquier lease vigente bloquea la escritura,
                // incluso del mismo usuario desde otro cliente. El lease expira solo.
                if (existing.lease != null && !existing.lease.expired()) {
                    long secondsLeft = (existing.lease.expiresAt - System.currentTimeMillis()) / 1000;
                    throw DfshaException.conflict("LOCKED",
                            "Otro cliente está escribiendo el archivo (lease de " + existing.lease.owner
                                    + ", expira en " + secondsLeft + " s)");
                }
                file = existing;
            } else {
                INode parent = store.resolve(NamespaceService.parentOf(target));
                if (parent == null || !parent.isDir()) {
                    throw DfshaException.notFound(NamespaceService.parentOf(target));
                }
                NamespaceService.requireWrite(parent, p);
                String name = target.substring(target.lastIndexOf('/') + 1);
                file = new INode(name, target, INode.Type.FILE, p.username(), p.username(), "rw-r-----");
                file.fileId = UUID.randomUUID().toString();
                parent.children.put(name, file);
                parent.modifiedAt = System.currentTimeMillis();
            }

            FileVersion version = new FileVersion(UUID.randomUUID().toString(), file.fileId,
                    target, props.blockSize(), p.username());
            store.putVersion(version);
            file.lease = new Lease(UUID.randomUUID().toString(), p.username(), version.versionId,
                    props.leaseTtlSeconds() * 1000);
            log.info("Versión borrador {} creada para {} por {}", version.versionId, target, p.username());
            return new CreateResult(file.fileId, version.versionId, file.lease.leaseId,
                    version.blockSize, target);
        });
    }

    // -------------------------------------------------------- asignar bloque

    public AllocatedBlock allocateBlock(String versionId, int index, Principal p) {
        FileVersion version = store.write(() -> {
            FileVersion v = requireDraft(versionId, p);
            BlockMeta block = new BlockMeta(UUID.randomUUID().toString(), index);
            v.blocks.put(index, block);
            return v;
        });
        BlockMeta block = version.blocks.get(index);
        List<DataNodeInfo> pipeline = blocks.choosePipeline(props.replicationFactor());
        List<NodeRef> refs = new ArrayList<>();
        pipeline.forEach(n -> refs.add(new NodeRef(n.nodeId, n.address, n.zone)));
        return new AllocatedBlock(block.blockId, index, refs,
                tokens.issue(block.blockId, p.username(), BlockTokenService.MODE_WRITE));
    }

    // ---------------------------------------------------------------- commit

    public CommitResult commit(String versionId, List<CommittedBlock> reported, Principal p) {
        // Se comprueba el quórum antes de tomar el lock de escritura de metadatos.
        for (CommittedBlock b : reported) {
            int replicas = blocks.replicasFor(b.blockId()).size();
            if (replicas < props.minReplicas()) {
                throw DfshaException.conflict("NOT_ENOUGH_REPLICAS",
                        "El bloque " + b.index() + " sólo tiene " + replicas + " réplica(s); se exigen "
                                + props.minReplicas());
            }
        }
        return store.write(() -> {
            FileVersion version = requireDraft(versionId, p);
            INode file = store.resolve(version.path);
            if (file == null) throw DfshaException.notFound(version.path);

            long total = 0;
            version.blocks.clear();
            for (CommittedBlock b : reported) {
                BlockMeta meta = new BlockMeta(b.blockId(), b.index());
                meta.size = b.size();
                meta.sha256 = b.sha256();
                version.blocks.put(b.index(), meta);
                blocks.track(meta);
                total += b.size();
            }
            version.size = total;
            version.state = FileVersion.State.COMMITTED;
            version.committedAt = System.currentTimeMillis();

            // Publicación atómica: la ruta pasa a apuntar a la nueva versión.
            if (file.current != null) file.history.add(file.current);
            file.current = version;
            file.modifiedAt = System.currentTimeMillis();
            file.lease = null;

            garbageCollectOldVersions(file);
            log.info("Versión {} confirmada: {} ({} bloques, {} bytes)",
                    versionId, version.path, version.blocks.size(), total);
            return new CommitResult(version.path, versionId, total, version.blocks.size());
        });
    }

    /** Conserva las últimas versiones y manda borrar los bloques que ya nadie usa. */
    private void garbageCollectOldVersions(INode file) {
        while (file.history.size() > VERSIONS_TO_KEEP) {
            FileVersion old = file.history.remove(0);
            List<String> orphan = new ArrayList<>();
            for (BlockMeta b : old.blocks.values()) {
                boolean stillUsed = file.current.blocks.values().stream()
                        .anyMatch(cur -> cur.blockId.equals(b.blockId))
                        || file.history.stream().anyMatch(v -> v.blocks.values().stream()
                        .anyMatch(x -> x.blockId.equals(b.blockId)));
                if (!stillUsed) orphan.add(b.blockId);
            }
            store.removeVersion(old.versionId);
            if (!orphan.isEmpty()) blocks.scheduleDeletion(orphan);
        }
    }

    public void abort(String versionId, Principal p) {
        store.write(() -> {
            FileVersion version = store.getVersion(versionId);
            if (version == null) throw DfshaException.notFound("versión " + versionId);
            if (!version.createdBy.equals(p.username()) && !p.isAdmin()) {
                throw DfshaException.forbidden("La versión pertenece a " + version.createdBy);
            }
            version.state = FileVersion.State.ABORTED;
            List<String> orphan = version.blocks.values().stream().map(b -> b.blockId).toList();
            blocks.scheduleDeletion(orphan);
            store.removeVersion(versionId);
            INode file = store.resolve(version.path);
            if (file != null && file.lease != null && file.lease.versionId.equals(versionId)) {
                file.lease = null;
                if (file.current == null) {  // archivo nuevo que nunca llegó a confirmarse
                    INode parent = store.resolve(NamespaceService.parentOf(version.path));
                    if (parent != null) parent.children.remove(file.name);
                }
            }
            log.info("Versión {} abortada ({} bloques liberados)", versionId, orphan.size());
            return null;
        });
    }

    // ------------------------------------------------------------------ open

    public OpenResult open(String path, Principal p) {
        return store.read(() -> {
            INode file = namespace.require(path, p);
            if (file.isDir()) throw DfshaException.conflict("IS_A_DIRECTORY", path + " es un directorio");
            if (file.current == null) throw DfshaException.notFound("contenido de " + path);
            FileVersion v = file.current;
            List<OpenBlock> out = new ArrayList<>();
            for (BlockMeta b : v.blocks.values()) {
                List<NodeRef> replicas = new ArrayList<>();
                blocks.replicasFor(b.blockId).forEach(n -> replicas.add(new NodeRef(n.nodeId, n.address, n.zone)));
                if (replicas.isEmpty()) {
                    throw new DfshaException(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,
                            "NO_REPLICA_AVAILABLE", "El bloque " + b.index + " no tiene réplicas vivas");
                }
                out.add(new OpenBlock(b.index, b.blockId, b.size, b.sha256, replicas,
                        tokens.issue(b.blockId, p.username(), BlockTokenService.MODE_READ)));
            }
            return new OpenResult(v.path, file.fileId, v.versionId, v.size, v.blockSize, out);
        });
    }

    // --------------------------------------------------------------- helpers

    private FileVersion requireDraft(String versionId, Principal p) {
        FileVersion version = store.getVersion(versionId);
        if (version == null) throw DfshaException.notFound("versión " + versionId);
        if (version.state != FileVersion.State.DRAFT) {
            throw DfshaException.conflict("NOT_A_DRAFT", "La versión ya no es un borrador");
        }
        if (!version.createdBy.equals(p.username()) && !p.isAdmin()) {
            throw DfshaException.forbidden("La versión pertenece a " + version.createdBy);
        }
        INode file = store.resolve(version.path);
        if (file != null && file.lease != null && file.lease.expired()) {
            throw DfshaException.conflict("LEASE_EXPIRED", "El lease de escritura expiró; reinicie la escritura");
        }
        return version;
    }
}
