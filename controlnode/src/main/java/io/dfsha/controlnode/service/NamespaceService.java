package io.dfsha.controlnode.service;

import io.dfsha.controlnode.api.DfshaException;
import io.dfsha.controlnode.metadata.InMemoryMetadataStore;
import io.dfsha.controlnode.metadata.MetadataStore;
import io.dfsha.controlnode.model.FileVersion;
import io.dfsha.controlnode.model.INode;
import io.dfsha.controlnode.security.Principal;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/** Operaciones del espacio de nombres: ls, mkdir, rm, mv, stat, y control de acceso ACL. */
@Service
public class NamespaceService {

    private final MetadataStore store;
    private final BlockManager blocks;

    public NamespaceService(MetadataStore store, BlockManager blocks) {
        this.store = store;
        this.blocks = blocks;
    }

    // -------------------------------------------------------------- permisos

    public static boolean canRead(INode node, Principal p) {
        if (p.isAdmin() || node.owner.equals(p.username())) return node.permissions.charAt(0) == 'r' || p.isAdmin();
        return node.permissions.charAt(6) == 'r';
    }

    public static boolean canWrite(INode node, Principal p) {
        if (p.isAdmin()) return true;
        if (node.owner.equals(p.username())) return node.permissions.charAt(1) == 'w';
        return node.permissions.charAt(7) == 'w';
    }

    public static void requireRead(INode node, Principal p) {
        if (!canRead(node, p)) throw DfshaException.forbidden("Sin permiso de lectura sobre " + node.path);
    }

    public static void requireWrite(INode node, Principal p) {
        if (!canWrite(node, p)) throw DfshaException.forbidden("Sin permiso de escritura sobre " + node.path);
    }

    // ------------------------------------------------------------ navegación

    public INode require(String path, Principal p) {
        INode node = store.resolve(path);
        if (node == null) throw DfshaException.notFound(InMemoryMetadataStore.normalize(path));
        requireRead(node, p);
        return node;
    }

    public record Entry(String name, String path, String type, long size, String owner, String group,
                        String permissions, long modifiedAt, Integer blocks, Integer blockSize) {}

    public List<Entry> list(String path, Principal p) {
        return store.read(() -> {
            INode node = require(path, p);
            List<Entry> out = new ArrayList<>();
            if (!node.isDir()) {
                out.add(toEntry(node));
                return out;
            }
            node.children.values().forEach(child -> out.add(toEntry(child)));
            return out;
        });
    }

    public Entry stat(String path, Principal p) {
        return store.read(() -> toEntry(require(path, p)));
    }

    private static Entry toEntry(INode n) {
        FileVersion v = n.current;
        return new Entry(n.name, n.path, n.isDir() ? "dir" : "file", n.size(), n.owner, n.group,
                n.permissions, n.modifiedAt,
                v == null ? null : v.blocks.size(),
                v == null ? null : v.blockSize);
    }

    // -------------------------------------------------------------- mutación

    public Entry mkdir(String path, boolean parents, Principal p) {
        String target = InMemoryMetadataStore.normalize(path);
        if (target.equals("/")) throw DfshaException.badRequest("La raíz ya existe");
        return store.write(() -> {
            INode existing = store.resolve(target);
            if (existing != null) {
                if (parents && existing.isDir()) return toEntry(existing);
                throw DfshaException.conflict("ALREADY_EXISTS", "Ya existe: " + target);
            }
            INode node = createDirectories(target, parents, p);
            return toEntry(node);
        });
    }

    /** Crea el directorio (y sus padres si se pide). Debe llamarse dentro de store.write(). */
    INode createDirectories(String target, boolean parents, Principal p) {
        String[] parts = target.substring(1).split("/");
        INode current = store.root();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            sb.append('/').append(parts[i]);
            INode child = current.children.get(parts[i]);
            if (child == null) {
                boolean last = i == parts.length - 1;
                if (!last && !parents) throw DfshaException.notFound(sb.toString());
                requireWrite(current, p);
                child = new INode(parts[i], sb.toString(), INode.Type.DIR, p.username(),
                        p.username(), "rwxr-x---");
                current.children.put(parts[i], child);
                current.modifiedAt = System.currentTimeMillis();
            } else if (!child.isDir()) {
                throw DfshaException.conflict("NOT_A_DIRECTORY", sb + " no es un directorio");
            }
            current = child;
        }
        return current;
    }

    public void delete(String path, boolean recursive, Principal p) {
        String target = InMemoryMetadataStore.normalize(path);
        if (target.equals("/")) throw DfshaException.badRequest("No se puede borrar la raíz");
        store.write(() -> {
            INode node = store.resolve(target);
            if (node == null) throw DfshaException.notFound(target);
            requireWrite(node, p);
            if (node.isDir() && !node.children.isEmpty() && !recursive) {
                throw DfshaException.conflict("NOT_EMPTY", "El directorio no está vacío: " + target);
            }
            if (!node.isDir() && node.lease != null && !node.lease.expired()
                    && !node.lease.owner.equals(p.username())) {
                throw DfshaException.conflict("LOCKED", "Archivo bloqueado por " + node.lease.owner);
            }
            List<String> blockIds = new ArrayList<>();
            collectBlocks(node, blockIds);
            INode parent = store.resolve(parentOf(target));
            parent.children.remove(node.name);
            parent.modifiedAt = System.currentTimeMillis();
            blocks.scheduleDeletion(blockIds);
            return null;
        });
    }

    private void collectBlocks(INode node, List<String> out) {
        if (node.isDir()) {
            node.children.values().forEach(c -> collectBlocks(c, out));
            return;
        }
        node.history.forEach(v -> {
            v.blocks.values().forEach(b -> out.add(b.blockId));
            store.removeVersion(v.versionId);
        });
        if (node.current != null) {
            node.current.blocks.values().forEach(b -> out.add(b.blockId));
            store.removeVersion(node.current.versionId);
        }
    }

    public Entry move(String src, String dst, Principal p) {
        String from = InMemoryMetadataStore.normalize(src);
        String to = InMemoryMetadataStore.normalize(dst);
        if (to.equals(from) || to.startsWith(from + "/")) {
            throw DfshaException.badRequest("Destino inválido");
        }
        return store.write(() -> {
            INode node = store.resolve(from);
            if (node == null) throw DfshaException.notFound(from);
            requireWrite(node, p);
            INode destination = store.resolve(to);
            String newName;
            INode newParent;
            if (destination != null && destination.isDir()) {
                newParent = destination;
                newName = node.name;
                if (newParent.children.containsKey(newName)) {
                    throw DfshaException.conflict("ALREADY_EXISTS", "Ya existe: " + to + "/" + newName);
                }
            } else if (destination != null) {
                throw DfshaException.conflict("ALREADY_EXISTS", "Ya existe: " + to);
            } else {
                newParent = store.resolve(parentOf(to));
                if (newParent == null || !newParent.isDir()) throw DfshaException.notFound(parentOf(to));
                newName = to.substring(to.lastIndexOf('/') + 1);
            }
            requireWrite(newParent, p);
            INode oldParent = store.resolve(parentOf(from));
            oldParent.children.remove(node.name);
            node.name = newName;
            newParent.children.put(newName, node);
            reparent(node, newParent.path.equals("/") ? "" : newParent.path);
            newParent.modifiedAt = System.currentTimeMillis();
            return toEntry(node);
        });
    }

    private void reparent(INode node, String parentPath) {
        node.path = parentPath + "/" + node.name;
        if (node.current != null) node.current.path = node.path;
        if (node.isDir()) node.children.values().forEach(c -> reparent(c, node.path));
    }

    // -------------------------------------------------------- inicialización

    void bootstrapDirectories(String admin) {
        Principal root = new Principal(admin, "admin");
        if (store.resolve("/home") == null) createDirectories("/home", true, root);
        if (store.resolve("/shared") == null) {
            INode shared = createDirectories("/shared", true, root);
            shared.permissions = "rwxrwxrwx";   // espacio común para todos los usuarios
        }
        createHome(admin);
    }

    void createHome(String username) {
        String home = "/home/" + username;
        if (store.resolve(home) != null) return;
        INode node = createDirectories(home, true, new Principal("admin", "admin"));
        node.owner = username;
        node.group = username;
        node.permissions = "rwx------";         // privado por defecto
    }

    public static String parentOf(String path) {
        String normalized = InMemoryMetadataStore.normalize(path);
        int i = normalized.lastIndexOf('/');
        return i <= 0 ? "/" : normalized.substring(0, i);
    }
}
