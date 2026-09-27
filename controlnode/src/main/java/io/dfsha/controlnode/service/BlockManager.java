package io.dfsha.controlnode.service;

import io.dfsha.controlnode.api.DfshaException;
import io.dfsha.controlnode.config.ControlNodeProperties;
import io.dfsha.controlnode.model.BlockMeta;
import io.dfsha.controlnode.model.DataNodeInfo;
import io.dfsha.controlnode.security.BlockTokenService;
import io.dfsha.proto.BlockInfo;
import io.dfsha.proto.Command;
import io.dfsha.proto.DeleteCommand;
import io.dfsha.proto.ReplicateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Gestor del plano de datos:
 *   - registro y vigilancia (heartbeats) de los DataNodes;
 *   - mapa bloque -> réplicas (estado blando, reconstruido con los reportes);
 *   - algoritmo de colocación de réplicas;
 *   - detección de fallos y re-replicación P2P;
 *   - cola de borrado de bloques huérfanos.
 */
@Service
public class BlockManager {

    private static final Logger log = LoggerFactory.getLogger(BlockManager.class);
    private static final long REPLICATION_TIMEOUT_MS = 60_000;
    private static final int MAX_REPLICATIONS_PER_NODE = 2;

    private final ControlNodeProperties props;
    private final BlockTokenService blockTokens;

    /** DataNodes conocidos: nodeId -> estado. */
    private final Map<String, DataNodeInfo> nodes = new ConcurrentHashMap<>();
    /** Estado blando: blockId -> nodeIds que reportaron tener la réplica. */
    private final Map<String, Set<String>> locations = new ConcurrentHashMap<>();
    /** Bloques confirmados que deben mantener el factor de replicación. */
    private final Map<String, BlockMeta> tracked = new ConcurrentHashMap<>();
    /** Comandos pendientes por DataNode, entregados en la respuesta del heartbeat. */
    private final Map<String, ConcurrentLinkedQueue<Command>> pending = new ConcurrentHashMap<>();
    /** Re-replicaciones en curso: blockId -> instante en que se ordenó. */
    private final Map<String, Long> inFlight = new ConcurrentHashMap<>();

    public BlockManager(ControlNodeProperties props, BlockTokenService blockTokens) {
        this.props = props;
        this.blockTokens = blockTokens;
    }

    public record HeartbeatResult(boolean registered, List<Command> commands) {}

    // ------------------------------------------------------------------ nodos

    public DataNodeInfo register(String nodeId, String address, String zone, long capacityBytes) {
        DataNodeInfo info = nodes.computeIfAbsent(nodeId,
                id -> new DataNodeInfo(id, address, zone, capacityBytes));
        info.address = address;
        info.zone = zone;
        info.capacityBytes = capacityBytes;
        info.alive = true;
        info.registeredAt = System.currentTimeMillis();
        info.lastHeartbeat = System.currentTimeMillis();
        // El nodo enviará un reporte completo enseguida: descartamos lo que creíamos saber.
        forgetNodeReplicas(nodeId);
        pending.put(nodeId, new ConcurrentLinkedQueue<>());
        log.info("DataNode registrado: {} en {} (zona {}, capacidad {} MB)",
                nodeId, address, zone, capacityBytes / (1024 * 1024));
        return info;
    }

    public HeartbeatResult heartbeat(String nodeId, long usedBytes, long freeBytes,
                                     int activeTransfers, int blockCount) {
        DataNodeInfo info = nodes.get(nodeId);
        if (info == null) return new HeartbeatResult(false, List.of());
        if (!info.alive) log.info("DataNode {} volvió a estar vivo", nodeId);
        info.alive = true;
        info.usedBytes = usedBytes;
        info.freeBytes = freeBytes;
        info.activeTransfers = activeTransfers;
        info.blockCount = blockCount;
        info.lastHeartbeat = System.currentTimeMillis();

        List<Command> commands = new ArrayList<>();
        ConcurrentLinkedQueue<Command> queue = pending.computeIfAbsent(nodeId, k -> new ConcurrentLinkedQueue<>());
        Command c;
        while (commands.size() < 10 && (c = queue.poll()) != null) commands.add(c);
        return new HeartbeatResult(true, commands);
    }

    public void blockReport(String nodeId, List<BlockInfo> blocks, boolean fullReport) {
        if (fullReport) forgetNodeReplicas(nodeId);
        for (BlockInfo b : blocks) addLocation(b.getBlockId(), nodeId);
        log.debug("Reporte de bloques de {}: {} bloques (completo={})", nodeId, blocks.size(), fullReport);
    }

    public void blockReceived(String nodeId, BlockInfo block) {
        addLocation(block.getBlockId(), nodeId);
        inFlight.remove(block.getBlockId());
        log.debug("DataNode {} confirmó el bloque {}", nodeId, block.getBlockId());
    }

    public void reportCorrupt(String nodeId, String blockId, String detail) {
        log.warn("Bloque corrupto reportado: {} en {} ({})", blockId, nodeId, detail);
        Set<String> set = locations.get(blockId);
        if (set != null) set.remove(nodeId);
        enqueue(nodeId, Command.newBuilder()
                .setDelete(DeleteCommand.newBuilder().addBlockIds(blockId).build()).build());
    }

    private void addLocation(String blockId, String nodeId) {
        locations.computeIfAbsent(blockId, k -> ConcurrentHashMap.newKeySet()).add(nodeId);
    }

    private void forgetNodeReplicas(String nodeId) {
        locations.values().forEach(set -> set.remove(nodeId));
    }

    // --------------------------------------------------------------- bloques

    /** Empieza a vigilar el factor de replicación de un bloque confirmado. */
    public void track(BlockMeta block) { tracked.put(block.blockId, block); }

    public void untrack(Collection<String> blockIds) {
        blockIds.forEach(id -> { tracked.remove(id); inFlight.remove(id); });
    }

    public Set<String> holders(String blockId) {
        return locations.getOrDefault(blockId, Set.of());
    }

    /** Réplicas vivas de un bloque, de menos a más cargada (el cliente lee de la primera). */
    public List<DataNodeInfo> replicasFor(String blockId) {
        return holders(blockId).stream()
                .map(nodes::get)
                .filter(n -> n != null && n.alive)
                .sorted(Comparator.comparingDouble(DataNodeInfo::loadScore))
                .toList();
    }

    /** Ordena a los DataNodes que tengan el bloque que lo borren (recolección de basura). */
    public void scheduleDeletion(Collection<String> blockIds) {
        for (String blockId : blockIds) {
            for (String nodeId : new HashSet<>(holders(blockId))) {
                enqueue(nodeId, Command.newBuilder()
                        .setDelete(DeleteCommand.newBuilder().addBlockIds(blockId).build()).build());
            }
            locations.remove(blockId);
        }
        untrack(blockIds);
    }

    // ------------------------------------------------------------ colocación

    /**
     * Elige los DataNodes destino de un bloque nuevo.
     * Estrategia: "power of two choices" (dos candidatos al azar, gana el menos
     * cargado) exigiendo zonas distintas mientras haya zonas disponibles.
     */
    public List<DataNodeInfo> choosePipeline(int desired) {
        List<DataNodeInfo> pipeline = choose(desired, Set.of(), Set.of());
        if (pipeline.isEmpty()) {
            throw DfshaException.unavailable("No hay DataNodes disponibles con espacio suficiente");
        }
        return pipeline;
    }

    private List<DataNodeInfo> choose(int desired, Set<String> excludeNodes, Set<String> excludeZones) {
        List<DataNodeInfo> alive = nodes.values().stream()
                .filter(n -> n.alive)
                .filter(n -> !excludeNodes.contains(n.nodeId))
                .filter(n -> n.freeBytes <= 0 || n.freeBytes > 2L * props.blockSize())
                .toList();

        List<DataNodeInfo> chosen = new ArrayList<>();
        Set<String> usedZones = new HashSet<>(excludeZones);
        while (chosen.size() < desired) {
            List<DataNodeInfo> candidates = alive.stream()
                    .filter(n -> !chosen.contains(n))
                    .filter(n -> !usedZones.contains(n.zone))
                    .toList();
            if (candidates.isEmpty()) {
                // No quedan zonas nuevas: se relaja el requisito a "host distinto".
                candidates = alive.stream().filter(n -> !chosen.contains(n)).toList();
                if (candidates.isEmpty()) break;
            }
            DataNodeInfo pick = powerOfTwoChoices(candidates);
            chosen.add(pick);
            usedZones.add(pick.zone);
        }
        return chosen;
    }

    private DataNodeInfo powerOfTwoChoices(List<DataNodeInfo> candidates) {
        if (candidates.size() == 1) return candidates.get(0);
        int i = ThreadLocalRandom.current().nextInt(candidates.size());
        int j = ThreadLocalRandom.current().nextInt(candidates.size());
        while (j == i) j = ThreadLocalRandom.current().nextInt(candidates.size());
        DataNodeInfo a = candidates.get(i), b = candidates.get(j);
        return a.loadScore() <= b.loadScore() ? a : b;
    }

    // ------------------------------------------- vigilancia y re-replicación

    /** Marca DataNodes muertos y repone las réplicas que falten. */
    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void monitor() {
        long now = System.currentTimeMillis();
        for (DataNodeInfo n : nodes.values()) {
            if (n.alive && now - n.lastHeartbeat > props.nodeDeadAfterMs()) {
                n.alive = false;
                forgetNodeReplicas(n.nodeId);
                pending.remove(n.nodeId);
                log.warn("DataNode {} declarado MUERTO (sin heartbeat por {} ms)",
                        n.nodeId, now - n.lastHeartbeat);
            }
        }
        inFlight.entrySet().removeIf(e -> now - e.getValue() > REPLICATION_TIMEOUT_MS);
        replicateUnderReplicated();
    }

    private void replicateUnderReplicated() {
        Map<String, Integer> perNode = new ConcurrentHashMap<>();
        // Primero los bloques con menos réplicas (cola de prioridad implícita).
        List<BlockMeta> candidates = tracked.values().stream()
                .filter(b -> !inFlight.containsKey(b.blockId))
                .filter(b -> replicasFor(b.blockId).size() < props.replicationFactor())
                .sorted(Comparator.comparingInt(b -> replicasFor(b.blockId).size()))
                .limit(50)
                .toList();

        for (BlockMeta block : candidates) {
            List<DataNodeInfo> current = replicasFor(block.blockId);
            if (current.isEmpty()) {
                log.error("Bloque {} sin réplicas vivas: no se puede recuperar", block.blockId);
                continue;
            }
            DataNodeInfo source = current.get(0);
            if (perNode.getOrDefault(source.nodeId, 0) >= MAX_REPLICATIONS_PER_NODE) continue;

            Set<String> excludeNodes = new HashSet<>(holders(block.blockId));
            Set<String> excludeZones = new HashSet<>();
            current.forEach(n -> excludeZones.add(n.zone));
            List<DataNodeInfo> targets = choose(1, excludeNodes, excludeZones);
            if (targets.isEmpty()) continue;

            DataNodeInfo target = targets.get(0);
            enqueue(source.nodeId, Command.newBuilder().setReplicate(
                    ReplicateCommand.newBuilder()
                            .setBlockId(block.blockId)
                            .setTargetAddress(target.address)
                            .setBlockToken(blockTokens.issueForNode(block.blockId, source.nodeId))
                            .setSha256(block.sha256 == null ? "" : block.sha256)
                            .setSize(block.size)
                            .build()).build());
            inFlight.put(block.blockId, System.currentTimeMillis());
            perNode.merge(source.nodeId, 1, Integer::sum);
            log.info("Re-replicando bloque {}: {} -> {} (réplicas vivas: {})",
                    block.blockId, source.nodeId, target.nodeId, current.size());
        }
    }

    private void enqueue(String nodeId, Command command) {
        pending.computeIfAbsent(nodeId, k -> new ConcurrentLinkedQueue<>()).add(command);
    }

    // ----------------------------------------------------------- monitoreo

    public Collection<DataNodeInfo> allNodes() { return nodes.values(); }

    public long aliveNodes() { return nodes.values().stream().filter(n -> n.alive).count(); }

    public int trackedBlocks() { return tracked.size(); }

    public List<Map<String, Object>> underReplicatedBlocks() {
        return tracked.values().stream()
                .filter(b -> replicasFor(b.blockId).size() < props.replicationFactor())
                .map(b -> Map.<String, Object>of(
                        "blockId", b.blockId,
                        "replicas", replicasFor(b.blockId).size(),
                        "desired", props.replicationFactor()))
                .toList();
    }
}
