package io.dfsha.controlnode.api;

import io.dfsha.controlnode.config.ControlNodeProperties;
import io.dfsha.controlnode.model.DataNodeInfo;
import io.dfsha.controlnode.service.BlockManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** RF5: estado del clúster y salud del nodo. */
@RestController
@RequestMapping("/api/v1")
public class ClusterController {

    private final BlockManager blocks;
    private final ControlNodeProperties props;

    public ClusterController(BlockManager blocks, ControlNodeProperties props) {
        this.blocks = blocks;
        this.props = props;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP", "nodeId", props.nodeId(),
                "role", "LEADER", "dataNodesAlive", blocks.aliveNodes());
    }

    @GetMapping("/cluster/status")
    public Map<String, Object> status() {
        List<Map<String, Object>> nodes = blocks.allNodes().stream().map(this::describe).toList();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("controlNode", Map.of("nodeId", props.nodeId(), "clusterId", props.clusterId(),
                "role", "LEADER", "raft", "pendiente (Hito 3)"));
        out.put("replicationFactor", props.replicationFactor());
        out.put("minReplicas", props.minReplicas());
        out.put("blockSize", props.blockSize());
        out.put("dataNodes", nodes);
        out.put("trackedBlocks", blocks.trackedBlocks());
        out.put("underReplicatedBlocks", blocks.underReplicatedBlocks());
        return out;
    }

    private Map<String, Object> describe(DataNodeInfo n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", n.nodeId);
        m.put("address", n.address);
        m.put("zone", n.zone);
        m.put("alive", n.alive);
        m.put("capacityBytes", n.capacityBytes);
        m.put("usedBytes", n.usedBytes);
        m.put("blockCount", n.blockCount);
        m.put("activeTransfers", n.activeTransfers);
        m.put("loadScore", Math.round(n.loadScore() * 1000) / 1000.0);
        m.put("lastHeartbeatMsAgo", System.currentTimeMillis() - n.lastHeartbeat);
        return m;
    }
}
