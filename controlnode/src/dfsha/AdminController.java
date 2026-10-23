package dfsha;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Administracion y monitoreo (RF5) + la puerta de entrada de los DataNodes.
 *
 * OJO: el registro y los heartbeats de los DataNodes van a ir por gRPC con
 * mTLS (proto/dfsha.proto). Mientras el contrato .proto se acuerda con el
 * equipo de DataNodes, estan expuestos aqui como REST con exactamente los
 * mismos campos, para poder probar el sistema completo.
 */
@RestController
public class AdminController {

    private final RaftService raft;
    private final DataNodes datanodes;

    @Value("${dfsha.node-id}")
    private String nodeId;
    @Value("${dfsha.block-size}")
    private long blockSize;
    @Value("${dfsha.replication}")
    private int replicacion;
    @Value("${dfsha.controlnodes}")
    private String controlnodes;

    public AdminController(RaftService raft, DataNodes datanodes) {
        this.raft = raft;
        this.datanodes = datanodes;
    }

    // ===================== monitoreo =====================

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("estado", "ok");
        m.put("node_id", nodeId);
        m.put("es_lider", raft.esLider());
        m.put("datanodes_vivos", datanodes.vivos().size());
        return m;
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("raft", raft.estado());
        m.put("datanodes", datanodes.estado());
        try {
            m.put("metadatos", raft.read("summary", Map.of()));
        } catch (ApiException e) {
            m.put("metadatos", "no disponible: " + e.getMessage());
        }
        return m;
    }

    @GetMapping("/datanodes")
    public Object listarDatanodes() {
        return datanodes.estado();
    }

    @GetMapping("/admin/under-replicated")
    public Object subReplicados() {
        return datanodes.subReplicados();
    }
}
    