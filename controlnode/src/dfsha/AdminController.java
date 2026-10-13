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

    // ===================== lo que llaman los DataNodes =====================

    @PostMapping("/datanodes/register")
    public Map<String, Object> registrar(@RequestBody Map<String, Object> body) {
        String id = Db.str(body, "node_id");
        String host = Db.str(body, "host");
        int port = (int) Db.lng(body, "port");
        int peerPort = body.get("peer_port") == null ? port : (int) Db.lng(body, "peer_port");
        String zona = body.get("zone") == null ? "default" : body.get("zone").toString();
        long capacidad = body.get("capacity") == null ? 0 : Db.lng(body, "capacity");
        long usado = body.get("used") == null ? 0 : Db.lng(body, "used");

        datanodes.registrar(id, host, port, peerPort, zona, capacidad, usado);

        // el registro si queda en los metadatos replicados
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("node_id", id);
        args.put("host", host);
        args.put("port", port);
        args.put("zone", zona);
        args.put("capacity", capacidad);
        args.put("now", System.currentTimeMillis());
        raft.write("dn_register", args);

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ok", true);
        r.put("heartbeat_segundos", 3);
        r.put("block_size", blockSize);
        r.put("replicas_por_bloque", replicacion);
        // El DataNode debe registrarse y mandar el heartbeat a LOS TRES
        // ControlNodes, no solo al lider: asi cualquiera de los tres sabe que
        // nodos estan vivos y puede responderle al cliente de una vez.
        r.put("controlnodes", List.of(controlnodes.split(",")));
        return r;
    }

    /** Heartbeat cada 3 s. La respuesta trae los comandos pendientes para ese nodo. */
    @PostMapping("/datanodes/heartbeat")
    public Map<String, Object> heartbeat(@RequestBody Map<String, Object> body) {
        String id = Db.str(body, "node_id");
        long usado = body.get("used") == null ? 0 : Db.lng(body, "used");
        int bloques = body.get("blocks") == null ? 0 : (int) Db.lng(body, "blocks");
        datanodes.heartbeat(id, usado, bloques);

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ok", true);
        r.put("es_lider", raft.esLider());
        r.put("comandos", datanodes.comandosPendientes(id));
        return r;
    }

    /** El DataNode avisa que ya tiene una replica (despues de una re-replicacion). */
    @PostMapping("/datanodes/block-received")
    public Object bloqueRecibido(@RequestBody Map<String, Object> body) {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("block_id", Db.str(body, "block_id"));
        args.put("node_id", Db.str(body, "node_id"));
        Object r = raft.write("add_replica", args);
        datanodes.replicaConfirmada(Db.str(body, "block_id"));
        return r;
    }
}
