package dfsha;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Arranca el servidor Raft de este ControlNode y expone dos operaciones al
 * resto de la aplicacion:
 *
 *   write(op, args) -> el comando se replica en los 3 nodos (quorum 2 de 3)
 *   read(op, args)  -> la consulta la responde el lider (lectura linealizable)
 *
 * El cliente de Ratis se encarga solo de buscar al lider, asi que da igual a
 * cual de los 3 ControlNodes le pegue el cliente Python.
 */
@Component
public class RaftService {

    private static final Logger log = LoggerFactory.getLogger(RaftService.class);

    private final MetadataStateMachine stateMachine;
    private final ObjectMapper json = new ObjectMapper();

    @Value("${dfsha.node-id}")
    private String nodeId;
    @Value("${dfsha.raft.peers}")
    private String peersConfig;
    @Value("${dfsha.raft.group-id}")
    private String groupIdConfig;
    @Value("${dfsha.storage-dir}")
    private String storageDir;

    private RaftServer server;
    private RaftClient client;
    private RaftGroup group;
    private List<RaftPeer> peers;

    public RaftService(MetadataStateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    // ===================== arranque =====================

    @EventListener(ApplicationReadyEvent.class)
    public void start() throws Exception {
        peers = parsePeers(peersConfig);
        RaftPeer yo = peers.stream().filter(p -> p.getId().toString().equals(nodeId)).findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "el nodo '" + nodeId + "' no aparece en dfsha.raft.peers"));

        group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString(groupIdConfig)), peers);

        File dirRaft = new File(storageDir + "/" + nodeId + "/raft");
        // Si ya hay un log de Raft de una ejecucion anterior hay que RECUPERARLO;
        // solo la primera vez se formatea el almacenamiento.
        String[] contenido = dirRaft.list();
        RaftStorage.StartupOption opcion = (contenido != null && contenido.length > 0)
                ? RaftStorage.StartupOption.RECOVER
                : RaftStorage.StartupOption.FORMAT;

        RaftProperties props = new RaftProperties();
        org.apache.ratis.RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setPort(props, puertoDe(yo));
        RaftServerConfigKeys.setStorageDir(props, List.of(dirRaft));
        // Eleccion de lider rapida: el README pide un lider nuevo en menos de 5 s
        RaftServerConfigKeys.Rpc.setTimeoutMin(props, TimeDuration.valueOf(1, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(props, TimeDuration.valueOf(2, TimeUnit.SECONDS));

        server = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(nodeId))
                .setGroup(group)
                .setProperties(props)
                .setStateMachine(stateMachine)
                .setOption(opcion)
                .build();
        server.start();

        client = RaftClient.newBuilder()
                .setProperties(props)
                .setRaftGroup(group)
                .build();

        log.info("ControlNode {} escuchando Raft en {} ({} peers)", nodeId, yo.getAddress(), peers.size());
    }

    @PreDestroy
    public void stop() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    // ===================== operaciones =====================

    /** Comando que cambia los metadatos: pasa por el log de Raft. */
    public Object write(String op, Map<String, Object> args) {
        return enviar(op, args, true);
    }

    /** Consulta: la responde el lider sin escribir en el log. */
    public Object read(String op, Map<String, Object> args) {
        return enviar(op, args, false);
    }

    @SuppressWarnings("unchecked")
    private Object enviar(String op, Map<String, Object> args, boolean escritura) {
        if (client == null) throw ApiException.unavailable("el ControlNode todavia esta arrancando");
        try {
            Map<String, Object> peticion = new LinkedHashMap<>();
            peticion.put("op", op);
            peticion.put("args", args);
            Message mensaje = Message.valueOf(json.writeValueAsString(peticion));

            RaftClientReply reply = escritura ? client.io().send(mensaje) : client.io().sendReadOnly(mensaje);
            if (!reply.isSuccess()) {
                throw ApiException.unavailable("Raft no pudo procesar la operacion: " + reply.getException());
            }
            Map<String, Object> respuesta = json.readValue(
                    reply.getMessage().getContent().toStringUtf8(), new TypeReference<Map<String, Object>>() {});
            if (!Boolean.TRUE.equals(respuesta.get("ok"))) {
                int status = respuesta.get("status") == null ? 400 : ((Number) respuesta.get("status")).intValue();
                throw new ApiException(status, String.valueOf(respuesta.get("error")));
            }
            return respuesta.get("data");
        } catch (ApiException e) {
            throw e;
        } catch (Exception e) {
            throw ApiException.unavailable("no se pudo contactar al lider del cluster: " + e.getMessage());
        }
    }

    // ===================== estado del cluster =====================

    public boolean esLider() {
        try {
            return server != null && server.getDivision(group.getGroupId()).getInfo().isLeader();
        } catch (Exception e) {
            return false;
        }
    }

    public Map<String, Object> estado() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("node_id", nodeId);
        try {
            RaftServer.Division d = server.getDivision(group.getGroupId());
            m.put("rol", d.getInfo().getCurrentRole().name());
            m.put("es_lider", d.getInfo().isLeader());
            m.put("term", d.getInfo().getCurrentTerm());
            m.put("indice_aplicado", d.getInfo().getLastAppliedIndex());
        } catch (Exception e) {
            m.put("rol", "ARRANCANDO");
            m.put("es_lider", false);
        }
        List<String> lista = new ArrayList<>();
        for (RaftPeer p : peers) lista.add(p.getId() + " -> " + p.getAddress());
        m.put("controlnodes", lista);
        return m;
    }

    // ===================== configuracion =====================

    /** Formato de dfsha.raft.peers:  n1|host:puerto,n2|host:puerto,n3|host:puerto */
    private static List<RaftPeer> parsePeers(String config) {
        List<RaftPeer> lista = new ArrayList<>();
        for (String trozo : config.split(",")) {
            String[] partes = trozo.trim().split("\\|");
            if (partes.length != 2) throw new IllegalArgumentException("peer mal escrito: " + trozo);
            lista.add(RaftPeer.newBuilder().setId(partes[0].trim()).setAddress(partes[1].trim()).build());
        }
        return lista;
    }

    private static int puertoDe(RaftPeer peer) {
        String dir = peer.getAddress();
        return Integer.parseInt(dir.substring(dir.lastIndexOf(':') + 1));
    }
}
