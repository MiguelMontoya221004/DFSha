package dfsha;

import dfsha.grpc.Ack;
import dfsha.grpc.BlockInfo;
import dfsha.grpc.BlockReceivedRequest;
import dfsha.grpc.BlockRemovedRequest;
import dfsha.grpc.BlockReportRequest;
import dfsha.grpc.Command;
import dfsha.grpc.ControlServiceGrpc;
import dfsha.grpc.DeleteBlock;
import dfsha.grpc.HeartbeatReply;
import dfsha.grpc.HeartbeatRequest;
import dfsha.grpc.RegisterReply;
import dfsha.grpc.RegisterRequest;
import dfsha.grpc.ReplicateBlock;
import dfsha.grpc.Target;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Servidor gRPC con el que hablan los DataNodes (contrato: dfsha.proto).
 *
 * Es la misma logica que ya atienden los endpoints REST /datanodes/*, que se
 * dejan funcionando en paralelo mientras el equipo de DataNodes migra a gRPC.
 * Los dos caminos terminan en DataNodes.java y en RaftService.
 *
 * Recordar del .proto: el DataNode le manda el heartbeat a LOS TRES
 * ControlNodes, y los comandos (re-replicar, borrar) viajan de vuelta en la
 * respuesta del heartbeat, solo si este nodo es el lider.
 */
@Component
public class GrpcService extends ControlServiceGrpc.ControlServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(GrpcService.class);

    private final DataNodes datanodes;
    private final RaftService raft;

    @Value("${dfsha.grpc-port}")
    private int grpcPort;
    @Value("${dfsha.block-size}")
    private long blockSize;
    @Value("${dfsha.replication}")
    private int replicacion;
    @Value("${dfsha.controlnodes}")
    private String controlnodes;
    @Value("${dfsha.raft.group-id}")
    private String clusterId;

    private Server server;

    public GrpcService(DataNodes datanodes, RaftService raft) {
        this.datanodes = datanodes;
        this.raft = raft;
    }

    // ===================== ciclo de vida =====================

    @EventListener(ApplicationReadyEvent.class)
    public void start() throws Exception {
        // Sin TLS por ahora. En el Hito 3 esto se cambia por
        // .sslContext(GrpcSslContexts.forServer(cert, key).trustManager(ca)
        //     .clientAuth(ClientAuth.REQUIRE).build())  para tener mTLS.
        server = NettyServerBuilder.forPort(grpcPort).addService(this).build().start();
        log.info("Servidor gRPC para DataNodes escuchando en el puerto {}", grpcPort);
    }

    @PreDestroy
    public void stop() {
        if (server != null) server.shutdownNow();
    }

    // ===================== registro =====================

    @Override
    public void register(RegisterRequest req, StreamObserver<RegisterReply> obs) {
        try {
            datanodes.registrar(req.getNodeId(), req.getHost(), req.getClientPort(), req.getPeerPort(),
                    req.getZone(), req.getCapacityBytes(), req.getUsedBytes());

            // el registro si queda en los metadatos replicados con Raft
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("node_id", req.getNodeId());
            args.put("host", req.getHost());
            args.put("port", req.getClientPort());
            args.put("zone", req.getZone());
            args.put("capacity", req.getCapacityBytes());
            args.put("now", System.currentTimeMillis());
            raft.write("dn_register", args);

            obs.onNext(RegisterReply.newBuilder()
                    .setAccepted(true)
                    .setClusterId(clusterId)
                    .setHeartbeatSeconds(3)
                    .setBlockSizeBytes(blockSize)
                    .setReplicationFactor(replicacion)
                    .addAllControlnodes(List.of(controlnodes.split(",")))
                    .build());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onNext(RegisterReply.newBuilder().setAccepted(false).setMessage(mensaje(e)).build());
            obs.onCompleted();
        }
    }

    // ===================== heartbeat =====================

    @Override
    public void heartbeat(HeartbeatRequest req, StreamObserver<HeartbeatReply> obs) {
        HeartbeatReply.Builder reply = HeartbeatReply.newBuilder().setIsLeader(raft.esLider());
        try {
            datanodes.heartbeat(req.getNodeId(), req.getUsedBytes(), req.getBlockCount());
        } catch (ApiException e) {
            // no lo conocemos (este ControlNode se reinicio): que se registre
            obs.onNext(reply.setReregister(true).build());
            obs.onCompleted();
            return;
        }
        // solo el lider reparte trabajo
        if (raft.esLider()) {
            for (Map<String, Object> c : datanodes.comandosPendientes(req.getNodeId())) {
                reply.addCommands(aComando(c));
            }
        }
        obs.onNext(reply.build());
        obs.onCompleted();
    }

    /** Pasa un comando de la cola (que es un Map, igual que en REST) a protobuf. */
    @SuppressWarnings("unchecked")
    private Command aComando(Map<String, Object> c) {
        if ("DELETE_BLOCK".equals(c.get("tipo"))) {
            return Command.newBuilder()
                    .setRemove(DeleteBlock.newBuilder().addAllBlockIds((List<String>) c.get("block_ids")))
                    .build();
        }
        ReplicateBlock.Builder r = ReplicateBlock.newBuilder().setBlockId((String) c.get("block_id"));
        for (Map<String, Object> t : (List<Map<String, Object>>) c.get("targets")) {
            r.addTargets(Target.newBuilder()
                    .setNodeId((String) t.get("node_id"))
                    .setHost((String) t.get("host"))
                    .setPeerPort((int) Db.lng(t, "peer_port")));
        }
        return Command.newBuilder().setReplicate(r).build();
    }

    // ===================== bloques =====================

    @Override
    public void blockReceived(BlockReceivedRequest req, StreamObserver<Ack> obs) {
        try {
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("block_id", req.getBlockId());
            args.put("node_id", req.getNodeId());
            raft.write("add_replica", args);
            datanodes.replicaConfirmada(req.getBlockId());
            responder(obs, true, "replica registrada");
        } catch (Exception e) {
            responder(obs, false, mensaje(e));
        }
    }

    @Override
    public void blockRemoved(BlockRemovedRequest req, StreamObserver<Ack> obs) {
        try {
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("block_id", req.getBlockId());
            args.put("node_id", req.getNodeId());
            raft.write("del_replica", args);
            log.warn("El DataNode {} perdio el bloque {} ({})", req.getNodeId(), req.getBlockId(), req.getReason());
            responder(obs, true, "replica dada de baja");
        } catch (Exception e) {
            responder(obs, false, mensaje(e));
        }
    }

    /**
     * El DataNode dice que bloques tiene de verdad en disco y se compara contra
     * los metadatos: lo que el nodo tiene y no estaba anotado se agrega, y lo que
     * estaba anotado pero el nodo ya no tiene se da de baja (solo si el reporte
     * es completo). Despues el chequeo de sub-replicados se encarga de volver a
     * copiar lo que haga falta.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void blockReport(BlockReportRequest req, StreamObserver<Ack> obs) {
        if (!raft.esLider()) {
            responder(obs, true, "no soy el lider, el reporte lo procesa el lider");
            return;
        }
        try {
            Set<String> enElNodo = new HashSet<>();
            for (BlockInfo b : req.getBlocksList()) enElNodo.add(b.getBlockId());

            List<Map<String, Object>> bloques = (List<Map<String, Object>>) raft.read("all_replicas", Map.of());
            Set<String> enLosMetadatos = new HashSet<>();
            int agregadas = 0, bajas = 0;

            for (Map<String, Object> b : bloques) {
                String blockId = (String) b.get("block_id");
                boolean anotado = ((List<String>) b.get("nodes")).contains(req.getNodeId());
                boolean lotiene = enElNodo.contains(blockId);
                enLosMetadatos.add(blockId);

                if (lotiene && !anotado) {
                    raft.write("add_replica", replica(blockId, req.getNodeId()));
                    agregadas++;
                } else if (!lotiene && anotado && req.getFull()) {
                    raft.write("del_replica", replica(blockId, req.getNodeId()));
                    bajas++;
                }
            }
            // Bloques que el DataNode tiene pero que ya no existen en los
            // metadatos (archivos borrados o escrituras que se cancelaron).
            List<String> sobran = new ArrayList<>();
            for (String id : enElNodo) if (!enLosMetadatos.contains(id)) sobran.add(id);
            if (!sobran.isEmpty()) {
                log.warn("El DataNode {} tiene {} bloques que ya no estan en los metadatos", req.getNodeId(), sobran.size());
            }

            responder(obs, true, "reporte procesado: " + agregadas + " replicas agregadas, " + bajas + " dadas de baja");
        } catch (Exception e) {
            responder(obs, false, mensaje(e));
        }
    }

    // ===================== helpers =====================

    private Map<String, Object> replica(String blockId, String nodeId) {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("block_id", blockId);
        args.put("node_id", nodeId);
        return args;
    }

    private void responder(StreamObserver<Ack> obs, boolean ok, String mensaje) {
        obs.onNext(Ack.newBuilder().setOk(ok).setMessage(mensaje).build());
        obs.onCompleted();
    }

    private String mensaje(Exception e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
}
