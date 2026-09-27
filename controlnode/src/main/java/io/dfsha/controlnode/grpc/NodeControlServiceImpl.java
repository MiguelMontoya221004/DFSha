package io.dfsha.controlnode.grpc;

import io.dfsha.controlnode.config.ControlNodeProperties;
import io.dfsha.controlnode.service.BlockManager;
import io.dfsha.proto.Ack;
import io.dfsha.proto.BlockReceivedRequest;
import io.dfsha.proto.BlockReportRequest;
import io.dfsha.proto.CorruptBlockRequest;
import io.dfsha.proto.HeartbeatRequest;
import io.dfsha.proto.HeartbeatResponse;
import io.dfsha.proto.NodeControlGrpc;
import io.dfsha.proto.RegisterRequest;
import io.dfsha.proto.RegisterResponse;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

/** Servicio gRPC que atiende a los DataNodes (registro, heartbeats, reportes). */
@Component
public class NodeControlServiceImpl extends NodeControlGrpc.NodeControlImplBase {

    private final BlockManager blocks;
    private final ControlNodeProperties props;

    public NodeControlServiceImpl(BlockManager blocks, ControlNodeProperties props) {
        this.blocks = blocks;
        this.props = props;
    }

    @Override
    public void register(RegisterRequest req, StreamObserver<RegisterResponse> obs) {
        blocks.register(req.getNodeId(), req.getAddress(), req.getZone(), req.getCapacityBytes());
        obs.onNext(RegisterResponse.newBuilder()
                .setAccepted(true)
                .setHeartbeatIntervalMs(props.heartbeatIntervalMs())
                .setClusterId(props.clusterId())
                .setMessage("Bienvenido al clúster " + props.clusterId())
                .build());
        obs.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest req, StreamObserver<HeartbeatResponse> obs) {
        BlockManager.HeartbeatResult result = blocks.heartbeat(req.getNodeId(), req.getUsedBytes(),
                req.getFreeBytes(), req.getActiveTransfers(), req.getBlockCount());
        obs.onNext(HeartbeatResponse.newBuilder()
                .setRegistered(result.registered())
                .setIsLeader(true)               // en el Hito 3 lo dirá Raft
                .addAllCommands(result.commands())
                .build());
        obs.onCompleted();
    }

    @Override
    public void blockReport(BlockReportRequest req, StreamObserver<Ack> obs) {
        blocks.blockReport(req.getNodeId(), req.getBlocksList(), req.getFullReport());
        ok(obs, "reporte recibido: " + req.getBlocksCount() + " bloques");
    }

    @Override
    public void blockReceived(BlockReceivedRequest req, StreamObserver<Ack> obs) {
        blocks.blockReceived(req.getNodeId(), req.getBlock());
        ok(obs, "bloque registrado");
    }

    @Override
    public void reportCorrupt(CorruptBlockRequest req, StreamObserver<Ack> obs) {
        blocks.reportCorrupt(req.getNodeId(), req.getBlockId(), req.getDetail());
        ok(obs, "bloque marcado como corrupto");
    }

    private void ok(StreamObserver<Ack> obs, String message) {
        obs.onNext(Ack.newBuilder().setOk(true).setMessage(message).build());
        obs.onCompleted();
    }
}
