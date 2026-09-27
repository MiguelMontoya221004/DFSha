package io.dfsha.controlnode.grpc;

import io.dfsha.controlnode.config.ControlNodeProperties;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Arranca el servidor gRPC del plano de control.
 * Hito 2: texto claro. Hito 3: mTLS con la CA del clúster.
 */
@Component
public class GrpcServerRunner {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerRunner.class);

    private final NodeControlServiceImpl service;
    private final ControlNodeProperties props;
    private Server server;

    public GrpcServerRunner(NodeControlServiceImpl service, ControlNodeProperties props) {
        this.service = service;
        this.props = props;
    }

    @PostConstruct
    public void start() throws IOException {
        server = ServerBuilder.forPort(props.grpcPort())
                .addService(service)
                .maxInboundMessageSize(8 * 1024 * 1024)
                .build()
                .start();
        log.info("Servidor gRPC NodeControl escuchando en el puerto {}", props.grpcPort());
    }

    @PreDestroy
    public void stop() {
        if (server != null) server.shutdownNow();
    }
}
