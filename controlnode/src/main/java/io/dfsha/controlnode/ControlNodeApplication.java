package io.dfsha.controlnode;

import io.dfsha.controlnode.config.ControlNodeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * DFSha ControlNode — plano de control del sistema de archivos distribuido.
 *
 * Expone:
 *   - API REST (puerto 8080 por defecto) para los clientes;
 *   - servicio gRPC NodeControl (puerto 9090) para los DataNodes.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ControlNodeProperties.class)
public class ControlNodeApplication {
    public static void main(String[] args) {
        SpringApplication.run(ControlNodeApplication.class, args);
    }
}
