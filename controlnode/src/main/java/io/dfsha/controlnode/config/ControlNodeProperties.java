package io.dfsha.controlnode.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuración del ControlNode (prefijo "dfsha" en application.yml). */
@ConfigurationProperties(prefix = "dfsha")
public record ControlNodeProperties(
        String nodeId,
        String clusterId,
        int grpcPort,
        int blockSize,
        int replicationFactor,
        int minReplicas,
        int heartbeatIntervalMs,
        long nodeDeadAfterMs,
        String jwtSecret,
        long jwtTtlMinutes,
        String blockTokenSecret,
        long blockTokenTtlSeconds,
        long leaseTtlSeconds,
        String adminUser,
        String adminPassword
) {}
