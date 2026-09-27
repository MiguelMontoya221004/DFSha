"""Configuración del DataNode, tomada de variables de entorno."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    node_id: str = os.getenv("DFSHA_NODE_ID", "dn1")
    # Puerto en el que escucha el servicio gRPC DataTransfer
    port: int = int(os.getenv("DFSHA_DN_PORT", "50051"))
    # Dirección que se anuncia al ControlNode (lo que usan clientes y otros DataNodes)
    advertise: str = os.getenv("DFSHA_DN_ADVERTISE", "")
    zone: str = os.getenv("DFSHA_ZONE", "zona-a")
    data_dir: str = os.getenv("DFSHA_DATA_DIR", "/var/lib/dfsha/blocks")
    capacity_bytes: int = int(os.getenv("DFSHA_CAPACITY_BYTES", str(8 * 1024 ** 3)))
    control_node: str = os.getenv("DFSHA_CONTROLNODE", "localhost:9090")
    block_token_secret: str = os.getenv(
        "DFSHA_BLOCK_SECRET", "dfsha-block-secret-cambiar-en-produccion"
    )
    chunk_size: int = int(os.getenv("DFSHA_CHUNK_SIZE", str(1024 * 1024)))
    heartbeat_interval_s: float = float(os.getenv("DFSHA_HEARTBEAT_S", "3"))
    full_report_interval_s: float = float(os.getenv("DFSHA_FULL_REPORT_S", "300"))
    log_level: str = os.getenv("DFSHA_LOG_LEVEL", "INFO")

    def __post_init__(self) -> None:
        if not self.advertise:
            self.advertise = f"localhost:{self.port}"


def load() -> Config:
    return Config()
