"""Configuración del DataNode.

Todo se lee de variables de entorno para que el mismo código sirva en local y en
Docker sin tocar nada.
"""
import os


class Config:
    def __init__(self):
        # Identidad del nodo dentro del clúster
        self.node_id = os.getenv("DFSHA_NODE_ID", "dn1")
        self.zone = os.getenv("DFSHA_ZONE", "zona-a")

        # Puerto donde escucha la API HTTP de bloques
        self.port = int(os.getenv("DFSHA_DN_PORT", "50051"))
        # Dirección que se anuncia al ControlNode: es la que usarán el cliente y
        # los otros DataNodes para hablarle a este nodo
        self.advertise = os.getenv("DFSHA_DN_ADVERTISE", f"localhost:{self.port}")

        # Dónde se guardan los bloques y cuánto espacio decimos tener
        self.data_dir = os.getenv("DFSHA_DATA_DIR", "/var/lib/dfsha/blocks")
        self.capacity_bytes = int(os.getenv("DFSHA_CAPACITY_BYTES", str(8 * 1024 ** 3)))

        # ControlNode: el plano de control sigue siendo gRPC
        self.control_node = os.getenv("DFSHA_CONTROLNODE", "localhost:9090")

        # Secreto compartido con el ControlNode para verificar los tokens de bloque
        self.block_secret = os.getenv(
            "DFSHA_BLOCK_SECRET", "dfsha-block-secret-cambiar-en-produccion"
        )

        # Tamaño de los trozos al leer y escribir en disco y por la red
        self.chunk_size = int(os.getenv("DFSHA_CHUNK_SIZE", str(1024 * 1024)))

        # Cada cuánto se envía un heartbeat y cada cuánto un reporte completo
        self.heartbeat_seconds = float(os.getenv("DFSHA_HEARTBEAT_S", "3"))
        self.full_report_seconds = float(os.getenv("DFSHA_FULL_REPORT_S", "300"))

        self.log_level = os.getenv("DFSHA_LOG_LEVEL", "INFO")

    def __str__(self):
        return (f"DataNode {self.node_id} (zona {self.zone}) · HTTP en :{self.port} · "
                f"anunciado como {self.advertise} · datos en {self.data_dir} · "
                f"ControlNode en {self.control_node}")
