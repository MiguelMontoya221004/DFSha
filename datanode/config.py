"""Configuracion del DataNode.

Todo se lee de variables de entorno para que el mismo codigo sirva en local y en
Docker sin tocar nada. Los nombres de los parametros siguen el contrato del
ControlNode (controlnode/src/dfsha.proto).
"""
import os

VERSION = "0.4.0"


class Config:
    def __init__(self):
        # Identidad del nodo. Tiene que sobrevivir a los reinicios: es con la
        # que quedan guardadas las replicas en los metadatos del ControlNode.
        self.node_id = os.getenv("DFSHA_NODE_ID", "dn-a1")
        self.zone = os.getenv("DFSHA_ZONE", "zona-a")

        # Host con el que alcanzan a este nodo el cliente y los otros DataNodes
        self.host = os.getenv("DFSHA_HOST", "localhost")
        # Puerto donde atiende al CLIENTE (subir y bajar bloques)
        self.client_port = int(os.getenv("DFSHA_CLIENT_PORT", "9001"))
        # Puerto donde atiende a los OTROS DataNodes (pipeline de replicacion).
        # Nuestro pipeline tambien es HTTP, asi que por defecto es el mismo.
        self.peer_port = int(os.getenv("DFSHA_PEER_PORT", str(self.client_port)))

        # Donde se guardan los bloques y cuanto espacio decimos tener
        self.data_dir = os.getenv("DFSHA_DATA_DIR", "./data/bloques")
        self.capacity_bytes = int(os.getenv("DFSHA_CAPACITY_BYTES", str(8 * 1024 ** 3)))

        # Los TRES ControlNodes (gRPC). Se le manda el heartbeat a todos, no
        # solo al lider: asi cualquiera de los tres sabe quien esta vivo.
        self.control_nodes = [
            d.strip() for d in os.getenv(
                "DFSHA_CONTROLNODES", "localhost:50051,localhost:50052,localhost:50053"
            ).split(",") if d.strip()
        ]

        # Secreto compartido con el ControlNode (dfsha.block-secret) para
        # verificar los tokens de bloque
        self.block_secret = os.getenv(
            "DFSHA_BLOCK_SECRET", "clave-hmac-compartida-con-los-datanodes"
        )

        # Tamano de los trozos al leer y escribir en disco y por la red
        self.chunk_size = int(os.getenv("DFSHA_CHUNK_SIZE", str(1024 * 1024)))

        # Cada cuanto se manda el heartbeat; el ControlNode puede cambiarlo en
        # la respuesta del registro (heartbeat_seconds)
        self.heartbeat_seconds = float(os.getenv("DFSHA_HEARTBEAT_S", "3"))
        # Cada cuanto se manda el reporte completo de bloques
        self.full_report_seconds = float(os.getenv("DFSHA_FULL_REPORT_S", "300"))

        self.log_level = os.getenv("DFSHA_LOG_LEVEL", "INFO")

    def __str__(self):
        return (f"DataNode {self.node_id} (zona {self.zone}) v{VERSION} · "
                f"cliente en http://{self.host}:{self.client_port} · "
                f"pipeline en :{self.peer_port} · datos en {self.data_dir} · "
                f"ControlNodes {', '.join(self.control_nodes)}")
