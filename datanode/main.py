"""Arranque del DataNode.

    python datanode/main.py          (o:  python -m uvicorn ... con las mismas variables)

Levanta la API HTTP de bloques y, en paralelo, el hilo que habla con el
ControlNode: se registra, manda heartbeats y obedece las órdenes que llegan en
la respuesta.
"""
import logging
import sys
from pathlib import Path

# Permite ejecutar el archivo directamente, sin instalar nada
sys.path.insert(0, str(Path(__file__).resolve().parent))

import uvicorn

from config import Config
from control_node import ControlNodeCliente
from server import crear_app
from storage import Almacen

log = logging.getLogger("datanode")


def main():
    config = Config()
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format=f"%(asctime)s %(levelname)-5s [{config.node_id}] %(name)s: %(message)s",
    )
    log.info("%s", config)

    almacen = Almacen(config.data_dir)
    control = ControlNodeCliente(config, almacen)
    app = crear_app(config, almacen, control)

    # Si el ControlNode todavía no responde no abortamos: el hilo de heartbeats
    # sigue reintentando y el nodo queda listo para cuando aparezca.
    if not control.registrarse():
        log.error("No fue posible registrarse en %s; se seguira reintentando",
                  config.control_node)
    control.arrancar()

    try:
        uvicorn.run(app, host="0.0.0.0", port=config.port, log_level="warning")
    finally:
        log.info("Apagando DataNode...")
        control.detener()


if __name__ == "__main__":
    main()
