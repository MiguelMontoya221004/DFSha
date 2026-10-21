"""Arranque del DataNode.

    python datanode/main.py

Levanta la API HTTP de bloques y, en paralelo, el hilo que habla con los tres
ControlNodes: se registra, manda heartbeats y obedece las ordenes que llegan en
la respuesta.
"""
import logging
import sys
import threading
from pathlib import Path

# Permite ejecutar el archivo directamente, sin instalar nada
sys.path.insert(0, str(Path(__file__).resolve().parent))

import uvicorn

from config import Config
from control_node import ControlNodes
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
    control = ControlNodes(config, almacen)
    app = crear_app(config, almacen, control)

    # El registro va en un hilo aparte para que la API HTTP quede escuchando
    # de una vez: si los ControlNodes todavia no estan listos, el hilo sigue
    # reintentando sin dejar al DataNode sordo mientras tanto.
    def presentarse():
        if not control.registrar():
            log.error("No fue posible registrarse en %s; se seguira reintentando",
                      ", ".join(config.control_nodes))
        control.arrancar()

    threading.Thread(target=presentarse, name="registro", daemon=True).start()

    try:
        uvicorn.run(app, host="0.0.0.0", port=config.client_port, log_level="warning")
    finally:
        log.info("Apagando DataNode...")
        control.detener()


if __name__ == "__main__":
    main()
