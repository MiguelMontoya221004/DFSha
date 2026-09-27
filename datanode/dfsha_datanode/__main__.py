"""Punto de entrada del DataNode:  python -m dfsha_datanode"""
from __future__ import annotations

import logging
import signal
import threading
from concurrent import futures

import grpc

from .config import load
from .controlnode import ControlNodeClient
from .pb import dfsha_pb2_grpc
from .service import DataTransferService
from .storage import BlockStore

log = logging.getLogger("dfsha.datanode")


def main() -> None:
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format=f"%(asctime)s %(levelname)-5s [{cfg.node_id}] %(name)s: %(message)s",
    )

    store = BlockStore(cfg.data_dir)
    control = ControlNodeClient(cfg, store)
    service = DataTransferService(cfg, store, control)
    control.active_transfers_provider = lambda: service.active_transfers

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[("grpc.max_send_message_length", 8 * 1024 * 1024),
                 ("grpc.max_receive_message_length", 8 * 1024 * 1024)],
    )
    dfsha_pb2_grpc.add_DataTransferServicer_to_server(service, server)
    server.add_insecure_port(f"0.0.0.0:{cfg.port}")
    server.start()
    log.info("DataTransfer escuchando en 0.0.0.0:%d (anunciado como %s, zona %s, datos en %s)",
             cfg.port, cfg.advertise, cfg.zone, cfg.data_dir)

    if not control.register():
        log.error("No fue posible registrarse en el ControlNode %s; se sigue reintentando",
                  cfg.control_node)
    control.start()

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    stop.wait()

    log.info("Apagando DataNode…")
    control.stop()
    server.stop(5).wait()


if __name__ == "__main__":
    main()
