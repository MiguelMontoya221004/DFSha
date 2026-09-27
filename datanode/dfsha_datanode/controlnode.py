"""Cliente gRPC hacia el ControlNode: registro, heartbeats, reportes y comandos.

El DataNode es el que inicia todas estas llamadas. El ControlNode responde a los
heartbeats con comandos (replicar o borrar bloques): es el mecanismo de
"piggyback" de HDFS, que evita que el ControlNode tenga que abrir conexiones
hacia los DataNodes.
"""
from __future__ import annotations

import logging
import threading
import time

import grpc

from .config import Config
from .pb import dfsha_pb2, dfsha_pb2_grpc
from .storage import BlockStore

log = logging.getLogger("dfsha.datanode.control")


class ControlNodeClient:
    def __init__(self, cfg: Config, store: BlockStore) -> None:
        self.cfg = cfg
        self.store = store
        self.channel = grpc.insecure_channel(cfg.control_node)
        self.stub = dfsha_pb2_grpc.NodeControlStub(self.channel)
        self.registered = False
        self.active_transfers_provider = lambda: 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------- registro
    def register(self, retries: int = 30) -> bool:
        for attempt in range(1, retries + 1):
            try:
                response = self.stub.Register(
                    dfsha_pb2.RegisterRequest(
                        node_id=self.cfg.node_id,
                        address=self.cfg.advertise,
                        zone=self.cfg.zone,
                        capacity_bytes=self.cfg.capacity_bytes,
                    ),
                    timeout=10,
                )
                if response.accepted:
                    self.registered = True
                    log.info("Registrado en el ControlNode %s (clúster %s)",
                             self.cfg.control_node, response.cluster_id)
                    self.send_full_report()
                    return True
            except grpc.RpcError as exc:
                log.warning("No se pudo registrar (intento %d/%d): %s",
                            attempt, retries, exc.code())
            time.sleep(min(2 * attempt, 10))
        return False

    # -------------------------------------------------------------- reportes
    def send_full_report(self) -> None:
        blocks = [
            dfsha_pb2.BlockInfo(block_id=b.block_id, size=b.size, sha256=b.sha256)
            for b in self.store.list_blocks()
        ]
        try:
            self.stub.BlockReport(
                dfsha_pb2.BlockReportRequest(
                    node_id=self.cfg.node_id, blocks=blocks, full_report=True),
                timeout=30,
            )
            log.info("Reporte completo enviado: %d bloques", len(blocks))
        except grpc.RpcError as exc:
            log.warning("Falló el reporte completo: %s", exc.code())

    def block_received(self, block_id: str, size: int, sha256: str) -> None:
        try:
            self.stub.BlockReceived(
                dfsha_pb2.BlockReceivedRequest(
                    node_id=self.cfg.node_id,
                    block=dfsha_pb2.BlockInfo(block_id=block_id, size=size, sha256=sha256),
                ),
                timeout=10,
            )
        except grpc.RpcError as exc:
            log.warning("No se pudo avisar del bloque %s: %s", block_id, exc.code())

    def report_corrupt(self, block_id: str, detail: str) -> None:
        try:
            self.stub.ReportCorrupt(
                dfsha_pb2.CorruptBlockRequest(
                    reporter=self.cfg.node_id, node_id=self.cfg.node_id,
                    block_id=block_id, detail=detail),
                timeout=10,
            )
        except grpc.RpcError:
            pass

    # ------------------------------------------------------------ heartbeats
    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self.channel.close()

    def _loop(self) -> None:
        last_full_report = time.time()
        while not self._stop.is_set():
            try:
                blocks = self.store.list_blocks()
                response = self.stub.Heartbeat(
                    dfsha_pb2.HeartbeatRequest(
                        node_id=self.cfg.node_id,
                        used_bytes=sum(b.size for b in blocks),
                        free_bytes=self.store.free_bytes(),
                        active_transfers=self.active_transfers_provider(),
                        block_count=len(blocks),
                    ),
                    timeout=10,
                )
                if not response.registered:
                    log.warning("El ControlNode no nos conoce: volviendo a registrarse")
                    self.register(retries=3)
                else:
                    for command in response.commands:
                        self._execute(command)
                if time.time() - last_full_report > self.cfg.full_report_interval_s:
                    self.send_full_report()
                    last_full_report = time.time()
            except grpc.RpcError as exc:
                log.warning("Heartbeat fallido: %s", exc.code())
            self._stop.wait(self.cfg.heartbeat_interval_s)

    # -------------------------------------------------------------- comandos
    def _execute(self, command) -> None:
        which = command.WhichOneof("cmd")
        if which == "replicate":
            threading.Thread(target=self._replicate, args=(command.replicate,), daemon=True).start()
        elif which == "delete":
            for block_id in command.delete.block_ids:
                if self.store.delete(block_id):
                    log.info("Bloque %s borrado por orden del ControlNode", block_id)

    def _replicate(self, cmd) -> None:
        """Copia un bloque local a otro DataNode (re-replicación P2P)."""
        meta = self.store.meta(cmd.block_id)
        if meta is None:
            log.warning("Me piden replicar %s pero no lo tengo", cmd.block_id)
            return
        header = dfsha_pb2.BlockHeader(
            block_id=cmd.block_id,
            block_token=cmd.block_token,
            pipeline=[],                      # el destino es el último de la cadena
            size=meta.size,
            sha256=meta.sha256,
            client_id=f"node:{self.cfg.node_id}",
        )

        def requests():
            yield dfsha_pb2.WriteBlockRequest(header=header)
            for chunk in self.store.read(cmd.block_id, chunk_size=self.cfg.chunk_size):
                yield dfsha_pb2.WriteBlockRequest(chunk=chunk)

        try:
            with grpc.insecure_channel(cmd.target_address) as channel:
                stub = dfsha_pb2_grpc.DataTransferStub(channel)
                response = stub.WriteBlock(requests(), timeout=180)
            if response.ok:
                log.info("Bloque %s replicado hacia %s", cmd.block_id, cmd.target_address)
            else:
                log.warning("El destino %s rechazó el bloque %s: %s",
                            cmd.target_address, cmd.block_id, response.error)
        except grpc.RpcError as exc:
            log.warning("Falló la replicación de %s hacia %s: %s",
                        cmd.block_id, cmd.target_address, exc.code())
