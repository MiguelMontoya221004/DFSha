"""Servicio gRPC DataTransfer: escritura con pipeline y lectura en streaming.

Este es el plano de datos: los bytes van del cliente al DataNode y de DataNode a
DataNode, nunca por el ControlNode.
"""
from __future__ import annotations

import logging
import queue
import threading

import grpc

from .config import Config
from .pb import dfsha_pb2, dfsha_pb2_grpc
from .storage import BlockStore
from . import tokens

log = logging.getLogger("dfsha.datanode.service")

_SENTINEL = object()


class DataTransferService(dfsha_pb2_grpc.DataTransferServicer):
    def __init__(self, cfg: Config, store: BlockStore, reporter) -> None:
        self.cfg = cfg
        self.store = store
        self.reporter = reporter          # ControlNodeClient, para BlockReceived
        self._active = 0
        self._active_lock = threading.Lock()

    # --------------------------------------------------------------- métricas
    @property
    def active_transfers(self) -> int:
        with self._active_lock:
            return self._active

    def _begin(self) -> None:
        with self._active_lock:
            self._active += 1

    def _end(self) -> None:
        with self._active_lock:
            self._active -= 1

    # ------------------------------------------------------------- escritura
    def WriteBlock(self, request_iterator, context):
        self._begin()
        writer = None
        forwarder = None
        try:
            header = None
            for message in request_iterator:
                kind = message.WhichOneof("payload")
                if kind == "header":
                    if header is not None:
                        return self._error(context, grpc.StatusCode.INVALID_ARGUMENT,
                                           "el header llegó dos veces")
                    header = message.header
                    valid, reason = tokens.verify(
                        header.block_token, header.block_id, "w", self.cfg.block_token_secret)
                    if not valid:
                        log.warning("Escritura rechazada del bloque %s: %s", header.block_id, reason)
                        return self._error(context, grpc.StatusCode.PERMISSION_DENIED,
                                           f"token de bloque inválido: {reason}")
                    writer = self.store.writer(header.block_id)
                    if header.pipeline:
                        forwarder = _PipelineForwarder(header, self.cfg)
                        forwarder.start()
                    log.info("Recibiendo bloque %s (%d bytes) · siguientes en pipeline: %s",
                             header.block_id, header.size, list(header.pipeline) or "-")
                elif kind == "chunk":
                    if writer is None:
                        return self._error(context, grpc.StatusCode.INVALID_ARGUMENT,
                                           "llegaron datos antes del header")
                    writer.write(message.chunk)
                    if forwarder is not None:
                        forwarder.send(message.chunk)

            if header is None or writer is None:
                return self._error(context, grpc.StatusCode.INVALID_ARGUMENT, "stream vacío")

            stored = writer.commit(header.sha256 or None)
            writer = None

            stored_on = [self.cfg.node_id]
            if forwarder is not None:
                downstream = forwarder.finish()
                stored_on.extend(downstream)

            # Aviso sincrónico al ControlNode: cuando el cliente recibe este ack,
            # el ControlNode ya sabe dónde está la réplica (evita carreras en el commit).
            self.reporter.block_received(stored.block_id, stored.size, stored.sha256)

            log.info("Bloque %s almacenado (%d bytes) · réplicas confirmadas: %s",
                     stored.block_id, stored.size, stored_on)
            return dfsha_pb2.WriteBlockResponse(ok=True, stored_on=stored_on)

        except ValueError as exc:                     # checksum inválido
            log.error("Checksum inválido: %s", exc)
            return self._error(context, grpc.StatusCode.DATA_LOSS, str(exc))
        except Exception as exc:                      # noqa: BLE001
            log.exception("Error escribiendo el bloque")
            return self._error(context, grpc.StatusCode.INTERNAL, str(exc))
        finally:
            if writer is not None:
                writer.abort()
            self._end()

    def _error(self, context, code, message):
        context.set_code(code)
        context.set_details(message)
        return dfsha_pb2.WriteBlockResponse(ok=False, error=message)

    # --------------------------------------------------------------- lectura
    def ReadBlock(self, request, context):
        valid, reason = tokens.verify(request.block_token, request.block_id, "r",
                                      self.cfg.block_token_secret)
        if not valid:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, f"token de bloque inválido: {reason}")
        if not self.store.exists(request.block_id):
            context.abort(grpc.StatusCode.NOT_FOUND, f"bloque {request.block_id} no está aquí")
        self._begin()
        try:
            for chunk in self.store.read(request.block_id, request.offset, request.length,
                                         self.cfg.chunk_size):
                yield dfsha_pb2.BlockChunk(data=chunk)
        finally:
            self._end()

    # ---------------------------------------------------------------- estado
    def NodeStatus(self, request, context):
        blocks = self.store.list_blocks()
        return dfsha_pb2.NodeStatusResponse(
            node_id=self.cfg.node_id,
            zone=self.cfg.zone,
            capacity_bytes=self.cfg.capacity_bytes,
            used_bytes=sum(b.size for b in blocks),
            block_count=len(blocks),
            active_transfers=self.active_transfers,
        )


class _PipelineForwarder:
    """Reenvía el mismo stream al siguiente DataNode del pipeline, en paralelo.

    El primer nodo manda a [D2, D3], el segundo a [D3] y el tercero a nadie, así
    la replicación va en cadena y el cliente sólo carga una vez los datos.
    """

    def __init__(self, header, cfg: Config) -> None:
        self.cfg = cfg
        self.target = header.pipeline[0]
        self.header = dfsha_pb2.BlockHeader(
            block_id=header.block_id,
            block_token=header.block_token,
            pipeline=list(header.pipeline[1:]),
            size=header.size,
            sha256=header.sha256,
            client_id=header.client_id,
        )
        self.queue: queue.Queue = queue.Queue(maxsize=16)
        self.response = None
        self.error: Exception | None = None
        self.thread = threading.Thread(target=self._run, name=f"pipeline->{self.target}", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def send(self, chunk: bytes) -> None:
        if self.error is not None:
            return          # el siguiente nodo del pipeline ya falló: no bloquear la escritura local
        try:
            self.queue.put(chunk, timeout=30)
        except queue.Full:
            log.warning("Cola del pipeline hacia %s llena: se deja de reenviar", self.target)
            self.error = TimeoutError("pipeline saturado")

    def finish(self) -> list[str]:
        self.queue.put(_SENTINEL)
        self.thread.join(timeout=120)
        if self.error is not None:
            log.warning("El pipeline hacia %s falló: %s", self.target, self.error)
            return []
        return list(self.response.stored_on) if self.response and self.response.ok else []

    def _requests(self):
        yield dfsha_pb2.WriteBlockRequest(header=self.header)
        while True:
            item = self.queue.get()
            if item is _SENTINEL:
                return
            yield dfsha_pb2.WriteBlockRequest(chunk=item)

    def _run(self) -> None:
        try:
            with grpc.insecure_channel(
                self.target,
                options=[("grpc.max_send_message_length", 8 * 1024 * 1024),
                         ("grpc.max_receive_message_length", 8 * 1024 * 1024)],
            ) as channel:
                stub = dfsha_pb2_grpc.DataTransferStub(channel)
                self.response = stub.WriteBlock(self._requests(), timeout=180)
        except Exception as exc:                      # noqa: BLE001
            self.error = exc
            # Se vacía la cola para no bloquear al hilo principal.
            try:
                while True:
                    if self.queue.get_nowait() is _SENTINEL:
                        break
            except queue.Empty:
                pass
