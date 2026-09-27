"""SDK de DFSha.

Dos planos, igual que el servicio:
  * metadatos y autenticación -> REST contra el ControlNode (requests);
  * bloques -> gRPC directo contra los DataNodes (grpcio), en paralelo.

El ControlNode nunca ve los bytes de los archivos.
"""
from __future__ import annotations

import hashlib
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import grpc
import requests

from .pb import dfsha_pb2, dfsha_pb2_grpc

log = logging.getLogger("dfsha.client")

DEFAULT_THREADS = int(os.getenv("DFSHA_THREADS", "4"))
CHUNK_SIZE = int(os.getenv("DFSHA_CHUNK_SIZE", str(1024 * 1024)))
MAX_BLOCK_RETRIES = 3


class DfshaError(RuntimeError):
    def __init__(self, code: str, message: str, status: int | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status = status


@dataclass
class TransferStats:
    path: str
    size: int = 0
    blocks: int = 0
    seconds: float = 0.0
    nodes_used: set[str] = field(default_factory=set)

    @property
    def mbps(self) -> float:
        return (self.size / (1024 * 1024)) / self.seconds if self.seconds > 0 else 0.0


class DfshaClient:
    def __init__(self, control_node: str, token: str | None = None, timeout: float = 30.0) -> None:
        self.base = control_node.rstrip("/") + "/api/v1"
        self.token = token
        self.timeout = timeout
        self.http = requests.Session()
        self._last_nodes: set[str] = set()

    # ------------------------------------------------------------------ REST
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def _request(self, method: str, path: str, **kwargs):
        response = self.http.request(method, self.base + path, headers=self._headers(),
                                     timeout=self.timeout, **kwargs)
        if response.status_code >= 400:
            try:
                body = response.json()
                raise DfshaError(body.get("error", "ERROR"), body.get("message", response.text),
                                 response.status_code)
            except ValueError:
                raise DfshaError("HTTP_" + str(response.status_code), response.text,
                                 response.status_code) from None
        return response.json() if response.content else {}

    # ------------------------------------------------------------- RF4: auth
    def register(self, username: str, password: str) -> dict:
        return self._request("POST", "/auth/register",
                             json={"username": username, "password": password})

    def login(self, username: str, password: str) -> dict:
        session = self._request("POST", "/auth/login",
                                json={"username": username, "password": password})
        self.token = session["token"]
        return session

    # --------------------------------------------------------- RF1: namespace
    def ls(self, path: str = "/") -> list[dict]:
        return self._request("GET", "/fs/list", params={"path": path})["entries"]

    def stat(self, path: str) -> dict:
        return self._request("GET", "/fs/stat", params={"path": path})

    def mkdir(self, path: str, parents: bool = False) -> dict:
        return self._request("POST", "/fs/mkdir", json={"path": path, "parents": parents})

    def rm(self, path: str, recursive: bool = False) -> dict:
        return self._request("DELETE", "/fs/delete",
                             params={"path": path, "recursive": str(recursive).lower()})

    def mv(self, src: str, dst: str) -> dict:
        return self._request("POST", "/fs/move", json={"src": src, "dst": dst})

    # --------------------------------------------------------- RF5: monitoreo
    def cluster_status(self) -> dict:
        return self._request("GET", "/cluster/status")

    def health(self) -> dict:
        return self._request("GET", "/health")

    # ------------------------------------------------- RF2: transferencia put
    def put(self, local_path: str | Path, remote_path: str, overwrite: bool = False,
            threads: int = DEFAULT_THREADS, progress=None) -> TransferStats:
        import time

        local = Path(local_path)
        if not local.is_file():
            raise DfshaError("LOCAL_NOT_FOUND", f"No existe el archivo local {local}")
        size = local.stat().st_size
        started = time.time()

        created = self._request("POST", "/files/create",
                                json={"path": remote_path, "overwrite": overwrite})
        version_id = created["versionId"]
        block_size = created["blockSize"]
        total_blocks = max(1, (size + block_size - 1) // block_size)
        stats = TransferStats(path=created["path"], size=size, blocks=total_blocks)
        done = threading.Lock()
        completed = [0]

        def upload(index: int) -> dict:
            with open(local, "rb") as handle:
                handle.seek(index * block_size)
                data = handle.read(block_size)
            sha = hashlib.sha256(data).hexdigest()
            last_error: Exception | None = None
            for attempt in range(1, MAX_BLOCK_RETRIES + 1):
                allocated = self._request("POST", f"/versions/{version_id}/blocks",
                                          json={"index": index})
                pipeline = [n["address"] for n in allocated["pipeline"]]
                try:
                    stored_on = self._write_block(pipeline, allocated["blockId"],
                                                  allocated["token"], data, sha)
                    stats.nodes_used.update(stored_on)
                    with done:
                        completed[0] += 1
                        if progress:
                            progress(completed[0], total_blocks)
                    return {"index": index, "blockId": allocated["blockId"],
                            "size": len(data), "sha256": sha}
                except Exception as exc:                       # noqa: BLE001
                    last_error = exc
                    log.warning("Bloque %d falló (intento %d/%d): %s",
                                index, attempt, MAX_BLOCK_RETRIES, exc)
            raise DfshaError("BLOCK_WRITE_FAILED",
                             f"No se pudo escribir el bloque {index}: {last_error}")

        try:
            blocks: list[dict] = []
            with ThreadPoolExecutor(max_workers=threads) as pool:
                for future in as_completed([pool.submit(upload, i) for i in range(total_blocks)]):
                    blocks.append(future.result())
            blocks.sort(key=lambda b: b["index"])
            self._request("POST", f"/versions/{version_id}/commit", json={"blocks": blocks})
        except Exception:
            try:
                self._request("POST", f"/versions/{version_id}/abort")
            except DfshaError:
                pass
            raise

        stats.seconds = time.time() - started
        return stats

    def _write_block(self, pipeline: list[str], block_id: str, token: str,
                     data: bytes, sha: str) -> list[str]:
        """Envía un bloque al primer DataNode del pipeline; él lo reenvía al resto."""
        header = dfsha_pb2.BlockHeader(
            block_id=block_id, block_token=token, pipeline=pipeline[1:],
            size=len(data), sha256=sha, client_id="cli")

        def requests_iter():
            yield dfsha_pb2.WriteBlockRequest(header=header)
            for offset in range(0, len(data), CHUNK_SIZE):
                yield dfsha_pb2.WriteBlockRequest(chunk=data[offset:offset + CHUNK_SIZE])

        with grpc.insecure_channel(pipeline[0], options=_GRPC_OPTIONS) as channel:
            stub = dfsha_pb2_grpc.DataTransferStub(channel)
            response = stub.WriteBlock(requests_iter(), timeout=180)
        if not response.ok:
            raise DfshaError("BLOCK_WRITE_REJECTED", response.error or "rechazado por el DataNode")
        return list(response.stored_on)

    # ------------------------------------------------- RF2: transferencia get
    def get(self, remote_path: str, local_path: str | Path,
            threads: int = DEFAULT_THREADS, progress=None) -> TransferStats:
        import time

        started = time.time()
        opened = self._request("POST", "/files/open", json={"path": remote_path})
        local = Path(local_path)
        local.parent.mkdir(parents=True, exist_ok=True)
        with open(local, "wb") as handle:                # reserva el tamaño final
            handle.truncate(opened["size"])

        stats = TransferStats(path=opened["path"], size=opened["size"],
                              blocks=len(opened["blocks"]))
        block_size = opened["blockSize"]
        lock = threading.Lock()
        completed = [0]

        def download(block: dict) -> str:
            data, node = self._read_block_with_node(block)
            with open(local, "r+b") as handle:
                handle.seek(block["index"] * block_size)
                handle.write(data)
            with lock:
                completed[0] += 1
                if progress:
                    progress(completed[0], stats.blocks)
            return node

        with ThreadPoolExecutor(max_workers=threads) as pool:
            for future in as_completed([pool.submit(download, b) for b in opened["blocks"]]):
                stats.nodes_used.add(future.result())

        stats.seconds = time.time() - started
        return stats

    def _read_block(self, block: dict, offset: int = 0, length: int = 0) -> bytes:
        return self._read_block_with_node(block, offset, length)[0]

    def _read_block_with_node(self, block: dict, offset: int = 0,
                              length: int = 0) -> tuple[bytes, str]:
        """Lee un bloque probando las réplicas en orden (failover) y verificando el checksum."""
        errors = []
        for replica in block["replicas"]:
            try:
                buffer = bytearray()
                with grpc.insecure_channel(replica["address"], options=_GRPC_OPTIONS) as channel:
                    stub = dfsha_pb2_grpc.DataTransferStub(channel)
                    for chunk in stub.ReadBlock(
                            dfsha_pb2.ReadBlockRequest(
                                block_id=block["blockId"], block_token=block["token"],
                                offset=offset, length=length),
                            timeout=180):
                        buffer.extend(chunk.data)
                data = bytes(buffer)
                if offset == 0 and length == 0 and block.get("sha256"):
                    if hashlib.sha256(data).hexdigest() != block["sha256"]:
                        raise DfshaError("CHECKSUM_MISMATCH",
                                         f"checksum inválido del bloque {block['index']} en "
                                         f"{replica['nodeId']}")
                return data, replica["nodeId"]
            except Exception as exc:                          # noqa: BLE001
                errors.append(f"{replica['nodeId']}: {exc}")
                log.warning("Réplica %s falló para el bloque %s (%s); probando la siguiente",
                            replica["nodeId"], block["blockId"], exc)
        raise DfshaError("NO_REPLICA_AVAILABLE",
                         f"Ninguna réplica del bloque {block['index']} respondió: {errors}")

    # -------------------------------------------------- RF3: acceso a archivos
    def open(self, remote_path: str) -> "DfshaFile":
        return DfshaFile(self, self._request("POST", "/files/open", json={"path": remote_path}))

    def write_at(self, remote_path: str, offset: int, data: bytes) -> dict:
        """Escritura parcial con copy-on-write.

        Sólo se vuelven a subir los bloques que cambian; el resto de la versión
        nueva reutiliza los mismos blockId, así que no se mueven esos bytes.
        """
        current = self._request("POST", "/files/open", json={"path": remote_path})
        block_size = current["blockSize"]
        blocks = {b["index"]: b for b in current["blocks"]}
        first = offset // block_size
        last = (offset + len(data) - 1) // block_size

        created = self._request("POST", "/files/create",
                                json={"path": remote_path, "overwrite": True})
        version_id = created["versionId"]
        committed: list[dict] = []
        try:
            for index in sorted(set(blocks) | set(range(first, last + 1))):
                if index < first or index > last:
                    old = blocks[index]                       # bloque intacto: se reutiliza
                    committed.append({"index": index, "blockId": old["blockId"],
                                      "size": old["size"], "sha256": old["sha256"]})
                    continue
                original = bytearray(self._read_block(blocks[index]) if index in blocks else b"")
                block_start = index * block_size
                local_offset = max(0, offset - block_start)
                slice_start = max(0, block_start - offset)
                slice_end = min(len(data), block_start + block_size - offset)
                patch = data[slice_start:slice_end]
                if len(original) < local_offset:
                    original.extend(b"\0" * (local_offset - len(original)))
                original[local_offset:local_offset + len(patch)] = patch
                payload = bytes(original[:block_size])
                sha = hashlib.sha256(payload).hexdigest()
                allocated = self._request("POST", f"/versions/{version_id}/blocks",
                                          json={"index": index})
                pipeline = [n["address"] for n in allocated["pipeline"]]
                self._write_block(pipeline, allocated["blockId"], allocated["token"], payload, sha)
                committed.append({"index": index, "blockId": allocated["blockId"],
                                  "size": len(payload), "sha256": sha})
            return self._request("POST", f"/versions/{version_id}/commit",
                                 json={"blocks": committed})
        except Exception:
            try:
                self._request("POST", f"/versions/{version_id}/abort")
            except DfshaError:
                pass
            raise


_GRPC_OPTIONS = [("grpc.max_send_message_length", 8 * 1024 * 1024),
                 ("grpc.max_receive_message_length", 8 * 1024 * 1024)]


class DfshaFile:
    """Lectura con acceso aleatorio (RF3): open / seek / read / close."""

    def __init__(self, client: DfshaClient, opened: dict) -> None:
        self.client = client
        self.path = opened["path"]
        self.size = opened["size"]
        self.block_size = opened["blockSize"]
        self.version_id = opened["versionId"]
        self._blocks = {b["index"]: b for b in opened["blocks"]}
        self._position = 0

    def seek(self, position: int) -> int:
        self._position = max(0, min(position, self.size))
        return self._position

    def tell(self) -> int:
        return self._position

    def read(self, count: int | None = None) -> bytes:
        remaining = self.size - self._position if count is None else min(count, self.size - self._position)
        out = bytearray()
        while remaining > 0:
            index = self._position // self.block_size
            block = self._blocks.get(index)
            if block is None:
                break
            inner = self._position % self.block_size
            take = min(remaining, self.block_size - inner)
            out.extend(self.client._read_block(block, offset=inner, length=take))
            self._position += take
            remaining -= take
        return bytes(out)

    def close(self) -> None:
        self._blocks = {}

    def __enter__(self) -> "DfshaFile":
        return self

    def __exit__(self, *_) -> None:
        self.close()
