"""Almacenamiento local de bloques.

Un bloque se guarda como dos archivos dentro de data_dir, en subdirectorios de
dos niveles para no tener millones de entradas en una sola carpeta:

    <data>/ab/cd/<blockId>.blk    contenido
    <data>/ab/cd/<blockId>.meta   JSON: tamaño, sha256, fecha

La escritura es atómica: se escribe un .tmp y al final se renombra, de modo que
nunca queda un .blk a medias si el proceso muere.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass
class StoredBlock:
    block_id: str
    size: int
    sha256: str


class BlockStore:
    def __init__(self, data_dir: str) -> None:
        self.root = Path(data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # ------------------------------------------------------------- ubicación
    def _dir_for(self, block_id: str) -> Path:
        clean = block_id.replace("/", "_")
        return self.root / clean[:2] / clean[2:4]

    def path_for(self, block_id: str) -> Path:
        return self._dir_for(block_id) / f"{block_id}.blk"

    def meta_path_for(self, block_id: str) -> Path:
        return self._dir_for(block_id) / f"{block_id}.meta"

    def exists(self, block_id: str) -> bool:
        return self.path_for(block_id).exists()

    # ------------------------------------------------------------- escritura
    class Writer:
        """Escribe un bloque en un temporal y calcula su SHA-256 al pasar."""

        def __init__(self, store: "BlockStore", block_id: str) -> None:
            self.store = store
            self.block_id = block_id
            self.directory = store._dir_for(block_id)
            self.directory.mkdir(parents=True, exist_ok=True)
            self.tmp = self.directory / f"{block_id}.{os.getpid()}.tmp"
            self.handle = open(self.tmp, "wb")
            self.digest = hashlib.sha256()
            self.size = 0

        def write(self, chunk: bytes) -> None:
            self.handle.write(chunk)
            self.digest.update(chunk)
            self.size += len(chunk)

        def abort(self) -> None:
            try:
                self.handle.close()
            finally:
                self.tmp.unlink(missing_ok=True)

        def commit(self, expected_sha256: str | None) -> StoredBlock:
            self.handle.flush()
            os.fsync(self.handle.fileno())
            self.handle.close()
            sha = self.digest.hexdigest()
            if expected_sha256 and expected_sha256 != sha:
                self.tmp.unlink(missing_ok=True)
                raise ValueError(
                    f"checksum inválido para {self.block_id}: esperado {expected_sha256}, calculado {sha}"
                )
            final = self.store.path_for(self.block_id)
            os.replace(self.tmp, final)          # renombrado atómico
            self.store.meta_path_for(self.block_id).write_text(
                json.dumps({"blockId": self.block_id, "size": self.size, "sha256": sha}),
                encoding="utf-8",
            )
            return StoredBlock(self.block_id, self.size, sha)

    def writer(self, block_id: str) -> "BlockStore.Writer":
        return BlockStore.Writer(self, block_id)

    # -------------------------------------------------------------- lectura
    def read(self, block_id: str, offset: int = 0, length: int = 0,
             chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        path = self.path_for(block_id)
        if not path.exists():
            raise FileNotFoundError(block_id)
        remaining = length if length > 0 else path.stat().st_size - offset
        with open(path, "rb") as handle:
            handle.seek(offset)
            while remaining > 0:
                chunk = handle.read(min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    def meta(self, block_id: str) -> StoredBlock | None:
        meta_path = self.meta_path_for(block_id)
        if not meta_path.exists():
            return None
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        return StoredBlock(data["blockId"], int(data["size"]), data["sha256"])

    def verify(self, block_id: str) -> bool:
        """Recalcula el checksum en disco (scrubbing)."""
        stored = self.meta(block_id)
        if stored is None:
            return False
        digest = hashlib.sha256()
        for chunk in self.read(block_id):
            digest.update(chunk)
        return digest.hexdigest() == stored.sha256

    # -------------------------------------------------------------- borrado
    def delete(self, block_id: str) -> bool:
        removed = False
        for path in (self.path_for(block_id), self.meta_path_for(block_id)):
            if path.exists():
                path.unlink()
                removed = True
        return removed

    # ------------------------------------------------------------ inventario
    def list_blocks(self) -> list[StoredBlock]:
        out: list[StoredBlock] = []
        for meta_path in self.root.glob("*/*/*.meta"):
            try:
                data = json.loads(meta_path.read_text(encoding="utf-8"))
                out.append(StoredBlock(data["blockId"], int(data["size"]), data["sha256"]))
            except (OSError, ValueError, KeyError):
                continue
        return out

    def used_bytes(self) -> int:
        return sum(p.stat().st_size for p in self.root.glob("*/*/*.blk"))

    def free_bytes(self) -> int:
        return shutil.disk_usage(self.root).free
