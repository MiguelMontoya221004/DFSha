"""Almacenamiento de bloques en el disco local del DataNode.

Cada bloque se guarda en dos archivos dentro de la carpeta de datos:

    <datos>/ab/cd/<blockId>.blk    el contenido
    <datos>/ab/cd/<blockId>.meta   un JSON con tamaño y checksum

Los dos niveles de subcarpetas evitan terminar con miles de archivos en un solo
directorio. La escritura es atómica: primero se escribe un archivo temporal y
solo al final se renombra, así nunca queda un bloque a medias si el proceso se
cae en mitad de una subida.
"""
import hashlib
import json
import os
import shutil
from pathlib import Path


class Bloque:
    """Los datos que el DataNode conoce de un bloque que tiene guardado."""

    def __init__(self, block_id, size, sha256):
        self.block_id = block_id
        self.size = size
        self.sha256 = sha256

    def como_dict(self):
        return {"blockId": self.block_id, "size": self.size, "sha256": self.sha256}


class Almacen:
    def __init__(self, carpeta):
        self.raiz = Path(carpeta)
        self.raiz.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- ubicación

    def _carpeta_de(self, block_id):
        limpio = block_id.replace("/", "_")
        return self.raiz / limpio[:2] / limpio[2:4]

    def ruta_de(self, block_id):
        return self._carpeta_de(block_id) / f"{block_id}.blk"

    def ruta_meta_de(self, block_id):
        return self._carpeta_de(block_id) / f"{block_id}.meta"

    def existe(self, block_id):
        return self.ruta_de(block_id).exists()

    # ------------------------------------------------------------- escritura

    def abrir_escritura(self, block_id):
        return Escritor(self, block_id)

    # -------------------------------------------------------------- lectura

    def leer(self, block_id, desde=0, cuantos=0, chunk_size=1024 * 1024):
        """Devuelve el bloque por trozos. Con 'desde' y 'cuantos' se lee solo un
        rango, que es lo que permite la lectura aleatoria del cliente."""
        ruta = self.ruta_de(block_id)
        if not ruta.exists():
            raise FileNotFoundError(block_id)

        total = ruta.stat().st_size
        faltan = cuantos if cuantos > 0 else total - desde
        with open(ruta, "rb") as archivo:
            archivo.seek(desde)
            while faltan > 0:
                trozo = archivo.read(min(chunk_size, faltan))
                if not trozo:
                    break
                faltan -= len(trozo)
                yield trozo

    def meta(self, block_id):
        ruta = self.ruta_meta_de(block_id)
        if not ruta.exists():
            return None
        datos = json.loads(ruta.read_text(encoding="utf-8"))
        return Bloque(datos["blockId"], int(datos["size"]), datos["sha256"])

    def verificar(self, block_id):
        """Recalcula el checksum del archivo en disco y lo compara con el
        guardado. Sirve para detectar corrupción silenciosa."""
        guardado = self.meta(block_id)
        if guardado is None:
            return False
        resumen = hashlib.sha256()
        for trozo in self.leer(block_id):
            resumen.update(trozo)
        return resumen.hexdigest() == guardado.sha256

    # --------------------------------------------------------------- borrado

    def borrar(self, block_id):
        borrado = False
        for ruta in (self.ruta_de(block_id), self.ruta_meta_de(block_id)):
            if ruta.exists():
                ruta.unlink()
                borrado = True
        return borrado

    # ------------------------------------------------------------ inventario

    def listar(self):
        bloques = []
        for ruta in self.raiz.glob("*/*/*.meta"):
            try:
                datos = json.loads(ruta.read_text(encoding="utf-8"))
                bloques.append(Bloque(datos["blockId"], int(datos["size"]), datos["sha256"]))
            except (OSError, ValueError, KeyError):
                continue  # un .meta corrupto no debe tumbar el inventario
        return bloques

    def bytes_usados(self):
        return sum(ruta.stat().st_size for ruta in self.raiz.glob("*/*/*.blk"))

    def bytes_libres(self):
        return shutil.disk_usage(self.raiz).free


class Escritor:
    """Escribe un bloque trozo a trozo y va calculando su SHA-256 al pasar."""

    def __init__(self, almacen, block_id):
        self.almacen = almacen
        self.block_id = block_id
        carpeta = almacen._carpeta_de(block_id)
        carpeta.mkdir(parents=True, exist_ok=True)
        self.temporal = carpeta / f"{block_id}.{os.getpid()}.tmp"
        self.archivo = open(self.temporal, "wb")
        self.resumen = hashlib.sha256()
        self.size = 0

    def escribir(self, trozo):
        self.archivo.write(trozo)
        self.resumen.update(trozo)
        self.size += len(trozo)

    def cancelar(self):
        try:
            self.archivo.close()
        finally:
            self.temporal.unlink(missing_ok=True)

    def confirmar(self, sha256_esperado=None):
        """Cierra el archivo, valida el checksum y lo deja en su sitio final."""
        self.archivo.flush()
        os.fsync(self.archivo.fileno())
        self.archivo.close()

        sha = self.resumen.hexdigest()
        if sha256_esperado and sha256_esperado != sha:
            self.temporal.unlink(missing_ok=True)
            raise ValueError(
                f"checksum invalido para {self.block_id}: "
                f"se esperaba {sha256_esperado} y llego {sha}"
            )

        os.replace(self.temporal, self.almacen.ruta_de(self.block_id))  # atómico
        self.almacen.ruta_meta_de(self.block_id).write_text(
            json.dumps({"blockId": self.block_id, "size": self.size, "sha256": sha}),
            encoding="utf-8",
        )
        return Bloque(self.block_id, self.size, sha)
