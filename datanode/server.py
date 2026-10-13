"""API HTTP del DataNode: el plano de datos de DFSha.

Por aquí pasan los bytes de los archivos. El cliente sube y baja bloques, y los
DataNodes se pasan copias entre ellos para mantener las tres réplicas. El
ControlNode nunca toca estos datos: solo reparte permisos (tokens) y decide
quién guarda qué.

    PUT    /blocks/{id}        sube un bloque (y lo reenvía al siguiente nodo)
    GET    /blocks/{id}        lo descarga, entero o por rango
    GET    /blocks/{id}/meta   tamaño y checksum
    DELETE /blocks/{id}        lo borra
    GET    /status             estado del nodo
    GET    /health             ¿está vivo?
"""
import logging
import threading

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

import tokens

log = logging.getLogger("datanode.http")

TIMEOUT_REENVIO = 180.0


class Contador:
    """Cuenta las transferencias en curso; el ControlNode lo usa para repartir
    la carga entre los DataNodes."""

    def __init__(self):
        self._valor = 0
        self._candado = threading.Lock()

    def entrar(self):
        with self._candado:
            self._valor += 1

    def salir(self):
        with self._candado:
            self._valor -= 1

    @property
    def valor(self):
        with self._candado:
            return self._valor


def leer_token(request: Request):
    """El token puede venir en la cabecera o como parámetro, para que sea
    cómodo tanto desde código como desde el navegador o curl."""
    return request.headers.get("X-Block-Token") or request.query_params.get("token", "")


def crear_app(config, almacen, control):
    app = FastAPI(title=f"DFSha DataNode {config.node_id}", version="0.3.0")
    transferencias = Contador()
    control.contador_transferencias = lambda: transferencias.valor

    def exigir_permiso(request, block_id, modo):
        valido, motivo = tokens.verificar(
            leer_token(request), block_id, modo, config.block_secret
        )
        if not valido:
            log.warning("Acceso rechazado al bloque %s: %s", block_id, motivo)
            raise HTTPException(status_code=403,
                                detail={"error": "TOKEN_INVALIDO", "message": motivo})

    # ------------------------------------------------------------- escritura

    @app.put("/blocks/{block_id}")
    async def subir_bloque(block_id: str, request: Request):
        """Recibe el contenido del bloque y, si quedan nodos en el pipeline, se
        lo pasa al siguiente. Así el cliente sube los datos una sola vez aunque
        se guarden tres copias."""
        exigir_permiso(request, block_id, tokens.MODO_ESCRITURA)

        sha_esperado = request.headers.get("X-Block-Sha256")
        siguientes = [d.strip() for d in request.headers.get("X-Pipeline", "").split(",") if d.strip()]

        transferencias.entrar()
        escritor = almacen.abrir_escritura(block_id)
        try:
            async for trozo in request.stream():
                if trozo:
                    escritor.escribir(trozo)
            bloque = escritor.confirmar(sha_esperado)
        except ValueError as error:          # el checksum no coincidió
            log.error("%s", error)
            return JSONResponse(status_code=422,
                                content={"ok": False, "error": "CHECKSUM_INVALIDO",
                                         "message": str(error)})
        except Exception as error:           # cualquier otro fallo: no dejar basura
            escritor.cancelar()
            log.exception("Error recibiendo el bloque %s", block_id)
            raise HTTPException(status_code=500,
                                detail={"error": "ERROR_INTERNO", "message": str(error)})
        finally:
            transferencias.salir()

        guardado_en = [config.node_id]
        if siguientes:
            guardado_en += await reenviar(request, bloque, siguientes)

        # Se avisa al ControlNode antes de responder: cuando el cliente reciba
        # este "ok", el ControlNode ya sabe dónde está la réplica y el commit
        # del archivo no se le adelanta.
        control.avisar_bloque_recibido(bloque)

        log.info("Bloque %s guardado (%d bytes) · replicas: %s",
                 block_id, bloque.size, ", ".join(guardado_en))
        return {"ok": True, "blockId": bloque.block_id, "size": bloque.size,
                "sha256": bloque.sha256, "storedOn": guardado_en}

    async def reenviar(request, bloque, siguientes):
        """Manda el bloque al siguiente DataNode del pipeline con el resto de la
        cadena en la cabecera, para que él haga lo mismo."""
        destino = siguientes[0]
        cabeceras = {
            "X-Block-Token": leer_token(request),
            "X-Block-Sha256": bloque.sha256,
            "X-Pipeline": ",".join(siguientes[1:]),
            "Content-Type": "application/octet-stream",
        }
        url = f"http://{destino}/blocks/{bloque.block_id}"

        async def cuerpo():
            # httpx necesita un generador asincrono para enviar el bloque por
            # trozos sin cargarlo entero en memoria
            for trozo in almacen.leer(bloque.block_id, chunk_size=config.chunk_size):
                yield trozo

        try:
            # trust_env=False: el trafico entre nodos del cluster nunca pasa por
            # un proxy, aunque el entorno tenga variables HTTP_PROXY
            async with httpx.AsyncClient(timeout=TIMEOUT_REENVIO, trust_env=False) as cliente:
                respuesta = await cliente.put(url, headers=cabeceras, content=cuerpo())
            if respuesta.status_code == 200:
                return respuesta.json().get("storedOn", [])
            log.warning("El DataNode %s rechazo el bloque %s: HTTP %d",
                        destino, bloque.block_id, respuesta.status_code)
        except httpx.HTTPError as error:
            # Que falle la cadena no invalida nuestra copia: el ControlNode
            # repondrá las réplicas que falten con su re-replicación.
            log.warning("No se pudo reenviar el bloque %s a %s: %s",
                        bloque.block_id, destino, error)
        return []

    # --------------------------------------------------------------- lectura

    @app.get("/blocks/{block_id}")
    def bajar_bloque(block_id: str, request: Request, offset: int = 0, length: int = 0):
        """Devuelve el bloque completo o solo un rango. El rango se puede pedir
        con los parámetros offset y length o con la cabecera Range."""
        exigir_permiso(request, block_id, tokens.MODO_LECTURA)

        guardado = almacen.meta(block_id)
        if guardado is None or not almacen.existe(block_id):
            raise HTTPException(status_code=404,
                                detail={"error": "BLOQUE_NO_ENCONTRADO",
                                        "message": f"el bloque {block_id} no esta en este nodo"})

        estado = 200
        cabeceras = {"X-Block-Sha256": guardado.sha256,
                     "X-Block-Size": str(guardado.size)}

        rango = request.headers.get("Range")
        if rango and rango.startswith("bytes="):
            desde_txt, _, hasta_txt = rango[6:].partition("-")
            offset = int(desde_txt or 0)
            length = (int(hasta_txt) - offset + 1) if hasta_txt else 0
            estado = 206
            ultimo = offset + (length or guardado.size - offset) - 1
            cabeceras["Content-Range"] = f"bytes {offset}-{ultimo}/{guardado.size}"

        if offset < 0 or offset > guardado.size:
            raise HTTPException(status_code=416,
                                detail={"error": "RANGO_INVALIDO",
                                        "message": "el desplazamiento esta fuera del bloque"})

        trozos = almacen.leer(block_id, offset, length, config.chunk_size)
        transferencias.entrar()

        def contenido():
            try:
                for trozo in trozos:
                    yield trozo
            finally:
                transferencias.salir()

        return StreamingResponse(contenido(), status_code=estado,
                                 media_type="application/octet-stream",
                                 headers=cabeceras)

    @app.get("/blocks/{block_id}/meta")
    def meta_bloque(block_id: str, request: Request):
        exigir_permiso(request, block_id, tokens.MODO_LECTURA)
        guardado = almacen.meta(block_id)
        if guardado is None:
            raise HTTPException(status_code=404,
                                detail={"error": "BLOQUE_NO_ENCONTRADO",
                                        "message": f"el bloque {block_id} no esta en este nodo"})
        return guardado.como_dict()

    @app.delete("/blocks/{block_id}")
    def borrar_bloque(block_id: str, request: Request):
        exigir_permiso(request, block_id, tokens.MODO_ESCRITURA)
        return {"ok": almacen.borrar(block_id), "blockId": block_id}

    # ---------------------------------------------------------------- estado

    @app.get("/health")
    def salud():
        return {"status": "ok", "nodeId": config.node_id,
                "registrado": control.registrado}

    @app.get("/status")
    def estado():
        bloques = almacen.listar()
        return {"nodeId": config.node_id,
                "zone": config.zone,
                "address": config.advertise,
                "capacityBytes": config.capacity_bytes,
                "usedBytes": sum(b.size for b in bloques),
                "freeBytes": almacen.bytes_libres(),
                "blockCount": len(bloques),
                "activeTransfers": transferencias.valor,
                "controlNode": config.control_node,
                "registrado": control.registrado}

    return app
