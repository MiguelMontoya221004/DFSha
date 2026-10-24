"""API HTTP del DataNode: el plano de datos de DFSha.

Por aqui pasan los bytes de los archivos. El cliente sube y baja bloques, y los
DataNodes se pasan copias entre ellos para mantener las tres replicas. El
ControlNode nunca toca estos datos: solo dice en que DataNodes va cada bloque y
entrega el token firmado que autoriza la operacion.

    PUT    /blocks/{id}        sube un bloque (y lo reenvia al siguiente nodo)
    GET    /blocks/{id}        lo baja, entero o por rango
    GET    /blocks/{id}/meta   tamano y checksum
    DELETE /blocks/{id}        lo borra
    GET    /status             estado del nodo
    GET    /health             vivo / caido

Los errores salen como {"error": "..."}, igual que los del ControlNode.
"""
import logging
import threading

import grpc
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

import tokens
from config import VERSION
from pb import dfsha_pb2, dfsha_pb2_grpc

log = logging.getLogger("datanode.http")

TIMEOUT_PIPELINE = 180.0


class Contador:
    """Cuenta las transferencias en curso. Va en el heartbeat: el ControlNode
    lo usa para mandar al cliente a la replica menos cargada (RNF5)."""

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
    """El token puede venir en la cabecera o como parametro, para que sea comodo
    tanto desde codigo como desde curl o el navegador."""
    return request.headers.get("X-Block-Token") or request.query_params.get("token", "")


def crear_app(config, almacen, control):
    app = FastAPI(title=f"DFSha DataNode {config.node_id}", version=VERSION)
    transferencias = Contador()
    control.contador_transferencias = lambda: transferencias.valor

    def exigir_permiso(request, block_id, operacion):
        valido, motivo = tokens.verificar(
            leer_token(request), block_id, operacion, config.block_secret)
        if not valido:
            log.warning("Acceso rechazado al bloque %s: %s", block_id, motivo)
            raise HTTPException(status_code=403, detail=motivo)

    # ------------------------------- escritura -------------------------------

    @app.put("/blocks/{block_id}")
    async def subir_bloque(block_id: str, request: Request):
        """Recibe el contenido del bloque y, si quedan nodos en el pipeline, se
        lo pasa al siguiente por gRPC. Asi el cliente sube los datos una sola vez."""
        exigir_permiso(request, block_id, tokens.ESCRIBIR)

        checksum_esperado = request.headers.get("X-Block-Sha256")
        siguientes = [d.strip() for d in request.headers.get("X-Pipeline", "").split(",") if d.strip()]

        transferencias.entrar()
        escritor = almacen.abrir_escritura(block_id)
        try:
            async for trozo in request.stream():
                if trozo:
                    escritor.escribir(trozo)
            bloque = escritor.confirmar(checksum_esperado)
        except ValueError as error:          # el checksum no coincidio
            log.error("%s", error)
            return JSONResponse(status_code=422, content={"error": str(error)})
        except Exception as error:           # cualquier otro fallo: no dejar basura
            escritor.cancelar()
            log.exception("Error recibiendo el bloque %s", block_id)
            raise HTTPException(status_code=500, detail=str(error))
        finally:
            transferencias.salir()

        guardado_en = [config.node_id]
        if siguientes:
            guardado_en += await reenviar(request, bloque, siguientes)

        # Se avisa al ControlNode ANTES de contestar: cuando el cliente reciba
        # este ok, el ControlNode ya escribio la replica en Raft y el commit no
        # se le adelanta.
        control.avisar_bloque_recibido(bloque)

        log.info("Bloque %s guardado (%d bytes) · replicas: %s",
                 block_id, bloque.size, ", ".join(guardado_en))
        return {"ok": True, "block_id": bloque.block_id, "size": bloque.size,
                "checksum": bloque.sha256, "stored_on": guardado_en}

    async def reenviar(request, bloque, siguientes):
        destino = siguientes[0]
        resto_pipeline = siguientes[1:]
        
        # Leemos el bloque que acabamos de guardar en disco
        datos_bloque = b"".join(almacen.leer(bloque.block_id, chunk_size=config.chunk_size))

        # Conexión gRPC asíncrona hacia el siguiente DataNode
        async with grpc.aio.insecure_channel(destino) as canal:
            stub = dfsha_pb2_grpc.DataNodeServiceStub(canal)
            
            peticion = dfsha_pb2.TransferRequest(
                block_id=bloque.block_id,
                token=request.headers.get("X-Block-Token", ""),
                checksum=bloque.sha256,
                pipeline=resto_pipeline,
                data=datos_bloque
            )
            
            respuesta = await stub.TransferBlock(peticion, timeout=TIMEOUT_PIPELINE)
            
            if not respuesta.success:
                raise HTTPException(status_code=500, detail=f"Fallo en pipeline hacia {destino}: {respuesta.message}")
        
        # Devolvemos los nodos donde logramos guardar para agregarlos a la respuesta
        nodos_exitosos = [destino.split(":")[0]]
        if resto_pipeline:
            nodos_exitosos.extend(resto_pipeline)
        return nodos_exitosos

    # -------------------------------- lectura --------------------------------

    @app.get("/blocks/{block_id}")
    def bajar_bloque(block_id: str, request: Request, offset: int = 0, length: int = 0):
        """Devuelve el bloque completo o solo un rango."""
        exigir_permiso(request, block_id, tokens.LEER)

        guardado = almacen.meta(block_id)
        if guardado is None or not almacen.existe(block_id):
            raise HTTPException(status_code=404, detail=f"el bloque {block_id} no esta en este nodo")

        estado = 200
        cabeceras = {"X-Block-Sha256": guardado.sha256, "X-Block-Size": str(guardado.size)}

        rango = request.headers.get("Range")
        if rango and rango.startswith("bytes="):
            desde_txt, _, hasta_txt = rango[6:].partition("-")
            offset = int(desde_txt or 0)
            length = (int(hasta_txt) - offset + 1) if hasta_txt else 0
            estado = 206
            ultimo = offset + (length or guardado.size - offset) - 1
            cabeceras["Content-Range"] = f"bytes {offset}-{ultimo}/{guardado.size}"

        if offset < 0 or offset > guardado.size:
            raise HTTPException(status_code=416, detail="el desplazamiento esta fuera del bloque")

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
        exigir_permiso(request, block_id, tokens.LEER)
        guardado = almacen.meta(block_id)
        if guardado is None:
            raise HTTPException(status_code=404, detail=f"el bloque {block_id} no esta en este nodo")
        return {"block_id": guardado.block_id, "size": guardado.size, "checksum": guardado.sha256}

    @app.delete("/blocks/{block_id}")
    def borrar_bloque(block_id: str, request: Request):
        exigir_permiso(request, block_id, tokens.ESCRIBIR)
        return {"ok": almacen.borrar(block_id), "block_id": block_id}

    # --------------------------------- estado --------------------------------

    @app.get("/health")
    def salud():
        return {"estado": "ok", "node_id": config.node_id, "registrado": control.registrado}

    @app.get("/status")
    def estado():
        bloques = almacen.listar()
        return {"node_id": config.node_id,
                "zone": config.zone,
                "host": config.host,
                "client_port": config.client_port,
                "peer_port": config.peer_port,
                "version": VERSION,
                "capacity_bytes": config.capacity_bytes,
                "used_bytes": sum(b.size for b in bloques),
                "free_bytes": almacen.bytes_libres(),
                "block_count": len(bloques),
                "active_transfers": transferencias.valor,
                "control_nodes": config.control_nodes,
                "registrado": control.registrado}

    @app.exception_handler(HTTPException)
    async def error_uniforme(request: Request, exc: HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})

    return app