"""Arranque del DataNode.

    python datanode/main.py

Levanta la API HTTP de bloques y, en paralelo, el hilo que habla con los tres
ControlNodes y el servidor gRPC interno para recibir réplicas.
"""
import logging
import sys
import threading
from pathlib import Path

import grpc
from concurrent import futures
from pb import dfsha_pb2, dfsha_pb2_grpc

# Permite ejecutar el archivo directamente, sin instalar nada
sys.path.insert(0, str(Path(__file__).resolve().parent))

import uvicorn

from config import Config
from control_node import ControlNodes
from server import crear_app
from storage import Almacen

log = logging.getLogger("datanode")

class DataNodeServicer(dfsha_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, almacen, control):
        self.almacen = almacen
        self.control = control

    def TransferBlock(self, request, context):
        try:
            # 1. Guardar el bloque localmente. Se usa la misma API que el
            # camino HTTP: abrir la escritura, escribir los trozos y
            # confirmar verificando el checksum.
            escritor = self.almacen.abrir_escritura(request.block_id)
            try:
                escritor.escribir(request.data)
                escritor.confirmar(request.checksum or None)
            except Exception:
                escritor.cancelar()   # no dejar un bloque a medias en disco
                raise
            
            # 2. Pipeline: Si hay más nodos en la cadena, reenviarlo al siguiente
            if request.pipeline:
                siguiente = request.pipeline[0]
                resto = list(request.pipeline[1:])
                
                with grpc.insecure_channel(siguiente, options=[
                    ("grpc.max_send_message_length", 16 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 16 * 1024 * 1024)]) as canal:
                    stub = dfsha_pb2_grpc.DataNodeServiceStub(canal)
                    peticion = dfsha_pb2.TransferRequest(
                        block_id=request.block_id,
                        token=request.token,
                        checksum=request.checksum,
                        pipeline=resto,
                        data=request.data
                    )
                    respuesta = stub.TransferBlock(peticion, timeout=180)
                    
                    if not respuesta.success:
                        return dfsha_pb2.TransferResponse(
                            success=False, 
                            message=f"Fallo reenviando a {siguiente}: {respuesta.message}"
                        )
            
            return dfsha_pb2.TransferResponse(success=True, message="Bloque replicado con éxito")
        except Exception as e:
            log.error("Error en TransferBlock gRPC: %s", e)
            return dfsha_pb2.TransferResponse(success=False, message=str(e))

def iniciar_servidor_grpc(almacen, control, puerto_interno):
    servidor = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        # Un bloque son 4 MiB EXACTOS y el mensaje lleva ademas el token y el
        # checksum, asi que siempre supera el tope por defecto de gRPC (4 MiB).
        options=[("grpc.max_send_message_length", 16 * 1024 * 1024),
                 ("grpc.max_receive_message_length", 16 * 1024 * 1024)],
    )
    dfsha_pb2_grpc.add_DataNodeServiceServicer_to_server(DataNodeServicer(almacen, control), servidor)
    servidor.add_insecure_port(f'[::]:{puerto_interno}')
    servidor.start()
    log.info("Servidor gRPC interno escuchando en puerto %s", puerto_interno)
    return servidor

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
    def presentarse():
        if not control.registrar():
            log.error("No fue posible registrarse en %s; se seguira reintentando",
                      ", ".join(config.control_nodes))
        control.arrancar()

    threading.Thread(target=presentarse, name="registro", daemon=True).start()

    # LEVANTAR EL SERVIDOR GRPC INTERNO PARA REPLICACIÓN
    servidor_grpc = iniciar_servidor_grpc(almacen, control, config.peer_port)

    try:
        # LEVANTAR LA API REST PARA EL CLIENTE
        uvicorn.run(app, host="0.0.0.0", port=config.client_port, log_level="warning")
    finally:
        log.info("Apagando DataNode...")
        servidor_grpc.stop(0)
        control.detener()

if __name__ == "__main__":
    main()