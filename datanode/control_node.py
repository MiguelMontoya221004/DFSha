"""Conversación del DataNode con el ControlNode (el plano de control).

Esta parte sigue siendo gRPC, como quedó en la especificación de comunicaciones:
el DataNode se registra, manda un heartbeat cada pocos segundos con su estado y
reporta los bloques que tiene. El ControlNode no abre conexiones hacia acá: sus
órdenes (replicar o borrar un bloque) viajan de vuelta dentro de la respuesta
del heartbeat.
"""
import logging
import threading
import time

import grpc
import httpx

from pb import dfsha_pb2, dfsha_pb2_grpc

log = logging.getLogger("datanode.control")


class ControlNodeCliente:
    def __init__(self, config, almacen):
        self.config = config
        self.almacen = almacen
        self.canal = grpc.insecure_channel(config.control_node)
        self.stub = dfsha_pb2_grpc.NodeControlStub(self.canal)
        self.registrado = False
        # El servidor HTTP le pone aquí una función para saber cuántas
        # transferencias hay en curso; mientras tanto, cero.
        self.contador_transferencias = lambda: 0
        self._parar = threading.Event()
        self._hilo = None

    # -------------------------------------------------------------- registro

    def registrarse(self, intentos=30):
        """Se presenta ante el ControlNode diciendo quién es, dónde está y
        cuánto espacio tiene. Reintenta porque al arrancar con Docker el
        ControlNode puede no estar listo todavía."""
        for intento in range(1, intentos + 1):
            try:
                respuesta = self.stub.Register(
                    dfsha_pb2.RegisterRequest(
                        node_id=self.config.node_id,
                        address=self.config.advertise,
                        zone=self.config.zone,
                        capacity_bytes=self.config.capacity_bytes,
                    ),
                    timeout=10,
                )
                if respuesta.accepted:
                    self.registrado = True
                    log.info("Registrado en el ControlNode %s (cluster %s)",
                             self.config.control_node, respuesta.cluster_id)
                    self.enviar_reporte_completo()
                    return True
            except grpc.RpcError as error:
                log.warning("No se pudo registrar (intento %d/%d): %s",
                            intento, intentos, error.code())
            time.sleep(min(2 * intento, 10))
        return False

    # -------------------------------------------------------------- reportes

    def enviar_reporte_completo(self):
        """Le dice al ControlNode todos los bloques que tiene este nodo. Es como
        se reconstruye el mapa bloque→réplicas, que el ControlNode no persiste."""
        bloques = [
            dfsha_pb2.BlockInfo(block_id=b.block_id, size=b.size, sha256=b.sha256)
            for b in self.almacen.listar()
        ]
        try:
            self.stub.BlockReport(
                dfsha_pb2.BlockReportRequest(
                    node_id=self.config.node_id, blocks=bloques, full_report=True),
                timeout=30,
            )
            log.info("Reporte completo enviado: %d bloques", len(bloques))
        except grpc.RpcError as error:
            log.warning("Fallo el reporte completo: %s", error.code())

    def avisar_bloque_recibido(self, bloque):
        """Avisa de un bloque nuevo apenas queda guardado."""
        try:
            self.stub.BlockReceived(
                dfsha_pb2.BlockReceivedRequest(
                    node_id=self.config.node_id,
                    block=dfsha_pb2.BlockInfo(block_id=bloque.block_id,
                                              size=bloque.size,
                                              sha256=bloque.sha256),
                ),
                timeout=10,
            )
        except grpc.RpcError as error:
            log.warning("No se pudo avisar del bloque %s: %s", bloque.block_id, error.code())

    def reportar_corrupto(self, block_id, detalle):
        try:
            self.stub.ReportCorrupt(
                dfsha_pb2.CorruptBlockRequest(reporter=self.config.node_id,
                                              node_id=self.config.node_id,
                                              block_id=block_id, detail=detalle),
                timeout=10,
            )
        except grpc.RpcError:
            pass

    # ------------------------------------------------------------ heartbeats

    def arrancar(self):
        self._hilo = threading.Thread(target=self._bucle, name="heartbeat", daemon=True)
        self._hilo.start()

    def detener(self):
        self._parar.set()
        self.canal.close()

    def _bucle(self):
        ultimo_reporte = time.time()
        while not self._parar.is_set():
            try:
                bloques = self.almacen.listar()
                respuesta = self.stub.Heartbeat(
                    dfsha_pb2.HeartbeatRequest(
                        node_id=self.config.node_id,
                        used_bytes=sum(b.size for b in bloques),
                        free_bytes=self.almacen.bytes_libres(),
                        active_transfers=self.contador_transferencias(),
                        block_count=len(bloques),
                    ),
                    timeout=10,
                )
                if not respuesta.registered:
                    # El ControlNode nos dio por muertos o se reinició
                    log.warning("El ControlNode no nos reconoce: volviendo a registrarse")
                    self.registrado = False
                    self.registrarse(intentos=3)
                else:
                    self.registrado = True
                    for orden in respuesta.commands:
                        self._ejecutar(orden)

                if time.time() - ultimo_reporte > self.config.full_report_seconds:
                    self.enviar_reporte_completo()
                    ultimo_reporte = time.time()
            except grpc.RpcError as error:
                log.warning("Heartbeat fallido: %s", error.code())
            self._parar.wait(self.config.heartbeat_seconds)

    # --------------------------------------------------------------- órdenes

    def _ejecutar(self, orden):
        cual = orden.WhichOneof("cmd")
        if cual == "replicate":
            # En un hilo aparte para no frenar el bucle de heartbeats
            threading.Thread(target=self._replicar, args=(orden.replicate,),
                             daemon=True).start()
        elif cual == "delete":
            for block_id in orden.delete.block_ids:
                if self.almacen.borrar(block_id):
                    log.info("Bloque %s borrado por orden del ControlNode", block_id)

    def _replicar(self, orden):
        """Copia un bloque de este nodo a otro DataNode. Es la re-replicación:
        cuando alguien se cae, el ControlNode pide a quien tenga una copia que
        se la pase a un nodo sano. Los datos van directo entre los dos nodos."""
        guardado = self.almacen.meta(orden.block_id)
        if guardado is None:
            log.warning("Me piden replicar %s pero no lo tengo", orden.block_id)
            return

        url = f"http://{orden.target_address}/blocks/{orden.block_id}"
        cabeceras = {"X-Block-Token": orden.block_token,
                     "X-Block-Sha256": guardado.sha256,
                     "X-Pipeline": "",
                     "Content-Type": "application/octet-stream"}
        try:
            with httpx.Client(timeout=180.0, trust_env=False) as cliente:
                respuesta = cliente.put(
                    url, headers=cabeceras,
                    content=self.almacen.leer(orden.block_id,
                                              chunk_size=self.config.chunk_size),
                )
            if respuesta.status_code == 200:
                log.info("Bloque %s replicado hacia %s", orden.block_id, orden.target_address)
            else:
                log.warning("El destino %s rechazo el bloque %s: HTTP %d",
                            orden.target_address, orden.block_id, respuesta.status_code)
        except httpx.HTTPError as error:
            log.warning("Fallo la replicacion de %s hacia %s: %s",
                        orden.block_id, orden.target_address, error)
