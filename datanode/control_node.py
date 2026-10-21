"""Conversacion del DataNode con los ControlNodes (el plano de control).

Sigue el contrato de controlnode/src/dfsha.proto (servicio ControlService) y
sus dos reglas:

  1. El DataNode se registra y manda el heartbeat a LOS TRES ControlNodes, no
     solo al lider. Los seguidores contestan is_leader = false y sin comandos.
  2. Los comandos (re-replicar o borrar) viajan en la RESPUESTA del heartbeat;
     el ControlNode nunca abre una conexion hacia aca.

Lo unico que no sale del contrato: el comando ReplicateBlock no trae token de
bloque, asi que el nodo origen firma uno con el mismo secreto compartido
(usuario "node:<id>") para que el destino acepte la copia.
"""
import base64
import hashlib
import hmac
import logging
import threading
import time

import grpc
import httpx

from config import VERSION
from pb import dfsha_pb2, dfsha_pb2_grpc

log = logging.getLogger("datanode.control")

TIMEOUT_COPIA = 180.0


class ControlNodes:
    """Habla con los tres ControlNodes a la vez."""

    def __init__(self, config, almacen):
        self.config = config
        self.almacen = almacen
        # Una conexion por ControlNode: direccion -> stub
        self.stubs = {}
        for direccion in config.control_nodes:
            canal = grpc.insecure_channel(direccion)
            self.stubs[direccion] = (canal, dfsha_pb2_grpc.ControlServiceStub(canal))
        # Cual contesto is_leader en el ultimo heartbeat; a ese se le mandan los
        # avisos que tienen que quedar en Raft
        self.lider = None
        self.registrado = False
        # El servidor HTTP le pone aqui una funcion para saber cuantas
        # transferencias hay en curso; mientras tanto, cero.
        self.contador_transferencias = lambda: 0
        self._parar = threading.Event()
        self._hilo = None

    # ========================= registro =========================

    def registrar(self, intentos=20):
        """Se presenta ante los tres ControlNodes. Basta con que uno acepte
        para empezar a trabajar; los demas se enteran en el heartbeat."""
        peticion = dfsha_pb2.RegisterRequest(
            node_id=self.config.node_id,
            host=self.config.host,
            client_port=self.config.client_port,
            peer_port=self.config.peer_port,
            zone=self.config.zone,
            capacity_bytes=self.config.capacity_bytes,
            used_bytes=self.almacen.bytes_usados(),
            version=VERSION,
        )
        for intento in range(1, intentos + 1):
            aceptaron = []
            for direccion, (_, stub) in self.stubs.items():
                try:
                    respuesta = stub.Register(peticion, timeout=10)
                    if respuesta.accepted:
                        aceptaron.append(direccion)
                        if respuesta.heartbeat_seconds > 0:
                            self.config.heartbeat_seconds = respuesta.heartbeat_seconds
                    else:
                        log.warning("%s no acepto el registro: %s", direccion, respuesta.message)
                except grpc.RpcError as error:
                    log.debug("%s no responde (%s)", direccion, error.code())
            if aceptaron:
                self.registrado = True
                log.info("Registrado en %d de %d ControlNodes (%s)",
                         len(aceptaron), len(self.stubs), ", ".join(aceptaron))
                self.reportar_bloques()
                return True
            log.warning("Ningun ControlNode respondio al registro (intento %d/%d)",
                        intento, intentos)
            time.sleep(min(2 * intento, 10))
        return False

    # ========================= avisos =========================

    def _al_lider(self, llamada, descripcion):
        """Manda algo que tiene que quedar en Raft. Se intenta primero con el
        lider conocido y, si no contesta, con los demas."""
        direcciones = ([self.lider] if self.lider in self.stubs else []) + \
                      [d for d in self.stubs if d != self.lider]
        for direccion in direcciones:
            try:
                llamada(self.stubs[direccion][1])
                return True
            except grpc.RpcError as error:
                log.debug("%s no pudo con %s (%s)", direccion, descripcion, error.code())
        log.warning("Ningun ControlNode pudo con %s", descripcion)
        return False

    def reportar_bloques(self, completo=True):
        """Que bloques tiene de verdad este nodo. Sirve para que el ControlNode
        arme el mapa bloque -> replicas y detecte los que se perdieron."""
        bloques = [
            dfsha_pb2.BlockInfo(block_id=b.block_id, size=b.size, sha256=b.sha256)
            for b in self.almacen.listar()
        ]
        peticion = dfsha_pb2.BlockReportRequest(
            node_id=self.config.node_id, full=completo, blocks=bloques)
        if self._al_lider(lambda stub: stub.BlockReport(peticion, timeout=30), "el reporte de bloques"):
            log.info("Reporte de bloques enviado: %d bloques (completo=%s)", len(bloques), completo)

    def avisar_bloque_recibido(self, bloque):
        """Ya tengo esta replica. El ControlNode recien aqui la escribe en Raft,
        asi que se manda antes de contestarle a quien nos mando el bloque."""
        peticion = dfsha_pb2.BlockReceivedRequest(
            block_id=bloque.block_id, node_id=self.config.node_id,
            size=bloque.size, sha256=bloque.sha256)
        self._al_lider(lambda stub: stub.BlockReceived(peticion, timeout=10),
                       f"el aviso del bloque {bloque.block_id}")

    def avisar_bloque_perdido(self, block_id, motivo):
        """Perdi un bloque: checksum malo, disco danado o lo borre porque me lo
        pidieron. motivo es uno de los Reason del contrato."""
        peticion = dfsha_pb2.BlockRemovedRequest(
            block_id=block_id, node_id=self.config.node_id, reason=motivo)
        self._al_lider(lambda stub: stub.BlockRemoved(peticion, timeout=10),
                       f"el aviso de perdida del bloque {block_id}")

    # ======================== heartbeats ========================

    def arrancar(self):
        self._hilo = threading.Thread(target=self._bucle, name="heartbeat", daemon=True)
        self._hilo.start()

    def detener(self):
        self._parar.set()
        for canal, _ in self.stubs.values():
            canal.close()

    def _bucle(self):
        ultimo_reporte = time.time()
        while not self._parar.is_set():
            bloques = self.almacen.listar()
            peticion = dfsha_pb2.HeartbeatRequest(
                node_id=self.config.node_id,
                capacity_bytes=self.config.capacity_bytes,
                used_bytes=sum(b.size for b in bloques),
                block_count=len(bloques),
                active_transfers=self.contador_transferencias(),
            )
            vivos = 0
            for direccion, (_, stub) in self.stubs.items():
                try:
                    respuesta = stub.Heartbeat(peticion, timeout=10)
                    vivos += 1
                    if respuesta.reregister:
                        # Ese ControlNode se reinicio y no nos conoce
                        log.info("%s nos pide registrarnos de nuevo", direccion)
                        self.registrar(intentos=1)
                        continue
                    if respuesta.is_leader:
                        self.lider = direccion
                        for comando in respuesta.commands:
                            self._ejecutar(comando)
                except grpc.RpcError as error:
                    log.debug("heartbeat a %s fallido (%s)", direccion, error.code())
            self.registrado = vivos > 0
            if vivos == 0:
                log.warning("Ningun ControlNode respondio el heartbeat")

            if time.time() - ultimo_reporte > self.config.full_report_seconds:
                self.reportar_bloques()
                ultimo_reporte = time.time()
            self._parar.wait(self.config.heartbeat_seconds)

    # ========================= comandos =========================

    def _ejecutar(self, comando):
        cual = comando.WhichOneof("command")
        if cual == "replicate":
            # En un hilo aparte para no frenar el bucle de heartbeats
            threading.Thread(target=self._replicar, args=(comando.replicate,),
                             daemon=True).start()
        elif cual == "remove":
            for block_id in comando.remove.block_ids:
                if self.almacen.borrar(block_id):
                    log.info("Bloque %s borrado por orden del ControlNode", block_id)
                    self.avisar_bloque_perdido(block_id, dfsha_pb2.BlockRemovedRequest.BORRADO)

    def _replicar(self, orden):
        """Copia un bloque de este nodo a los destinos que pide el ControlNode.
        Los datos van directo entre los dos DataNodes; el que recibe es el que
        avisa con BlockReceived."""
        guardado = self.almacen.meta(orden.block_id)
        if guardado is None:
            log.warning("Me piden replicar %s pero no lo tengo", orden.block_id)
            return

        for destino in orden.targets:
            url = f"http://{destino.host}:{destino.peer_port}/blocks/{orden.block_id}"
            cabeceras = {
                # El comando no trae token, asi que firmamos uno nosotros con el
                # secreto compartido para que el destino acepte la copia
                "X-Block-Token": self._token_interno(orden.block_id),
                "X-Block-Sha256": guardado.sha256,
                "X-Pipeline": "",
                "Content-Type": "application/octet-stream",
            }
            try:
                with httpx.Client(timeout=TIMEOUT_COPIA, trust_env=False) as cliente:
                    respuesta = cliente.put(
                        url, headers=cabeceras,
                        content=self.almacen.leer(orden.block_id,
                                                  chunk_size=self.config.chunk_size),
                    )
                if respuesta.status_code == 200:
                    log.info("Bloque %s copiado a %s", orden.block_id, destino.node_id)
                else:
                    log.warning("%s rechazo el bloque %s: HTTP %d",
                                destino.node_id, orden.block_id, respuesta.status_code)
            except httpx.HTTPError as error:
                log.warning("Fallo la copia de %s hacia %s: %s",
                            orden.block_id, destino.node_id, error)

    def _token_interno(self, block_id, minutos=10):
        vence = int(time.time() * 1000) + minutos * 60 * 1000
        cuerpo = f"{block_id}:node:{self.config.node_id}:write:{vence}"
        firma = hmac.new(self.config.block_secret.encode("utf-8"),
                         cuerpo.encode("utf-8"), hashlib.sha256).digest()
        return cuerpo + ":" + base64.urlsafe_b64encode(firma).decode().rstrip("=")
