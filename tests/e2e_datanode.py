#!/usr/bin/env python3
"""Pruebas end-to-end del DataNode HTTP de DFSha.

Levanta un clúster completo (1 ControlNode + 4 DataNodes), hace todo el ciclo de
un archivo usando solo HTTP —exactamente como lo hará el cliente nuevo, que solo
depende de `requests`— y apaga todo al terminar.

    python3 tests/e2e_datanode.py            # levanta, prueba y apaga
    python3 tests/e2e_datanode.py --keep     # deja el cluster arriba

Sirve también de referencia: cada paso es la llamada HTTP que el cliente tiene
que hacer, en el mismo orden.
"""
import argparse
import hashlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import requests

RAIZ = Path(__file__).resolve().parents[1]
CONTROL = os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8080")
API = CONTROL + "/api/v1"
SECRETO = "dfsha-block-secret-pruebas"
ZONAS = ["zona-a", "zona-a", "zona-b", "zona-c"]

resultados = []


def revisar(nombre, condicion, detalle=""):
    resultados.append((nombre, bool(condicion), detalle))
    marca = "\033[92mPASS\033[0m" if condicion else "\033[91mFALLA\033[0m"
    print(f"  [{marca}] {nombre}" + (f" — {detalle}" if detalle else ""), flush=True)


class Cluster:
    def __init__(self, carpeta, datanodes=4):
        self.carpeta = carpeta
        self.datanodes = datanodes
        self.procesos = {}

    def levantar(self):
        jar = next((RAIZ / "controlnode" / "target").glob("dfsha-controlnode-*.jar"), None)
        if jar is None:
            raise SystemExit("Falta el jar del ControlNode: ejecute 'mvn package' en controlnode/")
        entorno = os.environ | {"DFSHA_BLOCK_SECRET": SECRETO,
                                "DFSHA_NODE_DEAD_MS": "8000"}
        self.procesos["cn1"] = self._lanzar(["java", "-jar", str(jar)], entorno, "cn1")
        self._esperar_control()

        for i in range(1, self.datanodes + 1):
            puerto = 50050 + i
            entorno_dn = entorno | {
                "DFSHA_NODE_ID": f"dn{i}",
                "DFSHA_DN_PORT": str(puerto),
                "DFSHA_DN_ADVERTISE": f"localhost:{puerto}",
                "DFSHA_ZONE": ZONAS[(i - 1) % len(ZONAS)],
                "DFSHA_DATA_DIR": str(self.carpeta / f"dn{i}"),
                "DFSHA_CONTROLNODE": "localhost:9090",
                "DFSHA_HEARTBEAT_S": "1",
            }
            self.procesos[f"dn{i}"] = self._lanzar(
                [sys.executable, str(RAIZ / "datanode" / "main.py")], entorno_dn, f"dn{i}")
        self._esperar_datanodes(self.datanodes)

    def _lanzar(self, comando, entorno, nombre):
        salida = open(self.carpeta / f"{nombre}.log", "wb")
        return subprocess.Popen(comando, env=entorno, stdout=salida,
                                stderr=subprocess.STDOUT, cwd=str(RAIZ),
                                start_new_session=True)

    def _esperar_control(self, limite=60):
        fin = time.time() + limite
        while time.time() < fin:
            try:
                if requests.get(f"{API}/health", timeout=2).ok:
                    return
            except requests.RequestException:
                time.sleep(1)
        raise SystemExit("El ControlNode no respondio a /health")

    def _esperar_datanodes(self, cuantos, limite=60):
        fin = time.time() + limite
        vivos = 0
        while time.time() < fin:
            vivos = requests.get(f"{API}/health", timeout=5).json()["dataNodesAlive"]
            if vivos >= cuantos:
                return
            time.sleep(1)
        raise SystemExit(f"Solo se registraron {vivos} de {cuantos} DataNodes")

    def matar(self, nombre):
        proceso = self.procesos.pop(nombre)
        os.killpg(os.getpgid(proceso.pid), signal.SIGKILL)
        proceso.wait(timeout=10)

    def apagar(self):
        for proceso in self.procesos.values():
            try:
                os.killpg(os.getpgid(proceso.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for proceso in self.procesos.values():
            try:
                proceso.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proceso.pid), signal.SIGKILL)


# --------------------------------------------------------------------------
# Las llamadas que hará el cliente. Primero al ControlNode por REST, después a
# los DataNodes por HTTP.
# --------------------------------------------------------------------------

def control(metodo, ruta, token=None, **kwargs):
    cabeceras = {"Authorization": f"Bearer {token}"} if token else {}
    respuesta = requests.request(metodo, API + ruta, headers=cabeceras, timeout=30, **kwargs)
    if respuesta.status_code >= 400:
        raise RuntimeError(f"{respuesta.status_code} {respuesta.text}")
    return respuesta.json() if respuesta.content else {}


def subir_bloque(pipeline, block_id, token, datos, sha):
    """PUT al primer DataNode; él se encarga de pasarlo a los demás."""
    destino = pipeline[0]["address"]
    respuesta = requests.put(
        f"http://{destino}/blocks/{block_id}",
        data=datos,
        headers={"X-Block-Token": token,
                 "X-Block-Sha256": sha,
                 "X-Pipeline": ",".join(n["address"] for n in pipeline[1:]),
                 "Content-Type": "application/octet-stream"},
        timeout=180,
    )
    respuesta.raise_for_status()
    return respuesta.json()


def bajar_bloque(bloque, desde=0, cuantos=0):
    """GET al primer DataNode que responda: si uno falla, se prueba el siguiente."""
    errores = []
    for replica in bloque["replicas"]:
        try:
            parametros = {"offset": desde, "length": cuantos} if (desde or cuantos) else None
            respuesta = requests.get(
                f"http://{replica['address']}/blocks/{bloque['blockId']}",
                headers={"X-Block-Token": bloque["token"]},
                params=parametros, timeout=120)
            respuesta.raise_for_status()
            return respuesta.content, replica["nodeId"]
        except requests.RequestException as error:
            errores.append(f"{replica['nodeId']}: {error}")
    raise RuntimeError(f"ninguna replica respondio: {errores}")


def subir_archivo(token, ruta_local, ruta_remota):
    creado = control("POST", "/files/create", token,
                     json={"path": ruta_remota, "overwrite": True})
    version = creado["versionId"]
    tam_bloque = creado["blockSize"]
    datos = Path(ruta_local).read_bytes()
    bloques, nodos = [], set()
    for indice in range(max(1, (len(datos) + tam_bloque - 1) // tam_bloque)):
        trozo = datos[indice * tam_bloque:(indice + 1) * tam_bloque]
        sha = hashlib.sha256(trozo).hexdigest()
        asignado = control("POST", f"/versions/{version}/blocks", token,
                           json={"index": indice})
        respuesta = subir_bloque(asignado["pipeline"], asignado["blockId"],
                                 asignado["token"], trozo, sha)
        nodos.update(respuesta["storedOn"])
        bloques.append({"index": indice, "blockId": asignado["blockId"],
                        "size": len(trozo), "sha256": sha})
    control("POST", f"/versions/{version}/commit", token, json={"blocks": bloques})
    return nodos


def bajar_archivo(token, ruta_remota, destino):
    abierto = control("POST", "/files/open", token, json={"path": ruta_remota})
    partes, nodos = [], set()
    for bloque in abierto["blocks"]:
        datos, nodo = bajar_bloque(bloque)
        partes.append(datos)
        nodos.add(nodo)
    Path(destino).write_bytes(b"".join(partes))
    return abierto, nodos


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="no apagar el cluster al terminar")
    parser.add_argument("--size-mb", type=int, default=12)
    args = parser.parse_args()

    carpeta = Path(tempfile.mkdtemp(prefix="dfsha-dn-"))
    cluster = Cluster(carpeta)
    print(f"Carpeta de trabajo: {carpeta}")
    print("\n== Levantando el cluster (1 ControlNode + 4 DataNodes) ==")
    cluster.levantar()
    print("  listo")

    try:
        pruebas(cluster, carpeta, args.size_mb)
    finally:
        if not args.keep:
            cluster.apagar()

    pasaron = sum(1 for _, ok, _ in resultados if ok)
    print(f"\n== Resumen: {pasaron}/{len(resultados)} pruebas pasaron ==")
    for nombre, ok, detalle in resultados:
        if not ok:
            print(f"  FALLA: {nombre} — {detalle}")
    if args.keep:
        print(f"Cluster arriba; logs en {carpeta}")
    else:
        shutil.rmtree(carpeta, ignore_errors=True)
    return 0 if pasaron == len(resultados) else 1


def pruebas(cluster, carpeta, tam_mb):
    print("\n== El DataNode responde por HTTP ==")
    salud = requests.get("http://localhost:50051/health", timeout=5).json()
    revisar("GET /health responde", salud["status"] == "ok", f"nodo {salud['nodeId']}")
    estado = requests.get("http://localhost:50051/status", timeout=5).json()
    revisar("GET /status trae el estado del nodo",
            estado["zone"] == "zona-a" and estado["blockCount"] == 0,
            f"zona {estado['zone']} · capacidad {estado['capacityBytes'] // 1024**3} GB")

    print("\n== Sesion contra el ControlNode (REST) ==")
    control("POST", "/auth/register", json={"username": "ana", "password": "secreto123"})
    sesion = control("POST", "/auth/login", json={"username": "ana", "password": "secreto123"})
    token = sesion["token"]
    revisar("login devuelve un JWT", bool(token), f"rol {sesion['role']}")

    print("\n== Subida: bloques repartidos y replicados ==")
    local = carpeta / "prueba.bin"
    local.write_bytes(os.urandom(tam_mb * 1024 * 1024))
    sha_original = hashlib.sha256(local.read_bytes()).hexdigest()
    nodos = subir_archivo(token, local, "/home/ana/prueba.bin")
    revisar("el pipeline HTTP deja 3 copias de cada bloque",
            len(nodos) >= 3, f"nodos que confirmaron: {sorted(nodos)}")

    abierto = control("POST", "/files/open", token, json={"path": "/home/ana/prueba.bin"})
    esperados = (tam_mb * 1024 * 1024 + 4 * 1024 * 1024 - 1) // (4 * 1024 * 1024)
    revisar("el archivo se partio en bloques de 4 MiB",
            len(abierto["blocks"]) == esperados, f"{len(abierto['blocks'])} bloques")
    revisar("cada bloque tiene 3 replicas",
            all(len(b["replicas"]) == 3 for b in abierto["blocks"]),
            f"replicas: {[len(b['replicas']) for b in abierto['blocks']]}")
    revisar("las replicas estan en zonas distintas",
            all(len({r["zone"] for r in b["replicas"]}) >= 2 for b in abierto["blocks"]))

    print("\n== Descarga y lectura por rango ==")
    bajado = carpeta / "bajado.bin"
    _, nodos_leidos = bajar_archivo(token, "/home/ana/prueba.bin", bajado)
    revisar("el archivo descargado es identico al original",
            hashlib.sha256(bajado.read_bytes()).hexdigest() == sha_original,
            f"leido de: {sorted(nodos_leidos)}")

    primer_bloque = abierto["blocks"][0]
    trozo, _ = bajar_bloque(primer_bloque, desde=1024, cuantos=64)
    revisar("lectura de un rango con offset y length",
            trozo == local.read_bytes()[1024:1088], f"{len(trozo)} bytes")

    respuesta = requests.get(
        f"http://{primer_bloque['replicas'][0]['address']}/blocks/{primer_bloque['blockId']}",
        headers={"X-Block-Token": primer_bloque["token"], "Range": "bytes=100-199"},
        timeout=30)
    revisar("lectura con cabecera Range (HTTP 206)",
            respuesta.status_code == 206 and respuesta.content == local.read_bytes()[100:200],
            respuesta.headers.get("Content-Range", ""))

    print("\n== Seguridad: el DataNode exige el token del ControlNode ==")
    sin_token = requests.get(
        f"http://{primer_bloque['replicas'][0]['address']}/blocks/{primer_bloque['blockId']}",
        timeout=15)
    revisar("sin token se rechaza la lectura", sin_token.status_code == 403,
            f"HTTP {sin_token.status_code}")
    token_falso = requests.get(
        f"http://{primer_bloque['replicas'][0]['address']}/blocks/{primer_bloque['blockId']}",
        headers={"X-Block-Token": "falso.token"}, timeout=15)
    revisar("un token mal firmado se rechaza", token_falso.status_code == 403)
    prestado = requests.get(
        f"http://{primer_bloque['replicas'][0]['address']}/blocks/{abierto['blocks'][-1]['blockId']}",
        headers={"X-Block-Token": primer_bloque["token"]}, timeout=15)
    revisar("un token de otro bloque no sirve", prestado.status_code == 403)
    escritura_con_token_lectura = requests.put(
        f"http://{primer_bloque['replicas'][0]['address']}/blocks/{primer_bloque['blockId']}",
        data=b"datos falsos", headers={"X-Block-Token": primer_bloque["token"]}, timeout=15)
    revisar("un token de lectura no autoriza escribir",
            escritura_con_token_lectura.status_code == 403)

    print("\n== Integridad: checksum incorrecto ==")
    creado = control("POST", "/files/create", token,
                     json={"path": "/home/ana/corrupto.bin", "overwrite": True})
    asignado = control("POST", f"/versions/{creado['versionId']}/blocks", token,
                       json={"index": 0})
    mal = requests.put(
        f"http://{asignado['pipeline'][0]['address']}/blocks/{asignado['blockId']}",
        data=b"contenido que no coincide",
        headers={"X-Block-Token": asignado["token"], "X-Block-Sha256": "0" * 64},
        timeout=30)
    revisar("el DataNode rechaza un bloque con checksum que no cuadra",
            mal.status_code == 422, f"HTTP {mal.status_code}")
    control("POST", f"/versions/{creado['versionId']}/abort", token)

    print("\n== Caida de un DataNode: lectura y re-replicacion ==")
    victima = abierto["blocks"][0]["replicas"][0]["nodeId"]
    cluster.matar(victima)
    print(f"  DataNode {victima} detenido a la fuerza")

    recuperado = carpeta / "bajado2.bin"
    _, nodos_leidos = bajar_archivo(token, "/home/ana/prueba.bin", recuperado)
    revisar("la descarga sigue funcionando con una replica caida",
            hashlib.sha256(recuperado.read_bytes()).hexdigest() == sha_original,
            f"leido de: {sorted(nodos_leidos)}")

    limite = time.time() + 90
    repuesto = False
    muertos = []
    while time.time() < limite:
        estado = control("GET", "/cluster/status", token)
        muertos = [n["nodeId"] for n in estado["dataNodes"] if not n["alive"]]
        if victima in muertos and not estado["underReplicatedBlocks"]:
            repuesto = True
            break
        time.sleep(2)
    revisar(f"el ControlNode detecta la caida de {victima}", victima in muertos,
            f"muertos: {muertos}")
    revisar("los bloques vuelven a 3 replicas copiandose entre DataNodes", repuesto,
            f"pendientes: {len(control('GET', '/cluster/status', token)['underReplicatedBlocks'])}")

    nodos = subir_archivo(token, local, "/home/ana/segundo.bin")
    revisar("se puede seguir subiendo con un DataNode menos", len(nodos) >= 3,
            f"nodos: {sorted(nodos)}")


if __name__ == "__main__":
    sys.exit(main())
