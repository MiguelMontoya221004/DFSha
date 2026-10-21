#!/usr/bin/env python3
"""Prueba end-to-end del sistema completo de DFSha.

Levanta los tres ControlNodes con Raft, cuatro DataNodes y usa el cliente de
verdad (client/dfs_client.py) para subir y bajar archivos. Comprueba tambien lo
que solo se ve desde adentro: que los bloques queden repartidos en zonas
distintas, que el DataNode exija el token, los rangos de lectura, y que al
matar un DataNode el archivo se siga leyendo y las replicas se repongan solas.

    python3 tests/e2e_sistema.py            # levanta todo, prueba y apaga
    python3 tests/e2e_sistema.py --keep     # deja el cluster arriba

Hace falta el jar del ControlNode:  cd controlnode && mvn package -DskipTests
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
sys.path.insert(0, str(RAIZ / "client"))

SECRETO = "clave-hmac-compartida-con-los-datanodes"
CONTROL_NODES = ["http://127.0.0.1:8443", "http://127.0.0.1:8444", "http://127.0.0.1:8445"]
GRPC = "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053"
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

    # ----------------------------- arranque -----------------------------

    def levantar(self):
        jar = next((RAIZ / "controlnode" / "target").glob("controlnode-*.jar"), None)
        if jar is None:
            raise SystemExit("Falta el jar: cd controlnode && mvn package -DskipTests")

        for i, puerto in enumerate([8443, 8444, 8445], start=1):
            self.procesos[f"n{i}"] = self._lanzar([
                "java", "-jar", str(jar),
                f"--dfsha.node-id=n{i}",
                f"--server.port={puerto}",
                f"--dfsha.grpc-port={50050 + i}",
                f"--dfsha.storage-dir={self.carpeta}/raft",
                f"--spring.datasource.url=jdbc:h2:file:{self.carpeta}/raft/n{i}/meta;DB_CLOSE_DELAY=-1",
            ], os.environ.copy(), f"n{i}")
        lider = self._esperar_lider()
        print(f"  Raft arriba · lider: {lider}")

        for i in range(1, self.datanodes + 1):
            puerto = 9000 + i
            entorno = os.environ | {
                "DFSHA_NODE_ID": f"dn-{i}",
                "DFSHA_HOST": "127.0.0.1",
                "DFSHA_CLIENT_PORT": str(puerto),
                "DFSHA_ZONE": ZONAS[(i - 1) % len(ZONAS)],
                "DFSHA_DATA_DIR": str(self.carpeta / f"dn-{i}"),
                "DFSHA_CONTROLNODES": GRPC,
                "DFSHA_BLOCK_SECRET": SECRETO,
                "DFSHA_HEARTBEAT_S": "1",
            }
            self.procesos[f"dn-{i}"] = self._lanzar(
                [sys.executable, str(RAIZ / "datanode" / "main.py")], entorno, f"dn-{i}")
            self._esperar_http(puerto)
        self._esperar_datanodes(self.datanodes)

    def _esperar_http(self, puerto, limite=60):
        fin = time.time() + limite
        while time.time() < fin:
            try:
                if requests.get(f"http://127.0.0.1:{puerto}/health", timeout=2).ok:
                    return
            except requests.RequestException:
                time.sleep(0.5)
        raise SystemExit(f"El DataNode del puerto {puerto} no respondio /health")

    def _lanzar(self, comando, entorno, nombre):
        salida = open(self.carpeta / f"{nombre}.log", "wb")
        return subprocess.Popen(comando, env=entorno, stdout=salida,
                                stderr=subprocess.STDOUT, cwd=str(RAIZ),
                                start_new_session=True)

    def _esperar_lider(self, limite=120):
        fin = time.time() + limite
        while time.time() < fin:
            for url in CONTROL_NODES:
                try:
                    salud = requests.get(f"{url}/health", timeout=2).json()
                    if salud.get("es_lider"):
                        return salud.get("node_id")
                except requests.RequestException:
                    pass
            time.sleep(2)
        raise SystemExit("Raft no eligio lider")

    def _esperar_datanodes(self, cuantos, limite=90):
        fin = time.time() + limite
        vivos = 0
        while time.time() < fin:
            for url in CONTROL_NODES:
                try:
                    vivos = requests.get(f"{url}/health", timeout=2).json().get("datanodes_vivos", 0)
                    if vivos >= cuantos:
                        return
                except requests.RequestException:
                    pass
            time.sleep(2)
        raise SystemExit(f"Solo se registraron {vivos} de {cuantos} DataNodes")

    # ------------------------------ control ------------------------------

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
                proceso.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proceso.pid), signal.SIGKILL)

    def lider(self):
        for url in CONTROL_NODES:
            try:
                if requests.get(f"{url}/health", timeout=2).json().get("es_lider"):
                    return url
            except requests.RequestException:
                pass
        return CONTROL_NODES[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--size-mb", type=int, default=10)
    args = parser.parse_args()

    carpeta = Path(tempfile.mkdtemp(prefix="dfsha-e2e-"))
    cluster = Cluster(carpeta)
    print(f"Carpeta de trabajo: {carpeta}")
    print("\n== Levantando 3 ControlNodes (Raft) + 4 DataNodes ==")
    cluster.levantar()
    print("  todo arriba")

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
    os.chdir(carpeta)                      # la sesion del cliente se guarda aqui
    from dfs_client import DFSClient       # el cliente de verdad, sin atajos

    lider = cluster.lider()
    cliente = DFSClient(base_url=lider)

    print("\n== El DataNode responde por HTTP ==")
    estado = requests.get("http://127.0.0.1:9001/status", timeout=5).json()
    revisar("GET /status del DataNode", estado["node_id"] == "dn-1" and estado["registrado"],
            f"zona {estado['zone']} · registrado en {len(estado['control_nodes'])} ControlNodes")

    print("\n== Usuario y namespace (cliente -> ControlNode) ==")
    ok, mensaje = cliente.registrar("ana", "secreto123")
    revisar("register crea el usuario", ok, mensaje)
    ok, mensaje = cliente.login("ana", "secreto123")
    revisar("login devuelve el JWT y deja al usuario en su home",
            ok and cliente.session.cwd == "/home/ana", cliente.session.cwd)
    ok, mensaje = cliente.crear_directorio("datos")
    revisar("mkdir crea la carpeta", ok, mensaje)
    ok, listado = cliente.listar_directorio()
    revisar("ls lista el home", ok, str(listado)[:70] if ok else listado)

    print("\n== Subir un archivo: bloques, pipeline y replicas ==")
    local = carpeta / "prueba.bin"
    local.write_bytes(os.urandom(tam_mb * 1024 * 1024))
    sha_original = hashlib.sha256(local.read_bytes()).hexdigest()

    ok, mensaje = cliente.subir_archivo(str(local), "datos/prueba.bin")
    revisar("put sube el archivo entero", ok, mensaje)

    archivo = requests.get(f"{lider}/files/read", params={"path": "/home/ana/datos/prueba.bin"},
                           headers=cliente._get_headers(), timeout=30).json()
    esperados = max(1, (tam_mb * 1024 * 1024 + 4 * 1024 * 1024 - 1) // (4 * 1024 * 1024))
    revisar("el archivo quedo partido en bloques de 4 MiB",
            len(archivo["blocks"]) == esperados, f"{len(archivo['blocks'])} bloques")
    revisar("cada bloque tiene 3 replicas",
            all(len(b["nodes"]) == 3 for b in archivo["blocks"]),
            f"replicas: {[len(b['nodes']) for b in archivo['blocks']]}")
    revisar("las replicas estan en zonas distintas",
            all(len({n["zona"] for n in b["nodes"]}) >= 2 for b in archivo["blocks"]))

    print("\n== Bajar el archivo ==")
    ok, mensaje = cliente.descargar_archivo("datos/prueba.bin", str(carpeta / "bajado.bin"))
    revisar("get reconstruye el archivo", ok, mensaje)
    revisar("el archivo bajado es identico al original",
            hashlib.sha256((carpeta / "bajado.bin").read_bytes()).hexdigest() == sha_original)

    print("\n== Lectura por rangos (RF3) ==")
    bloque = archivo["blocks"][0]
    nodo = bloque["nodes"][0]
    base = f"http://{nodo['host']}:{nodo['port']}/blocks/{bloque['block_id']}"
    cabecera = {"X-Block-Token": bloque["token"]}
    trozo = requests.get(base, params={"offset": 1024, "length": 64},
                         headers=cabecera, timeout=30).content
    revisar("lectura con offset y length", trozo == local.read_bytes()[1024:1088],
            f"{len(trozo)} bytes")
    respuesta = requests.get(base, headers=dict(cabecera, Range="bytes=100-199"), timeout=30)
    revisar("lectura con cabecera Range (HTTP 206)",
            respuesta.status_code == 206 and respuesta.content == local.read_bytes()[100:200],
            respuesta.headers.get("Content-Range", ""))

    print("\n== Seguridad: el DataNode exige el token del ControlNode ==")
    revisar("sin token se rechaza", requests.get(base, timeout=15).status_code == 403)
    revisar("un token mal firmado se rechaza",
            requests.get(base, headers={"X-Block-Token": "x:ana:read:9999999999999:firma"},
                         timeout=15).status_code == 403)
    if len(archivo["blocks"]) > 1:
        otro = archivo["blocks"][-1]["block_id"]
        prestado = requests.get(f"http://{nodo['host']}:{nodo['port']}/blocks/{otro}",
                                headers=cabecera, timeout=15)
        revisar("un token de otro bloque no sirve", prestado.status_code == 403)
    revisar("un token de lectura no autoriza escribir",
            requests.put(base, data=b"datos falsos", headers=cabecera, timeout=15).status_code == 403)

    print("\n== Integridad: checksum que no cuadra ==")
    apertura = requests.post(f"{lider}/files/write/begin", json={"path": "/home/ana/malo.bin"},
                             headers=cliente._get_headers(), timeout=30).json()
    asignado = requests.post(f"{lider}/files/write/allocate",
                             json={"upload_id": apertura["upload_id"]},
                             headers=cliente._get_headers(), timeout=30).json()
    destino = asignado["nodes"][0]
    mal = requests.put(f"http://{destino['host']}:{destino['port']}/blocks/{asignado['block_id']}",
                       data=b"contenido que no coincide",
                       headers={"X-Block-Token": asignado["token"], "X-Block-Sha256": "0" * 64},
                       timeout=30)
    revisar("el DataNode rechaza un bloque corrupto", mal.status_code == 422,
            f"HTTP {mal.status_code}")
    requests.post(f"{lider}/files/write/abort", json={"upload_id": apertura["upload_id"]},
                  headers=cliente._get_headers(), timeout=30)

    print("\n== Caida de un DataNode: failover y re-replicacion ==")
    victima = archivo["blocks"][0]["nodes"][0]["node_id"]
    cluster.matar(victima)
    print(f"  DataNode {victima} detenido a la fuerza")

    ok, mensaje = cliente.descargar_archivo("datos/prueba.bin", str(carpeta / "bajado2.bin"))
    revisar("el archivo se sigue bajando con una replica caida", ok, mensaje)
    revisar("y sigue siendo identico",
            hashlib.sha256((carpeta / "bajado2.bin").read_bytes()).hexdigest() == sha_original)

    limite = time.time() + 150
    repuesto, pendientes = False, []
    while time.time() < limite:
        try:
            pendientes = requests.get(f"{cluster.lider()}/admin/under-replicated",
                                      headers=cliente._get_headers(), timeout=10).json()
            if not pendientes:
                repuesto = True
                break
        except requests.RequestException:
            pass
        time.sleep(3)
    revisar("los bloques vuelven a 3 replicas (re-replicacion entre DataNodes)", repuesto,
            f"{len(pendientes)} bloques pendientes")

    ok, mensaje = cliente.subir_archivo(str(local), "datos/segundo.bin")
    revisar("se puede seguir subiendo con un DataNode menos", ok, mensaje)


if __name__ == "__main__":
    sys.exit(main())
