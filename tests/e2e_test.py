#!/usr/bin/env python3
"""Pruebas end-to-end del Hito 2 de DFSha.

Levanta un clúster completo (1 ControlNode + 4 DataNodes), ejecuta las pruebas
funcionales, de distribución, de fallos y de seguridad, e imprime un resumen.

    python3 tests/e2e_test.py            # levanta el clúster y prueba
    python3 tests/e2e_test.py --keep     # deja el clúster arriba al terminar

Requisitos: el jar del ControlNode compilado (mvn package) y las dependencias
de Python de datanode/ y client/.
"""
from __future__ import annotations

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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))

import grpc                                                    # noqa: E402
import requests                                                # noqa: E402

from dfsha.client import DfshaClient, DfshaError                # noqa: E402
from dfsha.pb import dfsha_pb2, dfsha_pb2_grpc                  # noqa: E402

REST = os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8080")
BLOCK_SECRET = "dfsha-block-secret-pruebas"
ZONES = ["zona-a", "zona-a", "zona-b", "zona-c"]

results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    results.append((name, bool(condition), detail))
    mark = "\033[92mPASS\033[0m" if condition else "\033[91mFALLA\033[0m"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)


class Cluster:
    def __init__(self, workdir: Path, datanodes: int = 4) -> None:
        self.workdir = workdir
        self.datanodes = datanodes
        self.processes: dict[str, subprocess.Popen] = {}

    def start(self) -> None:
        jar = next((ROOT / "controlnode" / "target").glob("dfsha-controlnode-*.jar"), None)
        if jar is None:
            raise SystemExit("Falta el jar del ControlNode: ejecute 'mvn package' en controlnode/")
        env = os.environ | {"DFSHA_BLOCK_SECRET": BLOCK_SECRET,
                            "DFSHA_NODE_DEAD_MS": "8000",
                            "DFSHA_LOG_LEVEL": "INFO"}
        self.processes["cn1"] = self._spawn(["java", "-jar", str(jar)], env, "cn1")
        self._wait_rest()
        for i in range(1, self.datanodes + 1):
            port = 50050 + i
            dn_env = env | {
                "DFSHA_NODE_ID": f"dn{i}",
                "DFSHA_DN_PORT": str(port),
                "DFSHA_DN_ADVERTISE": f"localhost:{port}",
                "DFSHA_ZONE": ZONES[(i - 1) % len(ZONES)],
                "DFSHA_DATA_DIR": str(self.workdir / f"dn{i}"),
                "DFSHA_CONTROLNODE": "localhost:9090",
                "DFSHA_HEARTBEAT_S": "1",
                "PYTHONPATH": str(ROOT / "datanode"),
            }
            self.processes[f"dn{i}"] = self._spawn(
                [sys.executable, "-m", "dfsha_datanode"], dn_env, f"dn{i}")
        self._wait_datanodes(self.datanodes)

    def _spawn(self, cmd: list[str], env: dict, name: str) -> subprocess.Popen:
        logfile = open(self.workdir / f"{name}.log", "wb")
        return subprocess.Popen(cmd, env=env, stdout=logfile, stderr=subprocess.STDOUT,
                                cwd=str(ROOT), start_new_session=True)

    def _wait_rest(self, timeout: float = 60) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if requests.get(f"{REST}/api/v1/health", timeout=2).ok:
                    return
            except requests.RequestException:
                time.sleep(1)
        raise SystemExit("El ControlNode no respondió a /health")

    def _wait_datanodes(self, expected: int, timeout: float = 60) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            alive = requests.get(f"{REST}/api/v1/health", timeout=5).json()["dataNodesAlive"]
            if alive >= expected:
                return
            time.sleep(1)
        raise SystemExit(f"Sólo se registraron {alive} de {expected} DataNodes")

    def kill(self, name: str) -> None:
        process = self.processes.pop(name)
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        process.wait(timeout=10)

    def stop(self) -> None:
        for process in self.processes.values():
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for process in self.processes.values():
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="no apagar el clúster al terminar")
    parser.add_argument("--size-mb", type=int, default=12, help="tamaño del archivo de prueba")
    args = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="dfsha-e2e-"))
    cluster = Cluster(workdir)
    print(f"Directorio de trabajo: {workdir}")
    print("\n== Levantando el clúster (1 ControlNode + 4 DataNodes) ==")
    cluster.start()
    print("  clúster listo")

    try:
        run_tests(cluster, workdir, args.size_mb)
    finally:
        if not args.keep:
            cluster.stop()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n== Resumen: {passed}/{len(results)} pruebas pasaron ==")
    for name, ok, detail in results:
        if not ok:
            print(f"  FALLA: {name} — {detail}")
    if args.keep:
        print(f"Clúster arriba; logs en {workdir}")
    else:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0 if passed == len(results) else 1


def run_tests(cluster: Cluster, workdir: Path, size_mb: int) -> None:
    client = DfshaClient(REST)
    other = DfshaClient(REST)

    print("\n== RF4: autenticación y usuarios ==")
    client.register("ana", "secreto123")
    session = client.login("ana", "secreto123")
    check("login devuelve un JWT", bool(session.get("token")), f"rol {session['role']}")
    other.register("bob", "secreto123")
    other.login("bob", "secreto123")
    try:
        DfshaClient(REST).ls("/")
        check("sin token se rechaza", False, "la petición pasó sin autenticación")
    except DfshaError as exc:
        check("sin token se rechaza", exc.code == "UNAUTHORIZED", exc.code)
    try:
        client.login("ana", "incorrecta")
        check("contraseña incorrecta se rechaza", False)
    except DfshaError as exc:
        check("contraseña incorrecta se rechaza", exc.status == 401, exc.code)

    print("\n== RF1: gestión del sistema de archivos ==")
    client.mkdir("/home/ana/datos/2026", parents=True)
    entries = {e["name"]: e for e in client.ls("/home/ana")}
    check("mkdir -p crea la jerarquía", "datos" in entries)
    check("el home del usuario es privado",
          client.stat("/home/ana")["permissions"] == "rwx------")

    print("\n== RF2: transferencia y particionamiento ==")
    local = workdir / "prueba.bin"
    local.write_bytes(os.urandom(size_mb * 1024 * 1024))
    original_sha = hashlib.sha256(local.read_bytes()).hexdigest()
    stats = client.put(local, "/home/ana/datos/prueba.bin")
    expected_blocks = (size_mb * 1024 * 1024 + 4 * 1024 * 1024 - 1) // (4 * 1024 * 1024)
    check("put divide el archivo en bloques", stats.blocks == expected_blocks,
          f"{stats.blocks} bloques de 4 MiB")
    check("los bloques se reparten entre varios DataNodes", len(stats.nodes_used) >= 3,
          f"nodos usados: {sorted(stats.nodes_used)}")

    opened = client._request("POST", "/files/open", json={"path": "/home/ana/datos/prueba.bin"})
    replicas_ok = all(len(b["replicas"]) == 3 for b in opened["blocks"])
    zones_ok = all(len({r["zone"] for r in b["replicas"]}) >= 2 for b in opened["blocks"])
    check("cada bloque tiene 3 réplicas", replicas_ok,
          f"réplicas por bloque: {[len(b['replicas']) for b in opened['blocks']]}")
    check("las réplicas están en zonas distintas", zones_ok)

    downloaded = workdir / "bajado.bin"
    get_stats = client.get("/home/ana/datos/prueba.bin", downloaded)
    check("get reconstruye el archivo intacto",
          hashlib.sha256(downloaded.read_bytes()).hexdigest() == original_sha,
          f"{get_stats.mbps:.1f} MB/s con 4 hilos")

    print("\n== RF3: acceso a archivos (open/seek/read y escritura parcial) ==")
    with client.open("/home/ana/datos/prueba.bin") as handle:
        handle.seek(1024)
        fragment = handle.read(64)
    check("lectura aleatoria con seek", fragment == local.read_bytes()[1024:1088])

    marker = b"DFSHA-HITO2-COPY-ON-WRITE"
    offset = 5 * 1024 * 1024
    before = {b["index"]: b["blockId"] for b in opened["blocks"]}
    client.write_at("/home/ana/datos/prueba.bin", offset, marker)
    after_open = client._request("POST", "/files/open",
                                 json={"path": "/home/ana/datos/prueba.bin"})
    after = {b["index"]: b["blockId"] for b in after_open["blocks"]}
    changed = [i for i in before if before[i] != after[i]]
    check("copy-on-write sólo reescribe el bloque afectado", changed == [1],
          f"bloques nuevos: {changed}")
    with client.open("/home/ana/datos/prueba.bin") as handle:
        handle.seek(offset)
        check("la escritura parcial es visible", handle.read(len(marker)) == marker)

    print("\n== RNF6: control de acceso y tokens de bloque ==")
    try:
        other.stat("/home/ana/datos/prueba.bin")
        check("otro usuario no puede leer el archivo", False, "bob pudo leerlo")
    except DfshaError as exc:
        check("otro usuario no puede leer el archivo", exc.code == "FORBIDDEN", exc.code)

    block = after_open["blocks"][0]
    replica = block["replicas"][0]["address"]
    try:
        with grpc.insecure_channel(replica) as channel:
            stub = dfsha_pb2_grpc.DataTransferStub(channel)
            list(stub.ReadBlock(dfsha_pb2.ReadBlockRequest(
                block_id=block["blockId"], block_token="falso.token"), timeout=10))
        check("el DataNode exige un token de bloque válido", False, "leyó sin token")
    except grpc.RpcError as exc:
        check("el DataNode exige un token de bloque válido",
              exc.code() == grpc.StatusCode.PERMISSION_DENIED, exc.code().name)

    print("\n== RNF3: consistencia (lease y quórum) ==")
    draft = client._request("POST", "/files/create", json={"path": "/home/ana/lock.txt"})
    try:
        client._request("POST", "/files/create",
                        json={"path": "/home/ana/lock.txt", "overwrite": True})
        check("un solo escritor por archivo", False, "se otorgaron dos leases")
    except DfshaError as exc:
        check("un solo escritor por archivo", exc.code == "LOCKED", exc.message)
    client._request("POST", f"/versions/{draft['versionId']}/abort")

    draft = client._request("POST", "/files/create",
                            json={"path": "/home/ana/quorum.txt", "overwrite": True})
    try:
        client._request("POST", f"/versions/{draft['versionId']}/commit",
                        json={"blocks": [{"index": 0, "blockId": "inexistente",
                                          "size": 10, "sha256": "aa"}]})
        check("no se confirma sin quórum de réplicas", False, "aceptó el commit")
    except DfshaError as exc:
        check("no se confirma sin quórum de réplicas", exc.code == "NOT_ENOUGH_REPLICAS",
              exc.message)
    client._request("POST", f"/versions/{draft['versionId']}/abort")

    print("\n== RNF2: caída de un DataNode, failover y re-replicación ==")
    victim = after_open["blocks"][0]["replicas"][0]["nodeId"]
    cluster.kill(victim)
    print(f"  DataNode {victim} detenido a la fuerza")

    recovered = workdir / "bajado2.bin"
    client.get("/home/ana/datos/prueba.bin", recovered)
    expected = bytearray(local.read_bytes())
    expected[offset:offset + len(marker)] = marker
    check("la lectura sigue funcionando con una réplica caída",
          hashlib.sha256(bytes(expected)).hexdigest()
          == hashlib.sha256(recovered.read_bytes()).hexdigest())

    deadline = time.time() + 90
    restored = False
    while time.time() < deadline:
        status = client.cluster_status()
        dead = [n["nodeId"] for n in status["dataNodes"] if not n["alive"]]
        if victim in dead and not status["underReplicatedBlocks"]:
            restored = True
            break
        time.sleep(2)
    check(f"el ControlNode detecta la caída de {victim}", victim in dead, f"muertos: {dead}")
    check("los bloques vuelven a 3 réplicas por re-replicación P2P", restored,
          f"pendientes: {len(client.cluster_status()['underReplicatedBlocks'])}")

    written = client.put(local, "/home/ana/datos/segundo.bin")
    check("se puede seguir escribiendo con un DataNode menos",
          written.blocks == expected_blocks, f"nodos: {sorted(written.nodes_used)}")

    print("\n== RF1: borrado y liberación de bloques ==")
    client.rm("/home/ana/datos/segundo.bin")
    try:
        client.stat("/home/ana/datos/segundo.bin")
        check("rm borra el archivo", False)
    except DfshaError as exc:
        check("rm borra el archivo", exc.code == "NOT_FOUND", exc.code)
    client.mv("/home/ana/datos/prueba.bin", "/home/ana/datos/renombrado.bin")
    check("mv renombra sin mover datos",
          client.stat("/home/ana/datos/renombrado.bin")["size"] == len(expected))


if __name__ == "__main__":
    sys.exit(main())
