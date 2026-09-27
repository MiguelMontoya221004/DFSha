"""CLI del cliente DFSha (RF1, RF2, RF3, RF4, RF5).

    dfsha login ana
    dfsha put ./video.mp4 /home/ana/video.mp4
    dfsha ls /home/ana
    dfsha get /home/ana/video.mp4 ./copia.mp4
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import typer

from .client import DfshaClient, DfshaError
from .session import Session

app = typer.Typer(add_completion=False, help="Cliente del sistema de archivos distribuido DFSha")


# --------------------------------------------------------------------- helpers

def _client(require_login: bool = True) -> tuple[DfshaClient, Session]:
    session = Session.load()
    if require_login and not session.token:
        typer.secho("No hay sesión activa. Ejecute: dfsha login <usuario>", fg=typer.colors.RED)
        raise typer.Exit(2)
    return DfshaClient(session.control_node, session.token), session


def _run(fn):
    try:
        return fn()
    except DfshaError as exc:
        typer.secho(f"Error [{exc.code}] {exc.message}", fg=typer.colors.RED)
        raise typer.Exit(1)


def _human(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:,.0f} {unit}" if unit == "B" else f"{value:,.1f} {unit}"
        value /= 1024
    return f"{size} B"


def _progress(done: int, total: int) -> None:
    width = 28
    filled = int(width * done / total) if total else width
    sys.stdout.write(f"\r  [{'#' * filled}{'.' * (width - filled)}] bloque {done}/{total}")
    sys.stdout.flush()
    if done == total:
        sys.stdout.write("\n")


# ----------------------------------------------------------------- RF4: auth

@app.command()
def register(username: str, password: str = typer.Option(..., prompt=True, hide_input=True)):
    """Registra un usuario nuevo y crea su directorio /home/<usuario>."""
    client, session = _client(require_login=False)
    result = _run(lambda: client.register(username, password))
    typer.secho(f"Usuario {username} creado · home {result['home']}", fg=typer.colors.GREEN)


@app.command()
def login(username: str, password: str = typer.Option(..., prompt=True, hide_input=True),
          control_node: str = typer.Option(None, "--control-node", help="URL del ControlNode")):
    """Inicia sesión y guarda el token JWT localmente."""
    session = Session.load()
    if control_node:
        session.control_node = control_node
    client = DfshaClient(session.control_node)
    result = _run(lambda: client.login(username, password))
    session.token = result["token"]
    session.username = result["username"]
    session.cwd = f"/home/{username}"
    session.save()
    typer.secho(f"Sesión iniciada como {username} ({result['role']}) · "
                f"directorio actual {session.cwd}", fg=typer.colors.GREEN)


@app.command()
def logout():
    """Cierra la sesión local."""
    session = Session.load()
    session.token = None
    session.username = None
    session.save()
    typer.echo("Sesión cerrada")


# ------------------------------------------------------------ RF1: namespace

@app.command("ls")
def list_dir(path: str = typer.Argument(None), long: bool = typer.Option(False, "-l", "--long")):
    """Lista el contenido de un directorio."""
    client, session = _client()
    target = session.resolve(path)
    entries = _run(lambda: client.ls(target))
    if not entries:
        typer.echo("(vacío)")
        return
    for e in sorted(entries, key=lambda x: (x["type"] != "dir", x["name"])):
        name = e["name"] + ("/" if e["type"] == "dir" else "")
        if long:
            when = datetime.fromtimestamp(e["modifiedAt"] / 1000).strftime("%Y-%m-%d %H:%M")
            kind = "d" if e["type"] == "dir" else "-"
            blocks = f"{e['blocks']} bloques" if e.get("blocks") else ""
            typer.echo(f"{kind}{e['permissions']}  {e['owner']:<10} {_human(e['size']):>10}  "
                       f"{when}  {name} {blocks}")
        else:
            typer.echo(name)


@app.command()
def cd(path: str):
    """Cambia el directorio de trabajo del cliente."""
    client, session = _client()
    target = session.resolve(path)
    info = _run(lambda: client.stat(target))
    if info["type"] != "dir":
        typer.secho(f"{target} no es un directorio", fg=typer.colors.RED)
        raise typer.Exit(1)
    session.cwd = target
    session.save()
    typer.echo(target)


@app.command()
def pwd():
    """Muestra el directorio de trabajo."""
    typer.echo(Session.load().cwd)


@app.command()
def mkdir(path: str, parents: bool = typer.Option(False, "-p", "--parents")):
    """Crea un directorio."""
    client, session = _client()
    result = _run(lambda: client.mkdir(session.resolve(path), parents))
    typer.secho(f"Directorio creado: {result['path']}", fg=typer.colors.GREEN)


@app.command("rm")
def remove(path: str, recursive: bool = typer.Option(False, "-r", "--recursive")):
    """Borra un archivo o directorio (sus bloques se liberan en los DataNodes)."""
    client, session = _client()
    result = _run(lambda: client.rm(session.resolve(path), recursive))
    typer.secho(f"Borrado: {result['deleted']}", fg=typer.colors.GREEN)


@app.command("mv")
def move(src: str, dst: str):
    """Mueve o renombra (operación atómica de metadatos)."""
    client, session = _client()
    result = _run(lambda: client.mv(session.resolve(src), session.resolve(dst)))
    typer.secho(f"Movido a {result['path']}", fg=typer.colors.GREEN)


@app.command()
def stat(path: str):
    """Muestra los metadatos de una ruta y dónde están sus bloques."""
    client, session = _client()
    target = session.resolve(path)
    info = _run(lambda: client.stat(target))
    typer.echo(f"ruta        : {info['path']}")
    typer.echo(f"tipo        : {info['type']}")
    typer.echo(f"dueño/grupo : {info['owner']}/{info['group']}  permisos {info['permissions']}")
    typer.echo(f"tamaño      : {_human(info['size'])}")
    if info.get("blocks"):
        typer.echo(f"bloques     : {info['blocks']} de {_human(info['blockSize'])}")
        opened = _run(lambda: client._request("POST", "/files/open", json={"path": target}))
        for b in opened["blocks"]:
            nodes = ", ".join(f"{r['nodeId']}({r['zone']})" for r in b["replicas"])
            typer.echo(f"  bloque {b['index']:>3}  {_human(b['size']):>10}  "
                       f"sha256 {b['sha256'][:12]}…  réplicas: {nodes}")


# -------------------------------------------------------- RF2: transferencia

@app.command()
def put(local: Path, remote: str = typer.Argument(None),
        overwrite: bool = typer.Option(False, "-f", "--overwrite"),
        threads: int = typer.Option(4, "-t", "--threads", help="Bloques en paralelo")):
    """Sube un archivo: lo parte en bloques y los distribuye entre los DataNodes."""
    client, session = _client()
    target = session.resolve(remote or local.name)
    stats = _run(lambda: client.put(local, target, overwrite, threads, progress=_progress))
    typer.secho(f"Subido {stats.path} · {_human(stats.size)} en {stats.blocks} bloques · "
                f"{stats.seconds:.2f} s ({stats.mbps:.1f} MB/s) · "
                f"DataNodes: {', '.join(sorted(stats.nodes_used))}", fg=typer.colors.GREEN)


@app.command()
def get(remote: str, local: Path = typer.Argument(None),
        threads: int = typer.Option(4, "-t", "--threads")):
    """Descarga un archivo leyendo sus bloques en paralelo desde varias réplicas."""
    client, session = _client()
    source = session.resolve(remote)
    destination = local or Path(source.split("/")[-1])
    stats = _run(lambda: client.get(source, destination, threads, progress=_progress))
    typer.secho(f"Descargado {destination} · {_human(stats.size)} en {stats.blocks} bloques · "
                f"{stats.seconds:.2f} s ({stats.mbps:.1f} MB/s) · "
                f"leído de: {', '.join(sorted(stats.nodes_used))}", fg=typer.colors.GREEN)


# ------------------------------------------------------------- RF3: acceso

@app.command()
def read(remote: str, offset: int = typer.Option(0, "--offset"),
         count: int = typer.Option(256, "--count", help="Bytes a leer")):
    """Lectura aleatoria: open + seek + read sobre los bloques que hagan falta."""
    client, session = _client()
    with _run(lambda: client.open(session.resolve(remote))) as handle:
        handle.seek(offset)
        data = handle.read(count)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
    typer.echo()


@app.command()
def write(remote: str, text: str = typer.Option(None, "--text"),
          offset: int = typer.Option(0, "--offset"),
          from_file: Path = typer.Option(None, "--from-file")):
    """Escritura parcial con copy-on-write: sólo se reescriben los bloques afectados."""
    client, session = _client()
    payload = from_file.read_bytes() if from_file else (text or "").encode()
    if not payload:
        typer.secho("Indique --text o --from-file", fg=typer.colors.RED)
        raise typer.Exit(2)
    result = _run(lambda: client.write_at(session.resolve(remote), offset, payload))
    typer.secho(f"Escritos {len(payload)} bytes en el desplazamiento {offset} · "
                f"nueva versión {result['versionId'][:8]}… · tamaño {_human(result['size'])}",
                fg=typer.colors.GREEN)


# ----------------------------------------------------------- RF5: monitoreo

@app.command()
def status():
    """Estado del clúster: ControlNode, DataNodes y bloques sub-replicados."""
    client, _ = _client()
    info = _run(client.cluster_status)
    cn = info["controlNode"]
    typer.secho(f"ControlNode {cn['nodeId']} · clúster {cn['clusterId']} · rol {cn['role']}",
                fg=typer.colors.CYAN)
    typer.echo(f"replicación {info['replicationFactor']} (mínimo {info['minReplicas']}) · "
               f"bloque {_human(info['blockSize'])} · bloques vigilados {info['trackedBlocks']}")
    typer.echo("")
    typer.echo(f"{'DataNode':<10}{'dirección':<24}{'zona':<10}{'estado':<9}"
               f"{'bloques':>8}{'usado':>12}{'carga':>8}")
    for n in info["dataNodes"]:
        estado = "vivo" if n["alive"] else "MUERTO"
        typer.echo(f"{n['nodeId']:<10}{n['address']:<24}{n['zone']:<10}{estado:<9}"
                   f"{n['blockCount']:>8}{_human(n['usedBytes']):>12}{n['loadScore']:>8.3f}")
    under = info["underReplicatedBlocks"]
    if under:
        typer.secho(f"\n{len(under)} bloque(s) sub-replicados; el ControlNode los está reponiendo",
                    fg=typer.colors.YELLOW)
    else:
        typer.secho("\nTodos los bloques cumplen el factor de replicación", fg=typer.colors.GREEN)


@app.command()
def shell():
    """Shell interactivo con directorio de trabajo (transparencia nivel 2)."""
    client, session = _client()
    typer.secho(f"DFSha shell · {session.control_node} · usuario {session.username}. "
                f"Escriba 'help' o 'exit'.", fg=typer.colors.CYAN)
    commands = {"ls": list_dir, "cd": cd, "pwd": pwd, "mkdir": mkdir, "rm": remove,
                "mv": move, "stat": stat, "put": put, "get": get, "status": status,
                "read": read, "write": write}
    while True:
        try:
            line = input(f"dfsha:{Session.load().cwd}$ ").strip()
        except (EOFError, KeyboardInterrupt):
            typer.echo()
            return
        if not line:
            continue
        if line in ("exit", "quit"):
            return
        if line == "help":
            typer.echo("comandos: " + ", ".join(sorted(commands)) + ", exit")
            continue
        parts = line.split()
        if parts[0] not in commands:
            typer.secho(f"comando desconocido: {parts[0]}", fg=typer.colors.RED)
            continue
        try:
            # Se reutilizan los mismos comandos del CLI dentro del shell.
            app_args = parts
            from typer.main import get_command
            get_command(app).main(args=app_args, standalone_mode=False)
        except SystemExit:
            pass
        except Exception as exc:                              # noqa: BLE001
            typer.secho(f"error: {exc}", fg=typer.colors.RED)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
