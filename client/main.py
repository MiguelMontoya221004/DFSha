import typer
from dfs_client import DFSClient

# Instanciamos Typer para crear nuestra aplicación de consola
app = typer.Typer(help="Cliente DFSha - Sistema de Archivos Distribuido")

# Instanciamos nuestro motor de comunicación (el cerebro)
cliente = DFSClient()


def mostrar(exito, mensaje):
    """Pinta en verde si salió bien y en rojo si no."""
    color = typer.colors.GREEN if exito else typer.colors.RED
    typer.echo(typer.style(mensaje, fg=color))


@app.command()
def register(username: str):
    """
    Crea un usuario nuevo en el sistema.
    Ejemplo: python main.py register esteban
    """
    password = typer.prompt(f"Contraseña para {username}", hide_input=True,
                            confirmation_prompt=True)
    mostrar(*cliente.registrar(username, password))


@app.command()
def login(username: str):
    """
    Inicia sesión en el sistema.
    Ejemplo: python main.py login esteban
    """
    # typer.prompt con hide_input=True oculta la contraseña mientras escribes
    password = typer.prompt(f"Ingresa la contraseña para {username}", hide_input=True)

    # Llamamos al motor, que hará la petición HTTP
    exito, mensaje = cliente.login(username, password)

    # Evaluamos la tupla que nos devolvió el motor para decidir qué color pintar
    mostrar(exito, mensaje)


@app.command()
def ls(ruta: str = typer.Argument(None, help="Carpeta a listar; por defecto la actual")):
    """
    Lista los archivos y carpetas en el directorio actual.
    Ejemplo: python main.py ls
    """
    exito, resultado = cliente.listar_directorio(ruta)

    if exito:
        donde = ruta or cliente.session.cwd
        typer.echo(typer.style(f"Contenido en {donde}:", fg=typer.colors.CYAN, bold=True))

        entradas = resultado.get("entries", resultado) if isinstance(resultado, dict) else resultado
        if isinstance(entradas, list) and len(entradas) > 0:
            for item in entradas:
                if isinstance(item, dict):
                    # Una carpeta se marca distinto que un archivo
                    nombre = item.get("name", item.get("path", "?"))
                    if item.get("type") == "dir" or item.get("tipo") == "dir":
                        typer.echo(f" 📁 {nombre}/")
                    else:
                        typer.echo(f" 📄 {nombre}  ({item.get('size', 0)} bytes)")
                else:
                    typer.echo(f" 📄 {item}")
        else:
            typer.echo(" (Directorio vacío)")
    else:
        mostrar(False, resultado)


@app.command()
def mkdir(ruta: str):
    """
    Crea una carpeta.
    Ejemplo: python main.py mkdir documentos
    """
    mostrar(*cliente.crear_directorio(ruta))


@app.command()
def put(archivo_local: str, ruta_remota: str = typer.Argument(None)):
    """
    Sube un archivo al sistema distribuido.
    Ejemplo: python main.py put video.mp4 /home/esteban/video.mp4
    """
    typer.echo("Subiendo (se parte en bloques y cada uno se replica 3 veces)...")
    mostrar(*cliente.subir_archivo(archivo_local, ruta_remota))


@app.command()
def get(ruta_remota: str, destino: str = typer.Argument(None)):
    """
    Descarga un archivo del sistema distribuido.
    Ejemplo: python main.py get /home/esteban/video.mp4 copia.mp4
    """
    typer.echo("Descargando...")
    mostrar(*cliente.descargar_archivo(ruta_remota, destino))


@app.command()
def status():
    """
    Muestra el estado del clúster: líder de Raft y DataNodes vivos.
    Ejemplo: python main.py status
    """
    exito, resultado = cliente.estado_cluster()
    if not exito:
        mostrar(False, resultado)
        return

    raft = resultado.get("raft", {})
    typer.echo(typer.style(f"Raft: líder {raft.get('lider', '?')} · term {raft.get('term', '?')}",
                           fg=typer.colors.CYAN, bold=True))
    for nodo in resultado.get("datanodes", []):
        estado = "vivo" if nodo.get("vivo", True) else "CAÍDO"
        typer.echo(f" 💾 {nodo.get('node_id')}  {nodo.get('host')}:{nodo.get('port')}  "
                   f"zona {nodo.get('zona')}  {estado}  "
                   f"{nodo.get('bloques', 0)} bloques")


# Este bloque asegura que la aplicación se ejecute correctamente desde la terminal
if __name__ == "__main__":
    app()
