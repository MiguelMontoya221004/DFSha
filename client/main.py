import typer
from dfs_client import DFSClient

# Instanciamos Typer para crear nuestra aplicación de consola
app = typer.Typer(help="Cliente DFSha - Sistema de Archivos Distribuido")

# Instanciamos nuestro motor de comunicación (el cerebro)
cliente = DFSClient()

@app.command()
def login(username: str):
    """
    Inicia sesión en el sistema.
    Ejemplo: python main.py login esteban
    """
    password = typer.prompt(f"Ingresa la contraseña para {username}", hide_input=True)
    exito, mensaje = cliente.login(username, password)
    
    if exito:
        typer.echo(typer.style(mensaje, fg=typer.colors.GREEN))
    else:
        typer.echo(typer.style(mensaje, fg=typer.colors.RED))

@app.command()
def ls():
    """
    Lista los archivos y carpetas en el directorio actual.
    Ejemplo: python main.py ls
    """
    exito, resultado = cliente.listar_directorio()
    
    if exito:
        typer.echo(typer.style(f"Contenido en {cliente.session.cwd}:", fg=typer.colors.CYAN, bold=True))
        if isinstance(resultado, list) and len(resultado) > 0:
            for item in resultado:
                typer.echo(f" 📄 {item}")
        else:
            typer.echo(" (Directorio vacío)")
    else:
        typer.echo(typer.style(resultado, fg=typer.colors.RED))

@app.command()
def put(ruta_archivo: str):
    """
    Sube un archivo local al sistema distribuido.
    Ejemplo: python main.py put archivo.txt
    """
    typer.echo(typer.style(f"Iniciando particionamiento de: {ruta_archivo}...", fg=typer.colors.YELLOW))
    exito, resultado = cliente.subir_archivo(ruta_archivo)
    
    if exito:
        mensaje_resumen, metadatos = resultado
        typer.echo(typer.style(mensaje_resumen, fg=typer.colors.GREEN, bold=True))
        
        typer.echo("Detalle de los bloques generados:")
        for bloque in metadatos:
            typer.echo(
                f"  📦 Bloque {bloque['bloque_id']} | "
                f"Tamaño: {bloque['tamano_bytes']} bytes | "
                f"Hash: {bloque['hash_sha256'][:15]}..."
            )
    else:
        typer.echo(typer.style(resultado, fg=typer.colors.RED))

@app.command()
def test_get(ruta_archivo: str):
    """
    Simula la descarga y reensamblaje de un archivo de prueba.
    Ejemplo: python main.py test-get archivo.txt
    """
    typer.echo(typer.style(f"Simulando descarga y ensamblaje de: {ruta_archivo}...", fg=typer.colors.CYAN))
    exito, mensaje = cliente.simular_get(ruta_archivo)
    
    if exito:
        typer.echo(typer.style(mensaje, fg=typer.colors.GREEN, bold=True))
    else:
        typer.echo(typer.style(mensaje, fg=typer.colors.RED))

if __name__ == "__main__":
    app()