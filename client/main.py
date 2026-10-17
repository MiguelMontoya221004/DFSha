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
    # typer.prompt con hide_input=True oculta la contraseña mientras escribes
    password = typer.prompt(f"Ingresa la contraseña para {username}", hide_input=True)
    
    # Llamamos al motor, que hará la petición HTTP
    exito, mensaje = cliente.login(username, password)
    
    # Evaluamos la tupla que nos devolvió el motor para decidir qué color pintar
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
        
        # Asumiendo que el backend en Java devuelve una lista de nombres
        if isinstance(resultado, list) and len(resultado) > 0:
            for item in resultado:
                typer.echo(f" 📄 {item}")
        else:
            typer.echo(" (Directorio vacío)")
    else:
        typer.echo(typer.style(resultado, fg=typer.colors.RED))

# Este bloque asegura que la aplicación se ejecute correctamente desde la terminal
if __name__ == "__main__":
    app()