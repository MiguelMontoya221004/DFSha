import requests
import os
import hashlib
from session import Session

# Tamaño del bloque: 4 MB para pruebas locales
TAMANO_BLOQUE = 4 * 1024 * 1024 

class DFSClient:
    def __init__(self, base_url="http://localhost:8080"):
        self.base_url = base_url
        self.session = Session()

    def _get_headers(self):
        """Inyecta el Token JWT en las peticiones HTTP si el usuario está logueado."""
        headers = {"Content-Type": "application/json"}
        if self.session.token:
            headers["Authorization"] = f"Bearer {self.session.token}"
        return headers

    def login(self, username, password):
        """Se comunica con AuthController.java."""
        url = f"{self.base_url}/auth/login"
        payload = {"username": username, "password": password}
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                self.session.login(username, data.get("token"))
                return True, f"Login exitoso. Bienvenido {username}."
            else:
                return False, f"Error de credenciales: {response.status_code}"
        except requests.ConnectionError:
            return False, "Error crítico: No se pudo contactar al ControlNode (Backend apagado)."

    def listar_directorio(self):
        """Se comunica con FsController.java para pedir el listado de archivos."""
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."
        url = f"{self.base_url}/fs/ls"
        payload = {"path": self.session.cwd}
        try:
            response = requests.post(url, json=payload, headers=self._get_headers())
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, f"Error al leer directorio: {response.status_code}"
        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."

    def subir_archivo(self, ruta_local):
        """Algoritmo manual para particionar archivos en bloques (Comando put)."""
        if not self.session.token:
            return False, "Acceso denegado: Inicia sesión antes de subir archivos."
        if not os.path.exists(ruta_local):
            return False, f"Error: El archivo '{ruta_local}' no existe en tu computadora."

        tamano_total = os.path.getsize(ruta_local)
        nombre_archivo = os.path.basename(ruta_local)
        metadatos_bloques = []
        numero_bloque = 0

        with open(ruta_local, "rb") as archivo:
            while True:
                pedazo = archivo.read(TAMANO_BLOQUE)
                if not pedazo:
                    break
                
                hash_pedazo = hashlib.sha256(pedazo).hexdigest()
                metadatos_bloques.append({
                    "bloque_id": numero_bloque,
                    "tamano_bytes": len(pedazo),
                    "hash_sha256": hash_pedazo
                })
                numero_bloque += 1
        
        resumen = (f"Archivo '{nombre_archivo}' ({tamano_total} bytes) "
                   f"particionado exitosamente en {numero_bloque} bloques.")
        return True, (resumen, metadatos_bloques)

    def simular_get(self, ruta_origen):
        """Algoritmo local para probar el reensamblaje de archivos (Comando test-get)."""
        if not os.path.exists(ruta_origen):
            return False, f"El archivo '{ruta_origen}' no existe para la simulación."

        nombre_original = os.path.basename(ruta_origen)
        nombre_salida = f"restaurado_{nombre_original}"
        bloques_en_memoria = []

        # Fase 1: Simulamos que descargamos los bloques de la red
        with open(ruta_origen, "rb") as archivo:
            while True:
                pedazo = archivo.read(TAMANO_BLOQUE)
                if not pedazo:
                    break
                bloques_en_memoria.append(pedazo)

        # Fase 2: Reensamblamos el archivo desde la memoria
        try:
            with open(nombre_salida, "wb") as archivo_restaurado:
                for pedazo in bloques_en_memoria:
                    archivo_restaurado.write(pedazo)
            return True, f"Éxito: Archivo reconstruido localmente como '{nombre_salida}'."
        except Exception as e:
            return False, f"Error al reconstruir el archivo: {str(e)}"
def cambiar_directorio(self, nueva_ruta):
    """Comando cd: Solicita cambiar la ruta actual."""
    if not self.session.token: return False, "Acceso denegado: Inicia sesión."
    
    url = f"{self.base_url}/fs/cd"
    # Enviamos dónde estamos y a dónde queremos ir
    payload = {"cwd": self.session.cwd, "path": nueva_ruta}
    
    try:
        response = requests.post(url, json=payload, headers=self._get_headers())
        if response.status_code == 200:
            data = response.json()
            # Actualizamos la memoria con la ruta que el servidor validó
            self.session.cwd = data.get("new_path", nueva_ruta)
            self.session.save()
            return True, f"Directorio actual: {self.session.cwd}"
        else:
            return False, f"Error: No se encontró la ruta '{nueva_ruta}'"
    except requests.ConnectionError:
        return False, "Error de red con el ControlNode."

def crear_directorio(self, nombre_carpeta):
    """Comando mkdir: Crea un directorio en el sistema."""
    if not self.session.token: return False, "Acceso denegado."
    
    url = f"{self.base_url}/fs/mkdir"
    payload = {"cwd": self.session.cwd, "name": nombre_carpeta}
    
    try:
        response = requests.post(url, json=payload, headers=self._get_headers())
        if response.status_code == 200 or response.status_code == 201:
            return True, f"Carpeta '{nombre_carpeta}' creada en {self.session.cwd}"
        else:
            return False, f"Error al crear carpeta: código {response.status_code}"
    except requests.ConnectionError:
        return False, "Error de red con el ControlNode."

def eliminar(self, nombre):
    """Comando rm: Elimina un archivo o directorio."""
    if not self.session.token: return False, "Acceso denegado."
    
    url = f"{self.base_url}/fs/rm"
    payload = {"cwd": self.session.cwd, "name": nombre}
    
    try:
        # Usamos requests.delete porque la acción es destructiva (Estándar REST)
        response = requests.delete(url, json=payload, headers=self._get_headers())
        if response.status_code == 200:
            return True, f"'{nombre}' eliminado exitosamente."
        else:
            return False, f"Error al eliminar: código {response.status_code}"
    except requests.ConnectionError:
        return False, "Error de red con el ControlNode."