import requests
from session import Session

class DFSClient:
    def __init__(self, base_url="http://localhost:8080"):
        # URL donde escucha tu backend en Java (Spring Boot)
        self.base_url = base_url
        # Instanciamos la memoria de la que hablamos antes
        self.session = Session()

    def _get_headers(self):
        """
        Método de ayuda interno (por eso el guion bajo).
        Inyecta el Token JWT en las peticiones HTTP si el usuario está logueado.
        """
        headers = {"Content-Type": "application/json"}
        if self.session.token:
            # Así es como Spring Security en Java espera recibir el token
            headers["Authorization"] = f"Bearer {self.session.token}"
        return headers

    def login(self, username, password):
        """
        Se comunica con AuthController.java.
        Retorna una tupla: (Éxito [Booleano], Mensaje [String])
        """
        url = f"{self.base_url}/auth/login"
        payload = {"username": username, "password": password}
        
        try:
            # Hacemos la petición POST
            response = requests.post(url, json=payload)
            
            # Si el backend de Java responde con 200 OK
            if response.status_code == 200:
                data = response.json()
                token = data.get("token")
                
                # Le decimos a la memoria (session.py) que guarde estos datos en el disco
                self.session.login(username, token)
                return True, f"Login exitoso. Bienvenido {username}."
            else:
                return False, f"Error de credenciales: {response.status_code}"
                
        except requests.ConnectionError:
            # Si el contenedor de Java está apagado, atrapamos el error para no colapsar
            return False, "Error crítico: No se pudo contactar al ControlNode (Backend apagado)."

    def listar_directorio(self):
        """
        Se comunica con FsController.java para pedir el listado de archivos.
        """
        # Primero verificamos si hay una sesión activa
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."
            
        url = f"{self.base_url}/fs/ls"
        
        # Le enviamos al backend la ruta en la que estamos parados actualmente
        payload = {"path": self.session.cwd}
        
        try:
            # Hacemos el POST, pero esta vez le pasamos _get_headers() para que lleve el Token
            response = requests.post(url, json=payload, headers=self._get_headers())
            
            if response.status_code == 200:
                # Retornamos la lista de archivos cruda que nos mandó Java
                return True, response.json()
            else:
                return False, f"Error al leer directorio: {response.status_code}"
                
        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."