import json
import os

SESSION_FILE = "session.json"

class Session:
    def __init__(self):
        self.token = None
        self.username = None
        self.cwd = "/"  # Current Working Directory (siempre iniciamos en la raíz)
        self.load()

    def load(self):
        """Carga los datos del archivo local si existe."""
        if os.path.exists(SESSION_FILE):
            try:
                with open(SESSION_FILE, "r") as f:
                    data = json.load(f)
                    self.token = data.get("token")
                    self.username = data.get("username")
                    self.cwd = data.get("cwd", "/")
            except:
                pass # Si hay un error leyendo, empezamos como invitados limpios

    def save(self):
        """Guarda el estado actual en el disco."""
        data = {
            "token": self.token,
            "username": self.username,
            "cwd": self.cwd
        }
        with open(SESSION_FILE, "w") as f:
            json.dump(data, f)

    def login(self, username, token):
        """Actualiza la sesión tras un login exitoso."""
        self.username = username
        self.token = token
        self.cwd = "/"
        self.save()

    def logout(self):
        """Limpia la sesión borrando el archivo."""
        self.token = None
        self.username = None
        self.cwd = "/"
        if os.path.exists(SESSION_FILE):
            os.remove(SESSION_FILE)