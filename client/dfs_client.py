import hashlib
import os
import time

import requests
from session import Session


class DFSClient:
    def __init__(self, base_url=None):
        # URL donde escucha el ControlNode (Spring Boot). Si el lider cambia,
        # cualquiera de los tres responde igual. En Docker se pasa por la
        # variable de entorno DFSHA_CONTROLNODE_URL.
        self.base_url = base_url or os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8443")
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

    def registrar(self, username, password):
        """
        Crea el usuario y su carpeta /home/<usuario> en el ControlNode.
        """
        url = f"{self.base_url}/auth/register"
        payload = {"username": username, "password": password}

        try:
            response = requests.post(url, json=payload)
            if response.status_code in (200, 201):
                return True, f"Usuario {username} creado."
            return False, f"No se pudo crear el usuario: {response.text}"
        except requests.ConnectionError:
            return False, "Error crítico: No se pudo contactar al ControlNode (Backend apagado)."

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
                # Cada usuario arranca parado en su propia carpeta
                self.session.cwd = f"/home/{username}"
                self.session.save()
                return True, f"Login exitoso. Bienvenido {username}."
            else:
                return False, f"Error de credenciales: {response.status_code}"

        except requests.ConnectionError:
            # Si el contenedor de Java está apagado, atrapamos el error para no colapsar
            return False, "Error crítico: No se pudo contactar al ControlNode (Backend apagado)."

    def listar_directorio(self, ruta=None):
        """
        Se comunica con FsController.java para pedir el listado de archivos.
        """
        # Primero verificamos si hay una sesión activa
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        url = f"{self.base_url}/fs/ls"

        # El ControlNode espera la ruta como parámetro de la URL (es un GET)
        parametros = {"path": ruta or self.session.cwd}

        try:
            response = requests.get(url, params=parametros, headers=self._get_headers())

            if response.status_code == 200:
                # Retornamos la lista de archivos cruda que nos mandó Java
                return True, response.json()
            else:
                return False, f"Error al leer directorio: {response.status_code} {response.text}"

        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."

    def crear_directorio(self, ruta):
        """
        Crea una carpeta nueva (FsController.mkdir).
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        url = f"{self.base_url}/fs/mkdir"
        payload = {"path": self._ruta_absoluta(ruta)}

        try:
            response = requests.post(url, json=payload, headers=self._get_headers())
            if response.status_code == 200:
                return True, f"Carpeta creada: {payload['path']}"
            return False, f"No se pudo crear la carpeta: {response.text}"
        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."
    def cambiar_directorio(self, ruta):
        """
        Comando cd: Solicita cambiar la ruta actual en la sesión.
        Valida con el ControlNode si la ruta destino existe.
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        url = f"{self.base_url}/fs/cd"
        # Usamos la nueva función de tu amigo para siempre enviar la ruta absoluta
        payload = {"path": self._ruta_absoluta(ruta)}

        try:
            response = requests.post(url, json=payload, headers=self._get_headers())
            if response.status_code == 200:
                data = response.json()
                # Actualizamos la memoria con la ruta que el servidor validó
                self.session.cwd = data.get("new_path", payload["path"])
                self.session.save()
                return True, f"Directorio actual: {self.session.cwd}"
            else:
                return False, f"Error: No se encontró la ruta '{ruta}'"
        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."

    def eliminar(self, ruta):
        """
        Comando rm: Elimina un archivo o directorio.
        Hace una petición DELETE al ControlNode.
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        url = f"{self.base_url}/fs/rm"
        payload = {"path": self._ruta_absoluta(ruta)}

        try:
            response = requests.delete(url, json=payload, headers=self._get_headers())
            if response.status_code == 200:
                return True, f"Eliminado exitosamente: {ruta}"
            else:
                return False, f"Error al eliminar: {response.text}"
        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."
    # =====================================================================
    # SUBIR UN ARCHIVO
    #
    # Son cuatro pasos y vale la pena tenerlos claros, porque es el corazón
    # del sistema:
    #   1. /files/write/begin     -> el ControlNode abre una escritura y nos
    #                                dice de qué tamaño son los bloques
    #   2. /files/write/allocate  -> por cada bloque nos dice a qué 3 DataNodes
    #                                mandarlo y nos da un token firmado
    #   3. PUT al PRIMER DataNode -> él mismo le pasa la copia a los otros dos
    #                                (eso es el pipeline; nosotros subimos los
    #                                datos una sola vez)
    #   4. /files/write/commit    -> recién aquí el archivo se vuelve visible
    # =====================================================================

    def subir_archivo(self, ruta_local, ruta_remota=None):
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."
        if not os.path.isfile(ruta_local):
            return False, f"No existe el archivo local: {ruta_local}"

        destino = self._ruta_absoluta(ruta_remota or os.path.basename(ruta_local))

        try:
            # ---------- paso 1: abrir la escritura ----------
            respuesta = requests.post(f"{self.base_url}/files/write/begin",
                                      json={"path": destino}, headers=self._get_headers())
            if respuesta.status_code != 200:
                return False, f"No se pudo abrir la escritura: {respuesta.text}"

            apertura = respuesta.json()
            upload_id = apertura["upload_id"]
            tam_bloque = int(apertura["block_size"])

            bloques = []
            nodos_usados = set()
            with open(ruta_local, "rb") as archivo:
                indice = 0
                while True:
                    trozo = archivo.read(tam_bloque)
                    # Un archivo vacío también necesita su bloque, por eso el
                    # "and indice > 0"
                    if not trozo and indice > 0:
                        break

                    # ---------- pasos 2 y 3: pedir destino y mandar los bytes ----------
                    # Si un DataNode se cae justo cuando nos toca usarlo, el
                    # ControlNode todavía lo cree vivo por unos segundos (tarda
                    # un par de heartbeats en darse cuenta). En vez de botar toda
                    # la subida, se pide otro destino y se reintenta.
                    ok, resultado, ultimo_error = False, None, ""
                    for intento in range(1, 4):
                        respuesta = requests.post(f"{self.base_url}/files/write/allocate",
                                                  json={"upload_id": upload_id},
                                                  headers=self._get_headers())
                        if respuesta.status_code != 200:
                            self._abortar(upload_id)
                            return False, f"No se pudo asignar el bloque {indice}: {respuesta.text}"

                        asignado = respuesta.json()
                        ok, resultado = self._subir_bloque(asignado, trozo)
                        if ok:
                            break
                        ultimo_error = resultado
                        time.sleep(2)   # darle tiempo al ControlNode a marcarlo caído

                    if not ok:
                        self._abortar(upload_id)
                        return False, (f"Falló la subida del bloque {indice} "
                                       f"después de 3 intentos: {ultimo_error}")

                    nodos_usados.update(resultado["stored_on"])
                    bloques.append({
                        "index": indice,
                        "block_id": asignado["block_id"],
                        "size": len(trozo),
                        "checksum": resultado["checksum"],
                        "nodes": resultado["stored_on"],
                    })
                    indice += 1
                    if not trozo:
                        break

            # ---------- paso 4: confirmar ----------
            respuesta = requests.post(f"{self.base_url}/files/write/commit",
                                      json={"upload_id": upload_id, "blocks": bloques},
                                      headers=self._get_headers())
            if respuesta.status_code != 200:
                self._abortar(upload_id)
                return False, f"No se pudo confirmar el archivo: {respuesta.text}"

            return True, (f"Subido {destino} · {len(bloques)} bloque(s) · "
                          f"DataNodes: {', '.join(sorted(nodos_usados))}")

        except requests.ConnectionError:
            return False, "Error de red durante la subida."

    def _subir_bloque(self, asignado, datos):
        """
        Manda un bloque al PRIMER DataNode de la lista. En la cabecera
        X-Pipeline van los otros dos, para que él les pase la copia.
        """
        nodos = asignado["nodes"]
        checksum = hashlib.sha256(datos).hexdigest()
        errores = []

        # Se intenta con cada DataNode como cabeza de la cadena. Si el primero
        # está apagado (el ControlNode tarda unos segundos en notarlo), se le
        # manda al segundo y ese replica a los demás: el token sirve para
        # cualquiera de los tres, así que no hay que volver a pedir destino.
        for posicion, cabeza in enumerate(nodos):
            resto = [f"{n['host']}:{n['peer_port']}"
                     for i, n in enumerate(nodos) if i != posicion]

            url = f"http://{cabeza['host']}:{cabeza['port']}/blocks/{asignado['block_id']}"
            cabeceras = {
                "X-Block-Token": asignado["token"],
                "X-Block-Sha256": checksum,
                "X-Pipeline": ",".join(resto),
                "Content-Type": "application/octet-stream",
            }

            try:
                respuesta = requests.put(url, data=datos, headers=cabeceras, timeout=180)
            except requests.RequestException as error:
                errores.append(f"{cabeza['node_id']}: {error}")
                continue

            if respuesta.status_code == 200:
                return True, respuesta.json()
            errores.append(f"{cabeza['node_id']}: HTTP {respuesta.status_code} {respuesta.text}")

        return False, "ningún DataNode de la cadena aceptó el bloque (" + "; ".join(errores) + ")"

    def _abortar(self, upload_id):
        """Si algo sale mal, se cancela la escritura para no dejar una versión
        a medias ni bloques sueltos ocupando disco."""
        try:
            requests.post(f"{self.base_url}/files/write/abort",
                          json={"upload_id": upload_id}, headers=self._get_headers())
        except requests.RequestException:
            pass

    # =====================================================================
    # BAJAR UN ARCHIVO
    #
    #   1. /files/read -> el ControlNode nos da la lista de bloques, en qué
    #                     DataNodes está cada uno y un token de lectura
    #   2. GET a un DataNode por cada bloque. Si uno no responde, probamos la
    #      siguiente réplica: por eso el archivo sigue disponible aunque se
    #      caiga un nodo.
    # =====================================================================

    def descargar_archivo(self, ruta_remota, destino_local=None):
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        origen = self._ruta_absoluta(ruta_remota)
        destino_local = destino_local or os.path.basename(origen)

        try:
            respuesta = requests.get(f"{self.base_url}/files/read",
                                     params={"path": origen}, headers=self._get_headers())
            if respuesta.status_code != 200:
                return False, f"No se pudo abrir el archivo: {respuesta.text}"

            archivo = respuesta.json()
            nodos_usados = set()
            with open(destino_local, "wb") as salida:
                for bloque in archivo["blocks"]:
                    ok, resultado = self._bajar_bloque(bloque)
                    if not ok:
                        return False, resultado
                    datos, nodo = resultado
                    salida.write(datos)
                    nodos_usados.add(nodo)

            return True, (f"Descargado {destino_local} · {len(archivo['blocks'])} bloque(s) · "
                          f"leído de: {', '.join(sorted(nodos_usados))}")

        except requests.ConnectionError:
            return False, "Error de red durante la descarga."

    def _bajar_bloque(self, bloque):
        """
        Pide el bloque a las réplicas en orden hasta que una responda bien.
        Además comprueba el checksum: si una copia está corrupta, se pasa a la
        siguiente.
        """
        errores = []
        for nodo in bloque["nodes"]:
            url = f"http://{nodo['host']}:{nodo['port']}/blocks/{bloque['block_id']}"
            try:
                respuesta = requests.get(url, headers={"X-Block-Token": bloque["token"]},
                                         timeout=180)
                if respuesta.status_code != 200:
                    errores.append(f"{nodo['node_id']}: HTTP {respuesta.status_code}")
                    continue

                datos = respuesta.content
                if bloque.get("checksum") and hashlib.sha256(datos).hexdigest() != bloque["checksum"]:
                    errores.append(f"{nodo['node_id']}: checksum que no cuadra")
                    continue

                return True, (datos, nodo["node_id"])
            except requests.RequestException as error:
                errores.append(f"{nodo['node_id']}: {error}")

        return False, f"Ninguna réplica del bloque {bloque['block_id']} respondió ({'; '.join(errores)})"

    # =====================================================================

    def estado_cluster(self):
        """Pide el estado del clúster: líder de Raft y DataNodes vivos."""
        try:
            respuesta = requests.get(f"{self.base_url}/cluster", headers=self._get_headers())
            if respuesta.status_code == 200:
                return True, respuesta.json()
            return False, f"No se pudo consultar el clúster: {respuesta.status_code}"
        except requests.ConnectionError:
            return False, "Error de red con el ControlNode."

    def _ruta_absoluta(self, ruta):
        """Convierte 'archivo.txt' en '/home/ana/archivo.txt' usando el
        directorio en el que está parada la sesión."""
        if ruta.startswith("/"):
            return ruta
        base = self.session.cwd.rstrip("/")
        return f"{base}/{ruta}"
