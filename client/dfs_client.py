import hashlib
import os
import time

import requests
from session import Session


class DFSClient:
    def __init__(self, urls=None):
        # Aceptamos una lista de URLs separadas por coma para Alta Disponibilidad (HA)
        urls_env = os.getenv("DFSHA_CONTROLNODE_URLS", "http://cn1:8080,http://cn2:8080,http://cn3:8080")
        self.control_nodes = urls.split(",") if urls else urls_env.split(",")
        self.session = Session()

    def _get_headers(self):
        """
        Inyecta el Token JWT en las peticiones HTTP si el usuario está logueado.
        """
        headers = {"Content-Type": "application/json"}
        if self.session.token:
            headers["Authorization"] = f"Bearer {self.session.token}"
        return headers

    def _peticion_cn(self, metodo, endpoint, **kwargs):
        """
        Intenta enviar la petición a los ControlNodes en orden. 
        Si uno está apagado (ConnectionError), prueba con el siguiente.
        """
        ultimo_error = None
        for nodo in self.control_nodes:
            url = f"{nodo.strip()}{endpoint}"
            try:
                # Timeout corto para no congelar la CLI si el nodo murió
                kwargs.setdefault('timeout', 5)
                
                if metodo.upper() == "GET":
                    return requests.get(url, **kwargs)
                elif metodo.upper() == "POST":
                    return requests.post(url, **kwargs)
                elif metodo.upper() == "DELETE":
                    return requests.delete(url, **kwargs)
                
            except requests.ConnectionError as e:
                ultimo_error = e
                continue # Probamos el siguiente nodo de la lista
                
        # Si salimos del ciclo, todos los nodos están caídos
        raise requests.ConnectionError(f"Clúster inalcanzable. Último error: {ultimo_error}")

    def registrar(self, username, password):
        """
        Crea el usuario y su carpeta /home/<usuario> en el ControlNode.
        """
        payload = {"username": username, "password": password}
        try:
            response = self._peticion_cn("POST", "/auth/register", json=payload)
            if response.status_code in (200, 201):
                return True, f"Usuario {username} creado."
            return False, f"No se pudo crear el usuario: {response.text}"
        except requests.ConnectionError:
            return False, "Error crítico: No se pudo contactar al clúster (Backend apagado)."

    def login(self, username, password):
        """
        Se comunica con AuthController.java.
        """
        payload = {"username": username, "password": password}
        try:
            response = self._peticion_cn("POST", "/auth/login", json=payload)

            if response.status_code == 200:
                data = response.json()
                token = data.get("token")

                self.session.login(username, token)
                self.session.cwd = f"/home/{username}"
                self.session.save()
                return True, f"Login exitoso. Bienvenido {username}."
            else:
                return False, f"Error de credenciales: {response.status_code}"
        except requests.ConnectionError:
            return False, "Error crítico: No se pudo contactar al clúster (Backend apagado)."

    def listar_directorio(self, ruta=None):
        """
        Se comunica con FsController.java para pedir el listado de archivos.
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        parametros = {"path": ruta or self.session.cwd}
        try:
            response = self._peticion_cn("GET", "/fs/ls", params=parametros, headers=self._get_headers())
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, f"Error al leer directorio: {response.status_code} {response.text}"
        except requests.ConnectionError:
            return False, "Error de red con el clúster de control."

    def crear_directorio(self, ruta):
        """
        Crea una carpeta nueva (FsController.mkdir).
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        payload = {"path": self._ruta_absoluta(ruta)}
        try:
            response = self._peticion_cn("POST", "/fs/mkdir", json=payload, headers=self._get_headers())
            if response.status_code == 200:
                return True, f"Carpeta creada: {payload['path']}"
            return False, f"No se pudo crear la carpeta: {response.text}"
        except requests.ConnectionError:
            return False, "Error de red con el clúster de control."

    def cambiar_directorio(self, ruta):
        """
        Comando cd: Solicita cambiar la ruta actual en la sesión.
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        payload = {"path": self._ruta_absoluta(ruta)}
        try:
            response = self._peticion_cn("POST", "/fs/cd", json=payload, headers=self._get_headers())
            if response.status_code == 200:
                data = response.json()
                self.session.cwd = data.get("new_path", payload["path"])
                self.session.save()
                return True, f"Directorio actual: {self.session.cwd}"
            else:
                return False, f"Error: No se encontró la ruta '{ruta}'"
        except requests.ConnectionError:
            return False, "Error de red con el clúster de control."

    def eliminar(self, ruta):
        """
        Comando rm: Elimina un archivo o directorio.
        """
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        payload = {"path": self._ruta_absoluta(ruta)}
        try:
            response = self._peticion_cn("DELETE", "/fs/rm", json=payload, headers=self._get_headers())
            if response.status_code == 200:
                return True, f"Eliminado exitosamente: {ruta}"
            else:
                return False, f"Error al eliminar: {response.text}"
        except requests.ConnectionError:
            return False, "Error de red con el clúster de control."

    # =====================================================================
    # SUBIR UN ARCHIVO
    # =====================================================================

    def subir_archivo(self, ruta_local, ruta_remota=None):
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."
        if not os.path.isfile(ruta_local):
            return False, f"No existe el archivo local: {ruta_local}"

        destino = self._ruta_absoluta(ruta_remota or os.path.basename(ruta_local))

        try:
            # ---------- paso 1: abrir la escritura ----------
            respuesta = self._peticion_cn("POST", "/files/write/begin", json={"path": destino}, headers=self._get_headers())
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
                    if not trozo and indice > 0:
                        break

                    # ---------- pasos 2 y 3: pedir destino y mandar los bytes ----------
                    ok, resultado, ultimo_error = False, None, ""
                    for intento in range(1, 4):
                        respuesta = self._peticion_cn("POST", "/files/write/allocate", json={"upload_id": upload_id}, headers=self._get_headers())
                        if respuesta.status_code != 200:
                            self._abortar(upload_id)
                            return False, f"No se pudo asignar el bloque {indice}: {respuesta.text}"

                        asignado = respuesta.json()
                        ok, resultado = self._subir_bloque(asignado, trozo)
                        if ok:
                            break
                        ultimo_error = resultado
                        time.sleep(2)   

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
            respuesta = self._peticion_cn("POST", "/files/write/commit", json={"upload_id": upload_id, "blocks": bloques}, headers=self._get_headers())
            if respuesta.status_code != 200:
                self._abortar(upload_id)
                return False, f"No se pudo confirmar el archivo: {respuesta.text}"

            return True, (f"Subido {destino} · {len(bloques)} bloque(s) · "
                          f"DataNodes: {', '.join(sorted(nodos_usados))}")

        except requests.ConnectionError:
            return False, "Error de red durante la subida."

    def _subir_bloque(self, asignado, datos):
        """
        Manda un bloque al PRIMER DataNode de la lista.
        """
        nodos = asignado["nodes"]
        checksum = hashlib.sha256(datos).hexdigest()
        errores = []

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
                # La transferencia de datos va directo al DataNode, no usa _peticion_cn
                respuesta = requests.put(url, data=datos, headers=cabeceras, timeout=180)
            except requests.RequestException as error:
                errores.append(f"{cabeza['node_id']}: {error}")
                continue

            if respuesta.status_code == 200:
                return True, respuesta.json()
            errores.append(f"{cabeza['node_id']}: HTTP {respuesta.status_code} {respuesta.text}")

        return False, "ningún DataNode de la cadena aceptó el bloque (" + "; ".join(errores) + ")"

    def _abortar(self, upload_id):
        try:
            self._peticion_cn("POST", "/files/write/abort", json={"upload_id": upload_id}, headers=self._get_headers())
        except requests.RequestException:
            pass

    # =====================================================================
    # BAJAR UN ARCHIVO
    # =====================================================================

    def descargar_archivo(self, ruta_remota, destino_local=None):
        if not self.session.token:
            return False, "Acceso denegado: Debes iniciar sesión primero."

        origen = self._ruta_absoluta(ruta_remota)
        destino_local = destino_local or os.path.basename(origen)

        try:
            respuesta = self._peticion_cn("GET", "/files/read", params={"path": origen}, headers=self._get_headers())
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
        """
        errores = []
        for nodo in bloque["nodes"]:
            url = f"http://{nodo['host']}:{nodo['port']}/blocks/{bloque['block_id']}"
            try:
                # La descarga va directo al DataNode
                respuesta = requests.get(url, headers={"X-Block-Token": bloque["token"]}, timeout=180)
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
            respuesta = self._peticion_cn("GET", "/cluster", headers=self._get_headers())
            if respuesta.status_code == 200:
                return True, respuesta.json()
            return False, f"No se pudo consultar el clúster: {respuesta.status_code}"
        except requests.ConnectionError:
            return False, "Error de red con el clúster de control."

    def _ruta_absoluta(self, ruta):
        if ruta.startswith("/"):
            return ruta
        base = self.session.cwd.rstrip("/")
        return f"{base}/{ruta}"