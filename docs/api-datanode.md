# API HTTP del DataNode

Versión 0.3.0 · plano de datos de DFSha

El DataNode expone una API HTTP sencilla para subir y bajar bloques. Es la que usa el
cliente (`client/`), que solo necesita `requests`, y la que usan los DataNodes entre
ellos para replicar.

Base: `http://<host>:<puerto>` — por defecto el puerto `50051`. No hay prefijo `/api/v1`:
el DataNode solo habla de bloques.

> El ControlNode (REST, puerto 8080) es quien dice **en qué DataNodes** va cada bloque y
> entrega el **token** que autoriza la operación. Este documento es solo la segunda mitad:
> qué hacer con esa información. El contrato del ControlNode está en `docs/openapi.yaml`.

## 1. Autorización: el token de bloque

Cada operación sobre un bloque necesita un token firmado por el ControlNode. El DataNode
no consulta usuarios ni contraseñas: solo verifica la firma, que el token sea de ese
bloque, que autorice la operación (`r` leer, `w` escribir) y que no haya expirado
(10 minutos).

El token se manda de cualquiera de estas dos formas:

```
X-Block-Token: <token>              (cabecera, la recomendada)
GET /blocks/<id>?token=<token>      (parámetro, cómodo para probar con el navegador)
```

Si falta o no sirve, la respuesta es **403** con `{"detail": {"error": "TOKEN_INVALIDO", "message": "..."}}`.

## 2. Subir un bloque

```
PUT /blocks/{blockId}
```

| Cabecera | Obligatoria | Para qué |
|---|---|---|
| `X-Block-Token` | sí | El token de escritura que entregó el ControlNode |
| `X-Block-Sha256` | recomendada | Checksum del contenido. El DataNode lo verifica y rechaza el bloque si no coincide |
| `X-Pipeline` | no | Los **demás** DataNodes que deben recibir una copia, separados por coma: `dn3:50051,dn4:50051` |
| `Content-Type` | no | `application/octet-stream` |

El cuerpo es el contenido del bloque, tal cual. El DataNode lo escribe en disco, y si
`X-Pipeline` trae nodos, se lo reenvía al primero de la lista pasándole el resto. Así el
cliente sube los datos **una sola vez** aunque queden tres copias.

Respuesta **200**:

```json
{
  "ok": true,
  "blockId": "f7536404-7b4f-4aeb-b02c-0f6cdd19bca2",
  "size": 4194304,
  "sha256": "862966e1...",
  "storedOn": ["dn1", "dn3", "dn4"]
}
```

`storedOn` es la lista de DataNodes que confirmaron tener el bloque. El cliente la necesita
para saber si alcanzó el quórum (el ControlNode exige 2 de 3 para confirmar el archivo).

Errores: **403** token inválido · **422** el checksum no coincide (`CHECKSUM_INVALIDO`) ·
**500** error de disco.

```python
respuesta = requests.put(
    f"http://{destino}/blocks/{block_id}",
    data=trozo,                                   # bytes del bloque
    headers={"X-Block-Token": token,
             "X-Block-Sha256": hashlib.sha256(trozo).hexdigest(),
             "X-Pipeline": "dn3:50051,dn4:50051"},
    timeout=180)
nodos = respuesta.json()["storedOn"]
```

## 3. Bajar un bloque

```
GET /blocks/{blockId}
GET /blocks/{blockId}?offset=1024&length=64
```

Devuelve los bytes del bloque. Con `offset` y `length` devuelve solo ese pedazo, que es lo
que permite la lectura aleatoria (`seek` + `read`) sin bajar el bloque completo. También
funciona la cabecera estándar `Range: bytes=100-199`, que responde **206** con
`Content-Range`.

Cabeceras de la respuesta: `X-Block-Sha256` y `X-Block-Size`, para verificar lo recibido.

Errores: **403** token inválido · **404** ese bloque no está en este nodo · **416** rango fuera del bloque.

```python
respuesta = requests.get(f"http://{replica}/blocks/{block_id}",
                         headers={"X-Block-Token": token}, timeout=120)
datos = respuesta.content
assert hashlib.sha256(datos).hexdigest() == sha_esperado
```

**Failover:** el ControlNode devuelve las réplicas ordenadas de menos a más cargada. Si una
falla (nodo caído, timeout, checksum que no cuadra), el cliente debe probar la siguiente;
por eso el archivo sigue disponible aunque se caiga un DataNode.

## 4. Metadatos de un bloque

```
GET /blocks/{blockId}/meta      ->  {"blockId": "...", "size": 4194304, "sha256": "..."}
```

Útil para verificar que una réplica está completa sin descargarla.

## 5. Borrar un bloque

```
DELETE /blocks/{blockId}        ->  {"ok": true, "blockId": "..."}
```

Requiere token de escritura. En la práctica el cliente no lo usa: cuando se borra un
archivo, el ControlNode ordena a los DataNodes eliminar sus bloques en la respuesta del
heartbeat.

## 6. Estado del nodo

```
GET /health   ->  {"status": "ok", "nodeId": "dn1", "registrado": true}
GET /status   ->  nodeId, zone, address, capacityBytes, usedBytes, freeBytes,
                  blockCount, activeTransfers, controlNode, registrado
```

`/health` no pide token y es lo que usan Docker y los scripts para saber si el nodo está
listo.

## 7. El ciclo completo, de principio a fin

Así se sube y se baja un archivo. Los pasos 1, 2 y 5 son contra el ControlNode (REST con
el JWT); los pasos 3 y 6, contra los DataNodes (HTTP con el token de bloque).

```python
import hashlib, requests

CONTROL = "http://localhost:8080/api/v1"
auth = {"Authorization": f"Bearer {jwt}"}

# 1. Abrir el archivo para escritura: devuelve la version y el tamaño de bloque
creado = requests.post(f"{CONTROL}/files/create",
                       json={"path": "/home/ana/video.mp4", "overwrite": True},
                       headers=auth).json()
version, tam_bloque = creado["versionId"], creado["blockSize"]   # 4 MiB

bloques = []
for indice, trozo in enumerate(partir(archivo, tam_bloque)):
    # 2. Pedir destino para este bloque
    asignado = requests.post(f"{CONTROL}/versions/{version}/blocks",
                             json={"index": indice}, headers=auth).json()
    pipeline = [n["address"] for n in asignado["pipeline"]]       # 3 DataNodes

    # 3. Subir los bytes al primero; el se los pasa a los demas
    sha = hashlib.sha256(trozo).hexdigest()
    requests.put(f"http://{pipeline[0]}/blocks/{asignado['blockId']}",
                 data=trozo,
                 headers={"X-Block-Token": asignado["token"],
                          "X-Block-Sha256": sha,
                          "X-Pipeline": ",".join(pipeline[1:])}).raise_for_status()

    bloques.append({"index": indice, "blockId": asignado["blockId"],
                    "size": len(trozo), "sha256": sha})

# 4. Confirmar: el archivo recien aqui se vuelve visible para los demas
requests.post(f"{CONTROL}/versions/{version}/commit",
              json={"blocks": bloques}, headers=auth).raise_for_status()

# 5. Para leer: abrir y pedir las ubicaciones
abierto = requests.post(f"{CONTROL}/files/open",
                        json={"path": "/home/ana/video.mp4"}, headers=auth).json()

# 6. Bajar cada bloque de la primera replica que responda
partes = []
for bloque in abierto["blocks"]:
    for replica in bloque["replicas"]:
        try:
            r = requests.get(f"http://{replica['address']}/blocks/{bloque['blockId']}",
                             headers={"X-Block-Token": bloque["token"]}, timeout=120)
            r.raise_for_status()
            if hashlib.sha256(r.content).hexdigest() != bloque["sha256"]:
                continue                      # replica corrupta: probar la siguiente
            partes.append(r.content)
            break
        except requests.RequestException:
            continue                          # nodo caido: probar la siguiente
contenido = b"".join(partes)
```

Si algo falla a mitad de la subida, el cliente debe llamar
`POST /versions/{version}/abort` para que no quede una versión a medias y los bloques
sueltos se liberen.

`tests/e2e_datanode.py` tiene este mismo flujo ya escrito y probado: sirve de referencia
directa para `client/dfs_client.py`.

## 8. Detalles que conviene conocer

- **Tamaño de bloque:** 4 MiB, lo decide el ControlNode y viene en `blockSize`. El cliente
  no debe asumirlo.
- **Transferencia por trozos:** el DataNode lee y escribe en trozos de 1 MiB, así que un
  bloque nunca se carga entero en su memoria. Con `requests` se puede pasar un generador
  en `data=` para subir sin cargar el archivo completo.
- **Escritura atómica:** el bloque se escribe en un temporal y se renombra al final; si el
  proceso muere a mitad, no queda un bloque corrupto.
- **Checksums:** el DataNode guarda el SHA-256 de cada bloque y puede recalcularlo para
  detectar corrupción silenciosa.
- **Seguridad hoy:** HTTP plano dentro de la red del clúster, con tokens firmados. El TLS
  y el cifrado en reposo entran en el Hito 3.
