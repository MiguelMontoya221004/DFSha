# API HTTP del DataNode

Version 0.4.0 · plano de datos de DFSha

El DataNode expone una API HTTP pequeña para subir y bajar bloques. Es la que usa el
cliente (`client/dfs_client.py`), que solo necesita `requests`, y la que usan los
DataNodes entre ellos para replicar.

Base: `http://<host>:<client_port>` — en el entorno local los DataNodes usan los puertos
9001 a 9004. No hay prefijo: el DataNode solo habla de bloques.

> El ControlNode es quien dice **en qué DataNodes** va cada bloque y entrega el **token**
> que autoriza la operación. Su API está documentada en
> [`controlnode/README.md`](../controlnode/README.md). Este documento es la otra mitad:
> qué hacer con esa información.

## 1. Autorización: el token de bloque

Cada operación necesita un token firmado por el ControlNode:

```
<block_id>:<usuario>:<op>:<vence>:<firma>
```

donde `op` es `read` o `write`, `vence` son milisegundos desde el epoch y `firma` es el
HMAC-SHA256 en base64url de todo lo anterior, con el secreto compartido
(`dfsha.block-secret` en el ControlNode, `DFSHA_BLOCK_SECRET` en el DataNode).

El DataNode no consulta usuarios ni contraseñas: recalcula la firma y comprueba que el
token sea de ese bloque, que autorice esa operación y que no se haya vencido.

Se manda de cualquiera de estas dos formas:

```
X-Block-Token: <token>              (cabecera, la recomendada)
GET /blocks/<id>?token=<token>      (parámetro, cómodo para probar con curl)
```

Si falta o no sirve, la respuesta es **403** con `{"error": "firma invalida"}`.

## 2. Subir un bloque

```
PUT /blocks/{block_id}
```

| Cabecera | Obligatoria | Para qué |
|---|---|---|
| `X-Block-Token` | sí | Token con `op = write` entregado por `/files/write/allocate` |
| `X-Block-Sha256` | recomendada | Checksum del contenido. El DataNode lo verifica y descarta el bloque si no coincide |
| `X-Pipeline` | no | Los **demás** DataNodes que deben recibir la copia, separados por coma: `dn3:9003,dn4:9004` |

El cuerpo es el contenido del bloque, tal cual. El DataNode lo escribe en disco y, si
`X-Pipeline` trae nodos, se lo reenvía al primero pasándole el resto: así el cliente sube
los datos **una sola vez** aunque queden tres copias.

Respuesta **200**:

```json
{
  "ok": true,
  "block_id": "4d91db59-c692-4e78-8d80-424018355089",
  "size": 4194304,
  "checksum": "862966e1...",
  "stored_on": ["dn-1", "dn-3", "dn-4"]
}
```

`stored_on` es la lista de DataNodes que confirmaron tener el bloque; va tal cual en el
campo `nodes` del `/files/write/commit`.

Antes de contestar, el DataNode avisa al ControlNode con `BlockReceived`, así que cuando
el cliente recibe este `ok` la réplica ya quedó escrita en Raft y el commit no se le
adelanta.

Errores: **403** token inválido · **422** el checksum no coincide · **500** error de disco.

```python
respuesta = requests.put(
    f"http://{nodo['host']}:{nodo['port']}/blocks/{block_id}",
    data=trozo,
    headers={"X-Block-Token": token,
             "X-Block-Sha256": hashlib.sha256(trozo).hexdigest(),
             "X-Pipeline": "dn3:9003,dn4:9004"},
    timeout=180)
nodos = respuesta.json()["stored_on"]
```

## 3. Bajar un bloque

```
GET /blocks/{block_id}
GET /blocks/{block_id}?offset=1024&length=64
```

Devuelve los bytes. Con `offset` y `length` devuelve solo ese pedazo, que es lo que
permite la lectura aleatoria (RF3) sin bajar el bloque completo. También funciona la
cabecera estándar `Range: bytes=100-199`, que responde **206** con `Content-Range`.

Cabeceras de la respuesta: `X-Block-Sha256` y `X-Block-Size`.

Errores: **403** token inválido · **404** ese bloque no está en este nodo · **416** rango
fuera del bloque.

**Failover:** `/files/read` devuelve las réplicas vivas de cada bloque. Si una falla
—nodo caído, timeout o checksum que no cuadra— el cliente prueba la siguiente. Por eso el
archivo sigue disponible aunque se caiga un DataNode.

## 4. Metadatos de un bloque

```
GET /blocks/{block_id}/meta  ->  {"block_id": "...", "size": 4194304, "checksum": "..."}
```

Útil para comprobar que una réplica está completa sin descargarla.

## 5. Borrar un bloque

```
DELETE /blocks/{block_id}  ->  {"ok": true, "block_id": "..."}
```

Requiere token de escritura. En la práctica el cliente no lo usa: cuando se borra un
archivo, el ControlNode manda la orden `DeleteBlock` en la respuesta del heartbeat.

## 6. Estado del nodo

```
GET /health  ->  {"estado": "ok", "node_id": "dn-1", "registrado": true}
GET /status  ->  node_id, zone, host, client_port, peer_port, version,
                 capacity_bytes, used_bytes, free_bytes, block_count,
                 active_transfers, control_nodes, registrado
```

`/health` no pide token: es lo que usan Docker y los scripts para saber si el nodo está
listo.

## 7. Lo que el DataNode habla con el ControlNode

Esto no lo ve el cliente, pero conviene tenerlo presente. Va por gRPC, con el contrato de
[`controlnode/src/dfsha.proto`](../controlnode/src/dfsha.proto):

| Llamada | Cuándo |
|---|---|
| `Register` | Al arrancar y cuando un ControlNode contesta `reregister` |
| `Heartbeat` | Cada 3 s, **a los tres ControlNodes**; la respuesta del líder trae las órdenes |
| `BlockReport` | Después del registro y cada 5 min: todo lo que tiene en disco |
| `BlockReceived` | Al guardar un bloque, antes de contestarle a quien lo mandó |
| `BlockRemoved` | Cuando pierde un bloque o lo borra por orden del ControlNode |

Órdenes que pueden llegar en el heartbeat:

- **`ReplicateBlock`** — copiar un bloque a otros DataNodes. El origen hace un `PUT` a cada
  destino y el **destino** es el que avisa con `BlockReceived`.
- **`DeleteBlock`** — borrar bloques de un archivo eliminado o de una versión vieja.

**Un detalle del contrato:** `ReplicateBlock` no trae token de bloque, así que el DataNode
origen firma uno con el secreto compartido (usuario `node:<id>`, válido 10 minutos) para
que el destino acepte la copia. Funciona porque los DataNodes comparten ese secreto con el
ControlNode; lo más limpio sería que el ControlNode incluyera el token en la orden, y vale
la pena anotarlo en el informe.

## 8. Detalles que conviene conocer

- **Tamaño de bloque:** 4 MiB, lo decide el ControlNode y viene en `block_size` al abrir la
  escritura. El cliente no debe asumirlo.
- **Transferencia por trozos:** el DataNode lee y escribe en trozos de 1 MiB, así que nunca
  carga un bloque entero en memoria.
- **Escritura atómica:** el bloque se escribe en un archivo temporal y se renombra al
  final; si el proceso muere a mitad, no queda un bloque corrupto.
- **Dos puertos:** `client_port` atiende al cliente y `peer_port` a los otros DataNodes. En
  esta implementación el pipeline también es HTTP, así que por defecto son el mismo.
- **Seguridad hoy:** HTTP plano dentro de la red del clúster, con tokens firmados. El TLS
  está pendiente, igual que en el ControlNode.
