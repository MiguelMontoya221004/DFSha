# DFSha — Sistema de archivos distribuido con alta disponibilidad, rendimiento y seguridad

**Proyecto 1 · ST0263 Tópicos Especiales en Telemática / SI3007 Sistemas Distribuidos · 2026-2**

Integrantes: Samuel Aguilar · Sebastián Martínez · Miguel Montoya
Video: https://youtu.be/FwuOfn7X9eA
---

## 1. Qué es

DFSha guarda archivos grandes partiéndolos en **bloques de 4 MiB** que se reparten, con
**tres réplicas en zonas distintas**, entre varios DataNodes. El usuario ve un árbol de
directorios tipo Linux y trabaja con un cliente de línea de comandos; nunca se entera de
en qué nodo está cada pedazo.

**La problemática:** un solo servidor limita la capacidad, el rendimiento y la
disponibilidad — si se cae, los archivos quedan inaccesibles. Repartiendo los bloques:

- la capacidad y el ancho de banda crecen agregando nodos (escalabilidad);
- la caída de un nodo no detiene el servicio ni pierde datos (alta disponibilidad);
- los metadatos sobreviven a la caída de un ControlNode gracias a Raft (consistencia);
- cada operación sobre un bloque va autorizada con un token firmado (seguridad).

## 2. Arquitectura

**Híbrida**: un plano de control cliente/servidor replicado y un plano de datos entre
pares.

| Plano | Quién | Qué hace |
|---|---|---|
| **Control** | 3 **ControlNodes** en Java, replicados con **Apache Ratis (Raft)** | Usuarios, permisos, árbol de directorios, versiones de cada archivo, en qué DataNodes está cada bloque, locks. Decide la colocación de réplicas y ordena las re-replicaciones |
| **Datos** | N **DataNodes** en Python | Guardan los bloques y se pasan copias entre ellos. Los bytes nunca pasan por el ControlNode |
| **Cliente** | Python (`requests` + `typer`) | Habla REST con el ControlNode y HTTP con los DataNodes |

```
                 Cliente (CLI)
                 │            ║
   ① REST + JWT  │            ║  ② HTTP PUT/GET + token de bloque
                 ▼            ▼
      ┌──────────────────┐   ┌─────────────────────────────────┐
      │  ControlNodes    │◄──│  DataNodes                      │
      │  n1 · n2 · n3    │ ③ │  dn-1 ⇄ dn-2 ⇄ dn-3 ⇄ dn-4      │
      │  Raft (Ratis)    │──►│  ⑤ pipeline y re-replicación    │
      └──────────────────┘ ④ └─────────────────────────────────┘
```

① metadatos y permisos · ② los bytes de los archivos · ③ registro, heartbeats y reportes ·
④ órdenes dentro de la respuesta del heartbeat · ⑤ copias entre DataNodes

| Comunicación | Protocolo | Puerto | Contrato |
|---|---|---|---|
| Cliente ↔ ControlNode | REST/JSON + JWT | 8443-8445 | [`controlnode/README.md`](controlnode/README.md) |
| Cliente ↔ DataNode | HTTP (`PUT`/`GET` de bloques) | 9001-9004 | este README, §7 |
| ControlNode ↔ DataNode | gRPC `ControlService` | 50051-50053 | [`controlnode/src/dfsha.proto`](controlnode/src/dfsha.proto) |
| DataNode ↔ DataNode | gRPC `DataNodeService.TransferBlock` | 9011-9014 (*peer port*) | [`controlnode/src/dfsha.proto`](controlnode/src/dfsha.proto) |
| ControlNode ↔ ControlNode | Raft (Apache Ratis) | 9501-9503 | Apache Ratis |

Cada DataNode escucha en **dos puertos distintos**: uno para el cliente (`DFSHA_CLIENT_PORT`,
HTTP) y otro para los demás DataNodes (`DFSHA_PEER_PORT`, gRPC). No pueden ser el mismo.

### El camino de un bloque al subir

1. El cliente parte el archivo en bloques de 4 MiB y le pide al ControlNode
   `/files/write/begin` y luego un `/files/write/allocate` por bloque.
2. El ControlNode responde con **tres DataNodes en zonas distintas** y un **token de bloque**
   firmado (HMAC-SHA256) que vale solo para ese bloque, ese usuario, esa operación y un rato.
3. El cliente sube el bloque **una sola vez**, por HTTP, al primer DataNode de la lista.
4. Ese DataNode lo guarda y se lo **reenvía por gRPC** al siguiente, que hace lo mismo con el
   tercero: es el *pipeline*. Cada uno avisa al ControlNode (`BlockReceived`) que ya tiene su
   copia.
5. Cuando están los bloques, el cliente hace `/files/write/commit` y recién ahí la versión
   nueva del archivo se vuelve visible (publicación atómica).

## 3. Qué funciona

- Usuarios con JWT y permisos POSIX; cada usuario arranca en su `/home/<usuario>`.
- Árbol de directorios replicado con Raft. La CLI expone `ls`, `mkdir`, `cd` y `rm`; el
  ControlNode además ofrece `stat`, `mv`, `rmdir` y `chmod` por REST.
- `put` y `get`: el archivo se parte en bloques, cada bloque se replica **3 veces en zonas
  distintas** por un **pipeline** entre DataNodes (el cliente sube los datos una sola vez).
- Publicación **atómica**: la versión solo se ve después del commit; hay lease de un solo
  escritor por archivo.
- **Failover de lectura**: si una réplica no responde o trae un checksum que no cuadra, el
  cliente pasa a la siguiente.
- **Failover del plano de control**: el cliente recibe las direcciones de los tres
  ControlNodes y, si uno no contesta, reintenta con el siguiente.
- **Re-replicación automática**: al caerse un DataNode, el líder ordena copiar los bloques
  que quedaron con menos de tres réplicas, y el sistema vuelve solo a su estado normal.
- **Tolerancia a un DataNode caído en la cadena**: si un nodo del pipeline no responde, la
  escritura no se pierde; el ControlNode repone la réplica que falte.
- Lectura **por rangos** (`offset`/`length` o cabecera `Range`), base del acceso aleatorio.
- **Tokens de bloque** firmados: el DataNode rechaza lecturas y escrituras sin autorización,
  tanto por HTTP como por el puerto del pipeline.
- Checksums SHA-256 por bloque, con escritura atómica en disco.

Pendiente: TLS/mTLS, 2FA, cifrado de los bloques en reposo, envío por trozos entre DataNodes
(hoy el bloque viaja completo en un mensaje gRPC) y las mediciones de rendimiento en AWS.

## 4. Estructura del repositorio

```
DFSha/
├── controlnode/               ControlNode · Java 21 + Spring Boot + Apache Ratis + H2
│   ├── src/dfsha.proto        contrato gRPC: ControlService y DataNodeService
│   ├── src/dfsha/             RaftService, MetadataStateMachine, Db, controllers, Auth
│   ├── src/schema.sql         tablas de los metadatos
│   ├── src/application.properties   parámetros del ControlNode
│   ├── Dockerfile
│   └── README.md              su API REST y cómo correrlo
├── datanode/                  DataNode · Python + FastAPI (bloques) + gRPC (control y pipeline)
│   ├── main.py                arranque: API HTTP + servidor gRPC + hilo de heartbeats
│   ├── server.py              PUT/GET de bloques, pipeline, rangos, tokens
│   ├── storage.py             bloques en disco, escritura atómica, checksums
│   ├── control_node.py        registro, heartbeats y órdenes de los 3 ControlNodes
│   ├── tokens.py              verificación de los tokens de bloque
│   ├── config.py              configuración por variables de entorno
│   ├── pb/                    stubs gRPC generados desde dfsha.proto
│   └── Dockerfile
├── client/                    Cliente · Python (requests + typer)
│   ├── main.py                la CLI
│   ├── dfs_client.py          register, login, ls, mkdir, cd, rm, put, get, status
│   ├── session.py             sesión local (token y directorio actual)
│   └── Dockerfile
├── datos/                     carpeta que el contenedor del cliente ve como /datos
└── docker-compose.yml         3 ControlNodes + 4 DataNodes + cliente
```

## 5. Cómo ejecutarlo

### Con Docker

```bash
# 1. Configuración: la dirección con la que los DataNodes se anuncian entre sí.
#    Tiene que ser una IP que los contenedores alcancen; 'localhost' no sirve,
#    porque cada contenedor se buscaría a sí mismo y el pipeline no llegaría.
echo "IP_PUBLICA=$(hostname -I | awk '{print $1}')" > .env

# 2. Arriba: 3 ControlNodes + 4 DataNodes + cliente
docker compose up -d --build
curl -s localhost:8443/cluster | python3 -m json.tool
```

El cliente se usa desde su contenedor. Necesita saber dónde están los tres ControlNodes:

```bash
alias dfsha='docker compose exec \
  -e DFSHA_CONTROLNODE_URLS=http://cn1:8443,http://cn2:8443,http://cn3:8443 \
  client dfsha'

dfsha register ana
dfsha login ana
dd if=/dev/urandom of=datos/prueba.bin bs=1M count=12
dfsha put /datos/prueba.bin
dfsha ls
dfsha get prueba.bin /datos/copia.bin
sha256sum datos/prueba.bin datos/copia.bin      # tienen que ser iguales
```

Para apagar todo: `docker compose down` (con `-v` también borra los datos).

### Nativo

Requisitos: **Java 21** (Maven lo baja el wrapper) y **Python 3.11+**.

```bash
pip install -r datanode/requirements.txt -r client/requirements.txt
cd controlnode && ./mvnw package -DskipTests && cd ..   # en Windows: mvnw.cmd
```

Los tres ControlNodes, una terminal cada uno:

```bash
java -jar controlnode/target/controlnode-1.0.0.jar --dfsha.node-id=n1 --server.port=8443 --dfsha.grpc-port=50051
java -jar controlnode/target/controlnode-1.0.0.jar --dfsha.node-id=n2 --server.port=8444 --dfsha.grpc-port=50052
java -jar controlnode/target/controlnode-1.0.0.jar --dfsha.node-id=n3 --server.port=8445 --dfsha.grpc-port=50053
```

Los DataNodes, una terminal cada uno. Cambia `1` por `2`, `3` y `4`, y las zonas por
`zona-a`, `zona-a`, `zona-b`, `zona-c` para que las réplicas queden repartidas:

```bash
DFSHA_NODE_ID=dn-1 DFSHA_HOST=127.0.0.1 \
DFSHA_CLIENT_PORT=9001 DFSHA_PEER_PORT=9101 \
DFSHA_ZONE=zona-a DFSHA_DATA_DIR=./data/dn-1 \
DFSHA_CONTROLNODES=127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 \
python3 datanode/main.py
```

Y el cliente:

```bash
export DFSHA_CONTROLNODE_URLS=http://127.0.0.1:8443,http://127.0.0.1:8444,http://127.0.0.1:8445
cd client
python3 main.py register ana
python3 main.py login ana
python3 main.py put ../archivo.bin
python3 main.py ls
python3 main.py get archivo.bin copia.bin
python3 main.py status
```

### Variables de entorno del DataNode

| Variable | Por defecto | Para qué |
|---|---|---|
| `DFSHA_NODE_ID` · `DFSHA_ZONE` | `dn-a1` · `zona-a` | Identidad y zona del nodo. El id tiene que sobrevivir a los reinicios: es con el que quedan guardadas las réplicas |
| `DFSHA_HOST` | `localhost` | Dirección con la que se anuncia al ControlNode y a los demás DataNodes |
| `DFSHA_CLIENT_PORT` | `9001` | Puerto HTTP para el cliente |
| `DFSHA_PEER_PORT` | igual al anterior | Puerto gRPC para el pipeline. **Hay que ponerlo distinto del de cliente** |
| `DFSHA_CONTROLNODES` | `localhost:50051,…` | Los tres ControlNodes (gRPC) |
| `DFSHA_DATA_DIR` | `./data/bloques` | Dónde guarda los bloques |
| `DFSHA_BLOCK_SECRET` | igual que `dfsha.block-secret` | Secreto compartido de los tokens |
| `DFSHA_CAPACITY_BYTES` | 8 GiB | Capacidad que reporta |
| `DFSHA_CHUNK_SIZE` | 1 MiB | Tamaño de los trozos al leer y escribir |
| `DFSHA_HEARTBEAT_S` | `3` | Cada cuánto manda el heartbeat |

Los parámetros del ControlNode están en
[`controlnode/src/application.properties`](controlnode/src/application.properties).

## 6. Cómo comprobar que funciona

Con el clúster arriba y un archivo ya subido:

```bash
# El DataNode está vivo y registrado en los tres ControlNodes
curl -s localhost:9001/status | python3 -m json.tool

# Cada bloque tiene 3 réplicas, en zonas distintas
JWT=$(docker compose exec -T client cat /app/session.json \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')
curl -s "localhost:8443/files/read?path=/home/ana/prueba.bin" \
     -H "Authorization: Bearer $JWT" | python3 -m json.tool

# El DataNode exige el token: sin él, 403
curl -i -X GET localhost:9001/blocks/cualquiera

# Caída de un nodo: el archivo se sigue leyendo y las réplicas se reponen solas
docker compose stop dn2
dfsha get prueba.bin /datos/copia2.bin
sha256sum datos/prueba.bin datos/copia2.bin
curl -s localhost:8443/admin/under-replicated -H "Authorization: Bearer $JWT"
```

El último comando muestra los bloques pendientes de reponer; a los pocos segundos
devuelve una lista vacía, que es la re-replicación terminada.

## 7. API HTTP del DataNode

Todas las rutas exigen el token de bloque, en la cabecera `X-Block-Token` o como
parámetro `?token=`. Sin él, o con uno que no corresponda, responden `403`.

| Método | Ruta | Para qué |
|---|---|---|
| `PUT` | `/blocks/{id}` | Sube el bloque. `X-Block-Sha256` lleva el checksum esperado y `X-Pipeline` la lista de los siguientes DataNodes, separados por coma |
| `GET` | `/blocks/{id}` | Baja el bloque entero, o un rango con `?offset=&length=` o la cabecera `Range` (responde `206`) |
| `GET` | `/blocks/{id}/meta` | Tamaño y checksum |
| `DELETE` | `/blocks/{id}` | Borra el bloque |
| `GET` | `/status` | Zona, capacidad, bloques guardados, transferencias en curso, si está registrado |
| `GET` | `/health` | Vivo o caído (lo usa Docker) |

Si el checksum no cuadra, el bloque se descarta y responde `422`. Los errores salen
siempre como `{"error": "..."}`, igual que los del ControlNode.

## 8. Documentación

| Documento | Contenido |
|---|---|
| [`controlnode/README.md`](controlnode/README.md) | API REST del ControlNode y cómo levantar los tres nodos |
| [`controlnode/src/dfsha.proto`](controlnode/src/dfsha.proto) | Contrato gRPC: `ControlService` (ControlNode ↔ DataNode) y `DataNodeService` (pipeline) |
| Este README, §2 y §7 | Matriz de comunicaciones y API HTTP del DataNode |

La especificación del Hito 1, la de comunicaciones del Hito 2 y el plan de pruebas están
en el historial del repositorio, en el commit `4c2b367`
(`git show 4c2b367:docs/comunicaciones.md`).
