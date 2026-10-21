# DFSha — Sistema de archivos distribuido con alta disponibilidad, rendimiento y seguridad

**Proyecto 1 · ST0263 Tópicos Especiales en Telemática / SI3007 Sistemas Distribuidos · 2026-2**

Integrantes: Samuel Aguilar · Sebastián Martínez · Miguel Montoya

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

Detalle de protocolos, mensajes, tiempos y errores: [`docs/comunicaciones.md`](docs/comunicaciones.md).

| Comunicación | Protocolo | Puerto | Contrato |
|---|---|---|---|
| Cliente ↔ ControlNode | REST/JSON + JWT | 8443-8445 | [`controlnode/README.md`](controlnode/README.md) |
| Cliente ↔ DataNode | HTTP (`PUT`/`GET` de bloques) | 9001-9004 | [`docs/api-datanode.md`](docs/api-datanode.md) |
| ControlNode ↔ DataNode | gRPC | 50051-50053 | [`controlnode/src/dfsha.proto`](controlnode/src/dfsha.proto) |
| DataNode ↔ DataNode | HTTP (pipeline) | 9001-9004 | [`docs/api-datanode.md`](docs/api-datanode.md) |
| ControlNode ↔ ControlNode | Raft (Apache Ratis) | 9501-9503 | Apache Ratis |

## 3. Qué funciona

Verificado por `tests/e2e_sistema.py`, que levanta el sistema completo y corre 22 pruebas:

- Usuarios con JWT y permisos POSIX; cada usuario arranca en su `/home/<usuario>`.
- `ls`, `mkdir`, `mv`, `rm`, `stat`, `chmod` sobre un árbol de directorios replicado con Raft.
- `put` y `get`: el archivo se parte en bloques, cada bloque se replica **3 veces en zonas
  distintas** por un **pipeline** entre DataNodes (el cliente sube los datos una sola vez).
- Publicación **atómica**: la versión solo se ve después del commit; hay lease de un solo
  escritor por archivo.
- **Failover de lectura**: si una réplica no responde o trae un checksum que no cuadra, el
  cliente pasa a la siguiente.
- **Re-replicación automática**: al caerse un DataNode, el líder ordena copiar los bloques
  que quedaron con menos de tres réplicas, y el sistema vuelve solo a su estado normal.
- Lectura **por rangos** (`offset`/`length` o cabecera `Range`), que es la base del acceso
  aleatorio del RF3.
- **Tokens de bloque** firmados: el DataNode rechaza lecturas y escrituras sin autorización.
- Checksums SHA-256 por bloque, con escritura atómica en disco.

Pendiente: TLS/mTLS, 2FA, cifrado de los bloques en reposo, y las mediciones de
rendimiento en AWS.

## 4. Estructura del repositorio

```
DFSha/
├── controlnode/               ControlNode · Java 21 + Spring Boot + Apache Ratis + H2
│   ├── src/dfsha.proto        contrato gRPC con los DataNodes
│   ├── src/dfsha/             RaftService, MetadataStateMachine, Db, controllers, Auth
│   ├── src/schema.sql         tablas de los metadatos
│   └── README.md              su API REST y cómo correrlo
├── datanode/                  DataNode · Python + FastAPI (bloques) + gRPC (control)
│   ├── main.py                arranque: API HTTP + hilo de heartbeats
│   ├── server.py              PUT/GET de bloques, pipeline, rangos, tokens
│   ├── storage.py             bloques en disco, escritura atómica, checksums
│   ├── control_node.py        registro, heartbeats y órdenes de los 3 ControlNodes
│   ├── tokens.py              verificación de los tokens de bloque
│   └── config.py              configuración por variables de entorno
├── client/                    Cliente · Python (requests + typer)
│   ├── dfs_client.py          login, ls, mkdir, put, get, status
│   ├── main.py                la CLI
│   └── session.py             sesión local (token y directorio actual)
├── deploy/docker-compose.yml  3 ControlNodes + 4 DataNodes + cliente
├── scripts/                   run-local.sh · stop-local.sh · gen_proto.sh
├── tests/e2e_sistema.py       prueba end-to-end del sistema completo
└── docs/                      comunicaciones.md · api-datanode.md · pruebas.md
```

## 5. Cómo ejecutarlo

### Con Docker

```bash
cd deploy
docker compose up -d --build        # 3 ControlNodes + 4 DataNodes + cliente
curl -s localhost:8443/cluster | python3 -m json.tool

docker compose exec client dfsha register ana
docker compose exec client dfsha login ana
docker compose exec client sh -c 'dd if=/dev/urandom of=/datos/prueba.bin bs=1M count=12'
docker compose exec client dfsha put /datos/prueba.bin
docker compose exec client dfsha ls
docker compose exec client dfsha get prueba.bin /datos/copia.bin
```

### Nativo

Requisitos: **Java 21** (Maven lo baja el wrapper) y **Python 3.11+**.

```bash
pip install -r datanode/requirements.txt -r client/requirements.txt
cd controlnode && ./mvnw package -DskipTests && cd ..   # en Windows: mvnw.cmd

./scripts/run-local.sh 4            # 3 ControlNodes + 4 DataNodes

cd client
python3 main.py register ana
python3 main.py login ana
python3 main.py put ../archivo.bin
python3 main.py ls
python3 main.py get archivo.bin copia.bin
python3 main.py status

cd .. && ./scripts/stop-local.sh
```

### Variables de entorno del DataNode

| Variable | Por defecto | Para qué |
|---|---|---|
| `DFSHA_NODE_ID` · `DFSHA_ZONE` | `dn-a1` · `zona-a` | Identidad y zona del nodo |
| `DFSHA_HOST` · `DFSHA_CLIENT_PORT` | `localhost` · `9001` | Dirección que se anuncia al ControlNode |
| `DFSHA_CONTROLNODES` | `localhost:50051,...` | Los tres ControlNodes (gRPC) |
| `DFSHA_DATA_DIR` | `./data/bloques` | Dónde guarda los bloques |
| `DFSHA_BLOCK_SECRET` | igual que `dfsha.block-secret` | Secreto compartido de los tokens |
| `DFSHA_CAPACITY_BYTES` | 8 GiB | Capacidad que reporta |

Los parámetros del ControlNode están en `controlnode/src/application.properties`.

## 6. Pruebas

```bash
python3 tests/e2e_sistema.py          # levanta todo, corre 22 pruebas y lo apaga
python3 tests/e2e_sistema.py --keep   # deja el clúster arriba para inspeccionarlo
```

Detalle de los casos y la última ejecución: [`docs/pruebas.md`](docs/pruebas.md).

## 7. Documentación

| Documento | Contenido |
|---|---|
| [`docs/comunicaciones.md`](docs/comunicaciones.md) | Especificación de comunicaciones: matriz de protocolos, mensajes, tiempos, errores y flujos |
| [`controlnode/README.md`](controlnode/README.md) | API REST del ControlNode y cómo levantar los tres nodos |
| [`docs/api-datanode.md`](docs/api-datanode.md) | API HTTP del DataNode, con ejemplos en `requests` |
| [`docs/pruebas.md`](docs/pruebas.md) | Plan de pruebas y resultados |
| [`docs/Hito1-Especificacion.md`](docs/Hito1-Especificacion.md) | Especificación del servicio (Hito 1) |
