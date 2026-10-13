# DFSha — Sistema de archivos distribuido con alta disponibilidad, rendimiento y seguridad

**Proyecto 1 · ST0263 Tópicos Especiales en Telemática / SI3007 Sistemas Distribuidos · 2026-2**

Integrantes: Samuel Aguilar  · Sebastián Martínez · Miguel Montoya 

---

## 1. Definición del servicio

DFSha es un sistema de archivos distribuido **por bloques** que permite a usuarios autenticados almacenar, recuperar y acceder a archivos grandes repartidos en varios nodos que se comunican sobre Internet. El cliente ve un espacio de nombres jerárquico tipo Linux (`/home/<usuario>`, `/shared`) y opera sobre él con un CLI, un shell interactivo o un SDK, sin saber en qué nodos están los datos.

### Problemática a resolver

Almacenar archivos grandes en un solo servidor limita la capacidad, el rendimiento y la disponibilidad: si ese servidor falla, los archivos quedan inaccesibles o se pierden, y todas las lecturas y escrituras compiten por el mismo disco y la misma red. DFSha resuelve esto partiendo cada archivo en bloques replicados sobre muchos nodos, de modo que:

- la capacidad y el ancho de banda crecen agregando nodos (escalabilidad);
- la caída de un nodo no detiene el servicio ni pierde datos (alta disponibilidad);
- la lectura y la escritura son paralelas entre varios nodos (rendimiento);
- los datos viajan y se almacenan cifrados, con control de acceso por usuario (seguridad).

### Modelo del servicio

- Archivos divididos en **bloques de 4 MiB** (configurable), cada uno con **3 réplicas** en máquinas distintas.
- **Versionamiento copy-on-write:** cada escritura genera una nueva versión que se publica de forma atómica al cerrar el archivo; los lectores siempre ven una versión completa y confirmada.
- Dos tipos de servicio sobre la misma base: **transferencia** (`put`/`send`, `get`/`receive`) y **acceso** (`open`, `read`, `write`, `seek`, `lock`, `close`).
- **Multiusuario** con autenticación y control de acceso por usuario y grupo.

## 2. Arquitectura escogida

**Arquitectura híbrida Cliente/Servidor + P2P** (opción 1 con plano de datos entre pares), organizada en dos planos:

| Plano | Modelo | Qué contiene | Por qué |
|---|---|---|---|
| **Control** | Cliente/Servidor con servidor replicado (maestro–trabajadores): **3 ControlNodes con Raft** (1 líder, 2 seguidores) | Espacio de nombres, ACL, usuarios, versiones, lista de bloques por archivo, locks | Autenticación, permisos y consistencia necesitan una autoridad única y orden total de operaciones. Raft elimina el punto único de falla |
| **Datos** | **P2P entre DataNodes** | Bloques cifrados y sus checksums | Los bytes nunca pasan por el ControlNode: replicación en pipeline y re-replicación entre pares, sin cuello de botella central |

**Componentes y lenguajes**

| Componente | Lenguaje | Responsabilidad |
|---|---|---|
| **Cliente** | Python | CLI, shell, SDK, cifrado AES-256-GCM, transferencia paralela de bloques con reintentos |
| **ControlNode** (×3) | Java 21 | Metadatos y namespace replicados con Raft, usuarios/ACL/JWT, colocación de réplicas, replicación, locks |
| **DataNode** (×N) | Python | Almacenamiento de bloques, pipeline de replicación, heartbeats y reportes, verificación de checksums |

**Protocolos de comunicación**

| Comunicación | Protocolo | Puerto | Seguridad | Estado |
|---|---|---|---|---|
| Cliente ↔ ControlNode | REST sobre HTTP/HTTPS (JSON) | 8080 | JWT + ACL · TLS en el Hito 3 | implementado |
| Cliente ↔ DataNode | HTTP (`PUT`/`GET` de bloques) | 50051 | Token de bloque HMAC · TLS en el Hito 3 | implementado |
| ControlNode ↔ DataNode | gRPC (registro, heartbeats, comandos) | 9090 | mTLS en el Hito 3 | implementado |
| DataNode ↔ DataNode | HTTP (pipeline y re-replicación) | 50051 | Token de bloque · mTLS en el Hito 3 | implementado |
| ControlNode ↔ ControlNode | Raft (Apache Ratis sobre gRPC) | 9500 | mTLS | Hito 3 |

Detalle completo de mensajes, tiempos, reintentos y códigos de error:
[`docs/comunicaciones.md`](docs/comunicaciones.md) · contratos en
[`docs/api-datanode.md`](docs/api-datanode.md) (bloques),
[`docs/openapi.yaml`](docs/openapi.yaml) (metadatos) y
[`proto/dfsha.proto`](proto/dfsha.proto) (plano de control).

## 3. Requerimientos funcionales

| ID | Requerimiento | Operaciones |
|---|---|---|
| **RF1** | Gestión del sistema de archivos | `ls`, `cd`, `pwd`, `mkdir`, `rmdir`, `rm`, `mv`, `stat`, `chmod`, `setfacl` |
| **RF2** | Transferencia de archivos | `put`/`send` y `get`/`receive` con bloques en paralelo, reintentos y failover entre réplicas |
| **RF3** | Acceso a archivos | `open`, `read`, `seek`, `write` (copy-on-write), `close`, `lock`/`unlock` con lease |
| **RF4** | Usuarios y seguridad | Registro y login (usuario/contraseña), 2FA TOTP, API keys, grupos, ACL por usuario y grupo |
| **RF5** | Administración y monitoreo | Estado del clúster (líder Raft, nodos vivos, capacidad), bloques sub-replicados, `/health` |

Cada nodo expone una API: REST (OpenAPI) en el ControlNode y gRPC (`proto/dfsha.proto`) en los DataNodes.

## 4. Requerimientos no funcionales

| ID | Requerimiento | Cómo se cumple en DFSha |
|---|---|---|
| **RNF1** | Escalabilidad | Plano de datos horizontal: agregar DataNodes aumenta capacidad y ancho de banda. Los bytes no pasan por el ControlNode. Archivos de tamaño ilimitado (n bloques) |
| **RNF2** | Alta disponibilidad | 3 ControlNodes con Raft (tolera 1 caída, nuevo líder en < 5 s). Replicación 3× en máquinas distintas. Heartbeats cada 3 s y re-replicación automática |
| **RNF3** | Consistencia | Metadatos linealizables vía Raft. Quórum de escritura 2 de 3. Versiones inmutables publicadas atómicamente. Un solo escritor por archivo (lease). Checksums SHA-256 |
| **RNF4** | Particionamiento | Bloques de 4 MiB distribuidos entre nodos; réplicas de un mismo bloque siempre en máquinas/zonas distintas (*power of two choices* por carga y espacio libre) |
| **RNF5** | Rendimiento y concurrencia | Transferencia paralela de bloques (4–8 hilos), gRPC streaming en chunks de 1 MiB, lectura desde la réplica menos cargada, múltiples lectores concurrentes (MVCC) |
| **RNF6** | Seguridad | TLS 1.3 cliente–nodos y mTLS entre nodos con CA propia. Cifrado en reposo AES-256-GCM del lado del cliente (los DataNodes nunca ven datos en claro). bcrypt + JWT + TOTP + API keys. ACL tipo POSIX. Tokens de bloque firmados |
| **RNF7** | Transparencia | Tres niveles: SDK/API, shell con rutas relativas y (opcional) montaje FUSE como carpeta local. La ubicación de los bloques siempre se resuelve dinámicamente en cada apertura |
| **RNF8** | Requisitos adicionales | Se incorporarán las aclaraciones que defina el profesor |

## 5. Estado del proyecto

| Hito | Contenido | Estado |
|---|---|---|
| Hito 1 | Especificación del servicio, RF/RNF, arquitectura, contratos | completo |
| **Hito 2** | **Arquitectura distribuida funcionando y protocolos de comunicación** | **completo** |
| Hito 3 | Raft (3 ControlNodes), TLS/mTLS, 2FA, API keys, cifrado en reposo | pendiente |
| Hito 4 | Pruebas de rendimiento en AWS, informe y video | pendiente |

Lo que ya funciona (Hito 2), verificado por `tests/e2e_datanode.py` (19 pruebas, todas en verde):

- Espacio de nombres jerárquico multiusuario con ACL: `ls`, `cd`, `pwd`, `mkdir -p`, `rm -r`, `mv`, `stat`.
- Particionamiento en bloques de 4 MiB con **3 réplicas en zonas distintas**, colocadas con *power of two choices* según carga y espacio libre.
- Escritura con **pipeline de replicación** (el cliente sube los datos una sola vez) y **quórum de 2 de 3** para publicar la versión.
- Lectura con **failover** entre réplicas, lectura por rangos (`offset`/`length` o `Range`) y verificación SHA-256 de cada bloque.
- Versiones inmutables con publicación atómica y **copy-on-write** en escrituras parciales.
- **Un solo escritor por archivo** mediante lease con expiración.
- **Heartbeats, detección de nodos muertos y re-replicación P2P** automática.
- Seguridad básica: bcrypt, JWT, ACL tipo POSIX y **tokens de bloque HMAC** verificados por los DataNodes.

## 6. Estructura del repositorio

```
DFSha/
├── proto/dfsha.proto          contrato gRPC compartido (Java y Python)
├── controlnode/               ControlNode · Java 21 + Spring Boot + gRPC
│   └── src/main/java/io/dfsha/controlnode/
│       ├── api/               controladores REST y manejo de errores
│       ├── grpc/              servicio NodeControl para los DataNodes
│       ├── metadata/          MetadataStore (punto de extensión para Raft en el Hito 3)
│       ├── model/             INode, FileVersion, BlockMeta, DataNodeInfo, Lease, User
│       ├── security/          JWT, tokens de bloque, filtro de autenticación
│       └── service/           namespace, archivos, usuarios y BlockManager
├── datanode/                  DataNode · Python + FastAPI (bloques) + gRPC (control)
│   ├── main.py                arranque: API HTTP + hilo de heartbeats
│   ├── server.py              PUT/GET de bloques, pipeline de replicación, rangos
│   ├── storage.py             bloques en disco con escritura atómica y checksums
│   ├── control_node.py        registro, heartbeats, reportes y ejecución de comandos
│   ├── tokens.py              verificación de tokens de bloque
│   └── config.py              configuración por variables de entorno
├── client/                    Cliente · Python (`requests` + `typer`)
│   ├── dfs_client.py          llamadas al ControlNode y a los DataNodes
│   ├── main.py                comandos de la CLI
│   └── session.py             sesión local (token y directorio de trabajo)
├── deploy/docker-compose.yml  clúster local: 1 ControlNode + 4 DataNodes + cliente
├── scripts/                   gen_proto.sh · run-local.sh · stop-local.sh
├── tests/e2e_datanode.py      suite end-to-end (funcional, fallos y seguridad)
└── docs/                      comunicaciones.md · api-datanode.md · openapi.yaml
```

## 7. Cómo ejecutarlo

### Con Docker (recomendado)

```bash
cd deploy
docker compose up -d --build           # 1 ControlNode + 4 DataNodes + cliente
docker compose ps

# Comprobar que el clúster responde
curl -s localhost:8080/api/v1/health          # ControlNode
curl -s localhost:50051/status                # DataNode dn1

# Probar la alta disponibilidad
docker compose stop dn2
docker compose logs -f cn1 | grep -E "MUERTO|Re-replicando"
```

### Nativo, sin Docker (desarrollo)

```bash
# Requisitos: Java 21, Maven, Python 3.11+
pip install -r datanode/requirements.txt -r client/requirements.txt
./scripts/gen_proto.sh                 # stubs de Python desde el .proto
cd controlnode && mvn -q package -DskipTests && cd ..
./scripts/run-local.sh 4               # ControlNode + 4 DataNodes en esta máquina

python3 tests/e2e_datanode.py          # levanta su propio clúster y lo prueba
curl -s localhost:50051/status         # estado de un DataNode
./scripts/stop-local.sh
```

### Variables de entorno principales

| Variable | Componente | Por defecto | Para qué |
|---|---|---|---|
| `DFSHA_BLOCK_SIZE` | ControlNode | 4194304 | Tamaño de bloque |
| `DFSHA_REPLICATION` / `DFSHA_MIN_REPLICAS` | ControlNode | 3 / 2 | Réplicas deseadas y quórum de escritura |
| `DFSHA_NODE_DEAD_MS` | ControlNode | 15000 | Cuándo se declara muerto un DataNode |
| `DFSHA_JWT_SECRET` · `DFSHA_BLOCK_SECRET` | ControlNode y DataNode | valores de demo | Secretos compartidos (cambiar en despliegue) |
| `DFSHA_NODE_ID` · `DFSHA_ZONE` · `DFSHA_DN_ADVERTISE` | DataNode | dn1 · zona-a · localhost:50051 | Identidad, zona y dirección anunciada |
| `DFSHA_CONTROLNODE` | DataNode | localhost:9090 | gRPC del ControlNode |
| `DFSHA_CONTROLNODE_URL` | Cliente | http://localhost:8080 | API REST del ControlNode |

## 8. Pruebas

```bash
python3 tests/e2e_datanode.py          # levanta el clúster, ejecuta 19 pruebas y lo apaga
python3 tests/e2e_datanode.py --keep   # deja el clúster arriba para inspeccionarlo
```

La suite usa solo HTTP, igual que el cliente: sirve además de referencia de cómo llamar a
la API. Detalle de los casos: [`docs/pruebas-hito2.md`](docs/pruebas-hito2.md).

## 9. Documentación

| Documento | Contenido |
|---|---|
| [`docs/comunicaciones.md`](docs/comunicaciones.md) | Especificación de comunicaciones del Hito 2: matriz de protocolos, mensajes, tiempos, reintentos, errores y flujos |
| [`docs/api-datanode.md`](docs/api-datanode.md) | API HTTP del DataNode: subir, bajar y leer por rangos un bloque, con ejemplos en `requests` |
| [`docs/openapi.yaml`](docs/openapi.yaml) | API REST del ControlNode (OpenAPI 3) |
| [`proto/dfsha.proto`](proto/dfsha.proto) | Servicios y mensajes gRPC del plano de control |
| [`docs/pruebas-hito2.md`](docs/pruebas-hito2.md) | Plan de pruebas y resultados |

## 10. Cronograma

| Semana | Hito |
|---|---|
| 1 | Hito 1 — Especificación definitiva, contratos y estructura del repositorio |
| 2–3 | Hito 2 — Arquitectura distribuida y comunicaciones: namespace, bloques, replicación en pipeline, lectura paralela |
| 4–5 | Hito 3 — Alta disponibilidad con Raft, consistencia y seguridad (mTLS, ACL, 2FA, cifrado) |
| 6 | Hito 4 — Pruebas de rendimiento en AWS, informe técnico y video |
