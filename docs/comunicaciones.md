# DFSha — Especificación de comunicaciones (Hito 2)

ST0263 / SI3007 · 2026-2 · versión 0.2.0

Este documento define **cómo se hablan los componentes de DFSha**: protocolos, mensajes,
puertos, tiempos, reintentos y errores. Es el contrato que implementan el ControlNode
(Java), el DataNode (Python) y el Cliente (Python), y la referencia para cualquiera que
quiera escribir otro cliente.

Fuentes de verdad del contrato:

| Contrato | Archivo |
|---|---|
| gRPC (plano de datos y plano de control) | [`proto/dfsha.proto`](../proto/dfsha.proto) |
| REST (clientes ↔ ControlNode) | [`docs/openapi.yaml`](openapi.yaml) |

## 1. Alcance del Hito 2

| Implementado en este hito | Queda para el Hito 3 |
|---|---|
| Arquitectura distribuida funcionando: 1 ControlNode + N DataNodes + cliente | Clúster de 3 ControlNodes con Raft (Apache Ratis) |
| Particionamiento en bloques de 4 MiB y colocación con réplicas en zonas distintas | Rebalanceo al agregar DataNodes |
| Replicación 3× en pipeline entre DataNodes | — |
| Heartbeats, reportes de bloques, detección de fallos y re-replicación P2P | Verificación periódica de checksums (scrubbing) programada |
| Lectura paralela con failover entre réplicas y verificación SHA-256 | — |
| Versiones inmutables, publicación atómica, lease de un solo escritor, quórum 2 de 3 | Locks compartidos explícitos (`lock()`/`unlock()` del SDK) |
| Autenticación bcrypt + JWT, ACL tipo POSIX, tokens de bloque HMAC | 2FA TOTP, API keys, TLS/mTLS con CA propia, cifrado AES-256-GCM en reposo |

Todo lo de la columna izquierda está cubierto por la suite `tests/e2e_test.py` (23 pruebas).

## 2. Vista general

```mermaid
flowchart LR
    C["Cliente (Python)<br/>CLI · SDK"]
    CN["ControlNode (Java)<br/>REST :8080 · gRPC :9090"]
    subgraph DN["DataNodes (Python) · gRPC :50051"]
        D1[dn1 zona-a]
        D2[dn2 zona-a]
        D3[dn3 zona-b]
        D4[dn4 zona-c]
    end
    C -->|"① REST/JSON + JWT<br/>metadatos, asignación de bloques"| CN
    C ==>|"② gRPC streaming + token de bloque<br/>bytes de los bloques"| DN
    DN -->|"③ gRPC: Register · Heartbeat · BlockReport"| CN
    CN -.->|"④ comandos en la respuesta del heartbeat<br/>Replicate · Delete"| DN
    D1 ==>|"⑤ gRPC streaming: pipeline y re-replicación"| D3
```

Principio de diseño: **el ControlNode nunca transporta datos**. Sólo entrega metadatos,
decide dónde van los bloques y firma tokens; los bytes viajan cliente↔DataNode y
DataNode↔DataNode. De ahí salen la escalabilidad (RNF1) y el rendimiento (RNF5).

## 3. Matriz de comunicaciones

| # | Comunicación | Protocolo | Formato | Quién inicia | Patrón | Puerto | Seguridad (Hito 2) | Seguridad (Hito 3) |
|---|---|---|---|---|---|---|---|---|
| ① | Cliente → ControlNode | HTTP/1.1 REST | JSON | Cliente | Petición/respuesta | 8080 | JWT (HS256) | HTTPS/TLS 1.3 + JWT, API keys |
| ② | Cliente ↔ DataNode | gRPC (HTTP/2) | Protobuf, streaming | Cliente | Stream cliente→servidor (escritura) / servidor→cliente (lectura) | 50051 | Token de bloque HMAC-SHA256 | TLS 1.3 + token |
| ③ | DataNode → ControlNode | gRPC | Protobuf | DataNode | Petición/respuesta periódica | 9090 | Confianza de red privada | mTLS con CA propia |
| ④ | ControlNode → DataNode | gRPC (piggyback en ③) | Protobuf | ControlNode (responde) | Comandos asíncronos | 9090 | Idem ③ | Idem ③ |
| ⑤ | DataNode ↔ DataNode | gRPC | Protobuf, streaming | DataNode | Stream en cadena | 50051 | Token de bloque | mTLS + token |
| ⑥ | ControlNode ↔ ControlNode | Raft (Apache Ratis sobre gRPC) | Protobuf | Líder/candidato | Log replicado | 9500 | — (Hito 3) | mTLS |

### Decisiones y alternativas

| Decisión | Por qué | Alternativa descartada |
|---|---|---|
| REST/JSON para el control | Se prueba con `curl`, se documenta con OpenAPI y cualquier lenguaje lo consume. Los mensajes son pequeños (KB) | gRPC también para el cliente: más eficiente pero más difícil de inspeccionar y de usar desde un navegador |
| gRPC con streaming para los datos | HTTP/2 permite enviar un bloque de 4 MiB en *chunks* de 1 MiB sin cargarlo entero en memoria del servidor, y el mismo `.proto` genera código Java y Python | REST con `multipart`: sin streaming bidireccional real y con más sobrecarga de codificación |
| Comandos del ControlNode dentro de la respuesta del heartbeat | El ControlNode no necesita abrir conexiones hacia los DataNodes (que pueden estar tras NAT) ni mantener sesiones abiertas. Es el modelo de HDFS | Conexión persistente o cola de mensajes por nodo |
| Sin MOM (Kafka/RabbitMQ) | La única comunicación asíncrona real son los comandos, y el heartbeat ya da un canal periódico. Un broker agregaría un componente más que también habría que hacer tolerante a fallos | Kafka para eventos de bloque |
| El cliente calcula el SHA-256 antes de enviar | Permite que cada DataNode del pipeline verifique el bloque al recibirlo y rechace datos corruptos en el momento | Verificar sólo en el cliente al leer |

## 4. ① Cliente ↔ ControlNode (REST, puerto 8080)

Base: `http://<controlnode>:8080/api/v1` · `Authorization: Bearer <JWT>` · errores como
`{"error":"CODIGO","message":"..."}`.

| Método | Ruta | RF | Qué hace |
|---|---|---|---|
| POST | `/auth/register` | RF4 | Crea usuario y su `/home/<usuario>` con permisos `rwx------` |
| POST | `/auth/login` | RF4 | Devuelve `{token, expiresIn, username, role}` |
| GET | `/fs/list?path=` | RF1 | `ls` |
| GET | `/fs/stat?path=` | RF1 | Metadatos de una ruta |
| POST | `/fs/mkdir` | RF1 | `mkdir [-p]` |
| DELETE | `/fs/delete?path=&recursive=` | RF1 | `rm` / `rmdir` |
| POST | `/fs/move` | RF1 | `mv` (sólo metadatos: los bloques no se mueven) |
| POST | `/files/create` | RF2/RF3 | Abre para escritura: crea versión borrador y otorga el **lease** |
| POST | `/versions/{id}/blocks` | RF2/RF3 | Asigna un bloque: devuelve `blockId`, **pipeline** de 3 DataNodes y **token** |
| POST | `/versions/{id}/commit` | RF2/RF3 | Publica la versión (atómico) si hay quórum de réplicas |
| POST | `/versions/{id}/abort` | RF2/RF3 | Descarta el borrador y libera los bloques |
| POST | `/files/open` | RF2/RF3 | Devuelve bloques, réplicas vivas ordenadas por carga y tokens de lectura |
| GET | `/cluster/status` | RF5 | Estado del clúster y bloques sub-replicados |
| GET | `/health` | RF5 | Salud del nodo (público) |

Ejemplo de asignación de bloque:

```http
POST /api/v1/versions/0c52.../blocks HTTP/1.1
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9...
Content-Type: application/json

{"index": 0}
```

```json
{
  "blockId": "3f0a9c12-7c1e-4f0a-9e2b-5d9c6b1a2f33",
  "index": 0,
  "token": "M2YwYTljMTIt...fA.qZ8k1...",
  "pipeline": [
    {"nodeId": "dn1", "address": "dn1:50051", "zone": "zona-a"},
    {"nodeId": "dn3", "address": "dn3:50051", "zone": "zona-b"},
    {"nodeId": "dn4", "address": "dn4:50051", "zone": "zona-c"}
  ]
}
```

## 5. ② Cliente ↔ DataNode (gRPC, puerto 50051)

```protobuf
service DataTransfer {
  rpc WriteBlock (stream WriteBlockRequest) returns (WriteBlockResponse);
  rpc ReadBlock  (ReadBlockRequest)         returns (stream BlockChunk);
  rpc NodeStatus (NodeStatusRequest)        returns (NodeStatusResponse);
}
```

**Escritura.** El stream es: un `BlockHeader` y después `chunk`s de hasta 1 MiB.

| Campo del header | Contenido |
|---|---|
| `block_id` | Identificador asignado por el ControlNode |
| `block_token` | Token HMAC que autoriza modo `w` sobre ese bloque |
| `pipeline` | DataNodes **siguientes** (`["dn3:50051","dn4:50051"]`): cada nodo reenvía al primero de su lista y le pasa el resto |
| `size`, `sha256` | Tamaño y checksum esperados; el DataNode aborta si no coinciden |

La respuesta trae `stored_on`, la lista de nodos que confirmaron la escritura; con ella el
cliente sabe si alcanzó el quórum antes de confirmar la versión.

**Lectura.** `ReadBlock(block_id, block_token, offset, length)` devuelve un stream de
`BlockChunk`. Con `offset`/`length` el cliente lee sólo el rango que necesita, lo que
hace posible el acceso aleatorio de RF3 sin bajar el archivo completo.

## 6. ③④ DataNode ↔ ControlNode (gRPC, puerto 9090)

```protobuf
service NodeControl {
  rpc Register      (RegisterRequest)      returns (RegisterResponse);
  rpc Heartbeat     (HeartbeatRequest)     returns (HeartbeatResponse);   // trae comandos
  rpc BlockReport   (BlockReportRequest)   returns (Ack);
  rpc BlockReceived (BlockReceivedRequest) returns (Ack);
  rpc ReportCorrupt (CorruptBlockRequest)  returns (Ack);
}
```

| Llamada | Cuándo | Efecto en el ControlNode |
|---|---|---|
| `Register` | Al arrancar el DataNode y si un heartbeat responde `registered=false` | Lo agrega al clúster con su dirección anunciada, zona y capacidad; olvida las réplicas que le atribuía |
| `Heartbeat` | Cada 3 s | Actualiza carga y espacio libre, marca el nodo vivo y **devuelve comandos** |
| `BlockReport` | Al registrarse y cada 5 min (completo) | Reconstruye el mapa bloque→réplicas (estado blando) |
| `BlockReceived` | Al terminar de escribir un bloque, **antes** de responder al escritor | Registra la réplica; evita carreras en el `commit` |
| `ReportCorrupt` | Checksum inválido detectado por un DataNode o un cliente | Quita esa réplica del mapa y ordena borrarla; el bloque entra en la cola de re-replicación |

Comandos que viajan en `HeartbeatResponse` (máximo 10 por heartbeat):

| Comando | Campos | Significado |
|---|---|---|
| `ReplicateCommand` | `block_id`, `target_address`, `block_token`, `sha256`, `size` | "Copia este bloque al DataNode indicado" (se convierte en una llamada ⑤) |
| `DeleteCommand` | `block_ids[]` | "Borra estos bloques" (archivo eliminado, versión vieja o réplica corrupta) |

### Parámetros de tiempo

| Parámetro | Valor | Variable de entorno |
|---|---|---|
| Intervalo de heartbeat | 3 s | `DFSHA_HEARTBEAT_S` (DataNode) / `DFSHA_HEARTBEAT_MS` (ControlNode) |
| DataNode declarado muerto | 15 s sin heartbeat | `DFSHA_NODE_DEAD_MS` |
| Reporte completo de bloques | 5 min | `DFSHA_FULL_REPORT_S` |
| Ciclo del vigilante de replicación | 2 s | — |
| Timeout de una re-replicación | 60 s | — |
| Re-replicaciones simultáneas por DataNode | 2 | — |
| Vigencia del token de bloque | 600 s | `DFSHA_BLOCK_TOKEN_TTL` |
| Vigencia del lease de escritura | 120 s | `DFSHA_LEASE_TTL` |
| Vigencia del JWT | 60 min | `DFSHA_JWT_TTL_MIN` |

## 7. ⑤ DataNode ↔ DataNode (gRPC, puerto 50051)

Se reutiliza `WriteBlock`, en dos situaciones:

1. **Pipeline de escritura.** El cliente envía a `dn1` con `pipeline=[dn3,dn4]`; `dn1`
   escribe en disco y a la vez reenvía el stream a `dn3` con `pipeline=[dn4]`, y `dn3`
   a `dn4` con `pipeline=[]`. El cliente sube los datos **una sola vez** y aun así
   quedan 3 copias. Los `ack` vuelven en cadena.
2. **Re-replicación.** Cuando el ControlNode detecta un bloque con menos de 3 réplicas,
   ordena (comando ④) al DataNode que lo tiene copiarlo al destino elegido, con
   `pipeline=[]`. Es tráfico exclusivamente entre pares: el ControlNode sólo decide.

```mermaid
sequenceDiagram
    participant C as Cliente
    participant D1 as dn1
    participant D3 as dn3
    participant D4 as dn4
    C->>D1: WriteBlock header(pipeline=[dn3,dn4]) + chunks
    D1->>D3: WriteBlock header(pipeline=[dn4]) + chunks
    D3->>D4: WriteBlock header(pipeline=[]) + chunks
    D4-->>D3: ok (stored_on=[dn4])
    D3-->>D1: ok (stored_on=[dn3,dn4])
    D1-->>C: ok (stored_on=[dn1,dn3,dn4])
```

## 8. ⑥ ControlNode ↔ ControlNode (Raft, Hito 3)

La interfaz `MetadataStore` del ControlNode ya encapsula todas las mutaciones en
`write(...)` y las consultas en `read(...)`. En el Hito 3 la implementación con Apache
Ratis convertirá cada `write` en una entrada del log de Raft replicada por mayoría. Las
operaciones que serán entradas del log son:

`CreateUser · Mkdir · Delete · Move · CreateVersion · AllocateBlock · CommitVersion ·
AbortVersion · AcquireLease · ReleaseLease`

Las **ubicaciones de los bloques no se replican**: son estado blando que cualquier
ControlNode reconstruye con los `BlockReport`, así que el nuevo líder puede atender de
inmediato tras un failover. Por eso los DataNodes enviarán heartbeats a los tres
ControlNodes.

## 9. Flujos completos

### 9.1 Escritura (`dfsha put archivo.bin /home/ana/archivo.bin`)

```mermaid
sequenceDiagram
    autonumber
    participant C as Cliente
    participant CN as ControlNode
    participant D as DataNodes (pipeline)
    C->>CN: POST /files/create {path}
    CN-->>C: versionId, leaseId, blockSize=4 MiB
    loop por cada bloque (4 hilos en paralelo)
        C->>CN: POST /versions/{id}/blocks {index}
        CN-->>C: blockId, pipeline[3], token
        C->>C: SHA-256 del bloque
        C->>D: WriteBlock(stream) → replicación en cadena
        D-->>CN: BlockReceived (cada réplica)
        D-->>C: ok, stored_on[]
    end
    C->>CN: POST /versions/{id}/commit {blocks[]}
    CN->>CN: verifica ≥2 réplicas por bloque · publica la versión · libera el lease
    CN-->>C: 200 {size, blockCount}
```

Reintentos: si un bloque falla (DataNode caído, checksum inválido, timeout), el cliente
pide un **pipeline nuevo** para ese bloque y reintenta hasta 3 veces; si aun así falla,
llama a `abort` y no queda ninguna versión a medias.

### 9.2 Lectura con failover (`dfsha get`)

```mermaid
sequenceDiagram
    autonumber
    participant C as Cliente
    participant CN as ControlNode
    participant DA as réplica A
    participant DB as réplica B
    C->>CN: POST /files/open {path}
    CN-->>C: bloques + réplicas (ordenadas por carga) + tokens
    par bloques en paralelo (4 hilos)
        C->>DA: ReadBlock(blockId, token)
        DA-->>C: stream de chunks
        C->>C: verifica SHA-256
    and
        C->>DA: ReadBlock(otro bloque)
        DA--xC: nodo caído / checksum inválido
        C->>DB: ReadBlock (siguiente réplica)
        DB-->>C: stream de chunks
    end
    C->>C: reensambla el archivo por índice de bloque
```

### 9.3 Caída de un DataNode

```mermaid
sequenceDiagram
    autonumber
    participant CN as ControlNode
    participant D2 as dn2 (vivo)
    participant D4 as dn4 (destino)
    Note over CN: dn1 deja de enviar heartbeats
    CN->>CN: a los 15 s marca dn1 MUERTO y olvida sus réplicas
    CN->>CN: encola los bloques con <3 réplicas (primero los que tienen 1)
    D2->>CN: Heartbeat
    CN-->>D2: ReplicateCommand{block, target=dn4, token}
    D2->>D4: WriteBlock(stream) — copia P2P
    D4->>CN: BlockReceived
    CN->>CN: el bloque vuelve a 3 réplicas
```

## 10. Límites y códigos de error

| Parámetro de red | Valor |
|---|---|
| Tamaño de bloque | 4 MiB (`DFSHA_BLOCK_SIZE`) |
| Chunk del stream gRPC | 1 MiB (`DFSHA_CHUNK_SIZE`) |
| Máximo mensaje gRPC | 8 MiB (envío y recepción) |
| Bloques en paralelo por cliente | 4 (`--threads`, `DFSHA_THREADS`) |
| Timeout de una llamada gRPC de bloque | 180 s |
| Timeout de una llamada REST | 30 s |
| Reintentos por bloque | 3, con pipeline nuevo en cada intento |

| Código | HTTP / gRPC | Cuándo aparece |
|---|---|---|
| `UNAUTHORIZED` | 401 | Falta el JWT, es inválido o expiró |
| `FORBIDDEN` | 403 | La ACL no permite la operación |
| `NOT_FOUND` | 404 | Ruta o versión inexistente |
| `ALREADY_EXISTS` | 409 | `mkdir`/`put` sin `overwrite` |
| `NOT_EMPTY` | 409 | `rm` de un directorio con contenido sin `-r` |
| `LOCKED` | 409 | Otro cliente tiene el lease de escritura |
| `LEASE_EXPIRED` | 409 | El lease venció durante la escritura |
| `NOT_A_DRAFT` | 409 | La versión ya fue confirmada o abortada |
| `NOT_ENOUGH_REPLICAS` | 409 | El `commit` no alcanza el quórum de 2 réplicas |
| `NO_DATANODES` | 503 | No hay DataNodes vivos con espacio |
| `NO_REPLICA_AVAILABLE` | 503 | Ningún DataNode vivo tiene el bloque |
| `PERMISSION_DENIED` | gRPC | Token de bloque ausente, mal firmado, de otro bloque, de otro modo o expirado |
| `DATA_LOSS` | gRPC | El SHA-256 recibido no coincide: el DataNode descarta el bloque |
| `NOT_FOUND` | gRPC | El DataNode no tiene ese bloque |

## 11. Seguridad de las comunicaciones

**Hoy (Hito 2).** Contraseñas con bcrypt (coste 12); sesión con JWT HS256 firmado por el
ControlNode; ACL tipo POSIX por usuario y grupo en cada operación de namespace; y
**tokens de bloque** HMAC-SHA256 que llevan `blockId|usuario|modo|expiración`, de modo
que el control de acceso también se aplica en el plano de datos sin que los DataNodes
consulten la base de usuarios. El tráfico va en claro dentro de la red privada.

**Hito 3.** TLS 1.3 en REST y gRPC del cliente; mTLS con CA propia entre nodos (un nodo
sin certificado válido no puede registrarse ni recibir bloques); 2FA TOTP y API keys; y
cifrado AES-256-GCM de los bloques del lado del cliente, con clave por archivo protegida
por la clave maestra del clúster.

## 12. Cómo verificar cada comunicación

```bash
# ① REST: salud, login y estado del clúster
curl -s localhost:8080/api/v1/health
TOKEN=$(curl -s -X POST localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' -d '{"username":"ana","password":"secreto123"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
curl -s localhost:8080/api/v1/cluster/status -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# ②⑤ gRPC de datos: subir y bajar, y ver en qué DataNodes quedó cada bloque
python3 -m dfsha put ./archivo.bin /home/ana/archivo.bin
python3 -m dfsha stat /home/ana/archivo.bin

# ③④ control: ver registro, heartbeats y comandos en el log del ControlNode
docker compose logs -f cn1 | grep -E "registrado|MUERTO|Re-replicando"

# Suite completa (levanta el clúster, prueba y lo apaga)
python3 tests/e2e_test.py
```
