# DFSha — Especificación de comunicaciones (Hito 2)

ST0263 / SI3007 · 2026-2 · versión 0.3.0

Este documento define **cómo se hablan los componentes de DFSha**: protocolos, mensajes,
puertos, tiempos, reintentos y errores. Es el contrato que implementan el ControlNode
(Java), el DataNode (Python) y el Cliente (Python), y la referencia para cualquiera que
quiera escribir otro cliente.

Fuentes de verdad del contrato:

| Contrato | Archivo |
|---|---|
| HTTP del DataNode (bloques) | [`docs/api-datanode.md`](api-datanode.md) |
| REST del ControlNode (metadatos) | [`docs/openapi.yaml`](openapi.yaml) |
| gRPC del plano de control (ControlNode ↔ DataNode) | [`proto/dfsha.proto`](../proto/dfsha.proto) |

> **Cambio respecto a la primera versión (v0.3.0).** El plano de datos pasó de gRPC a
> **HTTP**: el cliente sube y baja bloques con `PUT`/`GET` y los DataNodes se replican
> entre ellos igual. El motivo es el cliente: solo depende de `requests`, sin stubs
> generados ni `grpcio`, lo que simplifica escribirlo y explicarlo. El plano de control
> (ControlNode ↔ DataNode) sigue en gRPC, donde el contrato tipado sí aporta.

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

Todo lo de la columna izquierda está cubierto por la suite `tests/e2e_datanode.py` (19 pruebas).

## 2. Vista general

```mermaid
flowchart LR
    C["Cliente (Python)<br/>CLI · SDK"]
    CN["ControlNode (Java)<br/>REST :8080 · gRPC :9090"]
    subgraph DN["DataNodes (Python) · HTTP :50051"]
        D1[dn1 zona-a]
        D2[dn2 zona-a]
        D3[dn3 zona-b]
        D4[dn4 zona-c]
    end
    C -->|"① REST/JSON + JWT<br/>metadatos, asignación de bloques"| CN
    C ==>|"② HTTP PUT/GET + token de bloque<br/>bytes de los bloques"| DN
    DN -->|"③ gRPC: Register · Heartbeat · BlockReport"| CN
    CN -.->|"④ comandos en la respuesta del heartbeat<br/>Replicate · Delete"| DN
    D1 ==>|"⑤ HTTP PUT: pipeline y re-replicación"| D3
```

Principio de diseño: **el ControlNode nunca transporta datos**. Sólo entrega metadatos,
decide dónde van los bloques y firma tokens; los bytes viajan cliente↔DataNode y
DataNode↔DataNode. De ahí salen la escalabilidad (RNF1) y el rendimiento (RNF5).

## 3. Matriz de comunicaciones

| # | Comunicación | Protocolo | Formato | Quién inicia | Patrón | Puerto | Seguridad (Hito 2) | Seguridad (Hito 3) |
|---|---|---|---|---|---|---|---|---|
| ① | Cliente → ControlNode | HTTP/1.1 REST | JSON | Cliente | Petición/respuesta | 8080 | JWT (HS256) | HTTPS/TLS 1.3 + JWT, API keys |
| ② | Cliente ↔ DataNode | HTTP/1.1 (`PUT`/`GET` de bloques) | Bytes crudos, por trozos | Cliente | Petición/respuesta con cuerpo en streaming | 50051 | Token de bloque HMAC-SHA256 | TLS 1.3 + token |
| ③ | DataNode → ControlNode | gRPC | Protobuf | DataNode | Petición/respuesta periódica | 9090 | Confianza de red privada | mTLS con CA propia |
| ④ | ControlNode → DataNode | gRPC (piggyback en ③) | Protobuf | ControlNode (responde) | Comandos asíncronos | 9090 | Idem ③ | Idem ③ |
| ⑤ | DataNode ↔ DataNode | HTTP/1.1 (`PUT` del bloque) | Bytes crudos, por trozos | DataNode | Reenvío en cadena | 50051 | Token de bloque | mTLS + token |
| ⑥ | ControlNode ↔ ControlNode | Raft (Apache Ratis sobre gRPC) | Protobuf | Líder/candidato | Log replicado | 9500 | — (Hito 3) | mTLS |

### Decisiones y alternativas

| Decisión | Por qué | Alternativa descartada |
|---|---|---|
| REST/JSON para el control | Se prueba con `curl`, se documenta con OpenAPI y cualquier lenguaje lo consume. Los mensajes son pequeños (KB) | gRPC también para el cliente: más eficiente pero más difícil de inspeccionar y de usar desde un navegador |
| HTTP para los datos | El cuerpo de un `PUT` ya es un stream de bytes: se envía un bloque de 4 MiB en trozos de 1 MiB sin cargarlo entero en memoria, y el cliente se escribe con `requests` y nada más. Además se inspecciona con `curl` y soporta `Range` para la lectura aleatoria | gRPC streaming: más eficiente en el formato, pero obliga al cliente a generar stubs y depender de `grpcio` para transportar bytes que no necesitan tipado |
| gRPC solo en el plano de control | Ahí sí hay mensajes con estructura (heartbeats, reportes, comandos) y un contrato tipado evita que Java y Python se desincronicen | REST también para los heartbeats: más código de serialización a mano en los dos lados |
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

## 5. ② Cliente ↔ DataNode (HTTP, puerto 50051)

El DataNode expone una API mínima, solo de bloques. El contrato completo, con ejemplos en
`requests`, está en [`docs/api-datanode.md`](api-datanode.md).

| Método y ruta | Para qué |
|---|---|
| `PUT /blocks/{blockId}` | Sube un bloque y lo reenvía al resto del pipeline |
| `GET /blocks/{blockId}` | Lo descarga, entero o por rango (`?offset=&length=` o cabecera `Range`) |
| `GET /blocks/{blockId}/meta` | Tamaño y checksum de esa réplica |
| `DELETE /blocks/{blockId}` | Lo borra (requiere token de escritura) |
| `GET /status` · `GET /health` | Estado del nodo y sonda de vida |

**Escritura.** El cuerpo del `PUT` son los bytes del bloque; el resto viaja en cabeceras:

| Cabecera | Contenido |
|---|---|
| `X-Block-Token` | Token HMAC que autoriza modo `w` sobre ese bloque (también vale `?token=`) |
| `X-Block-Sha256` | Checksum esperado: el DataNode lo verifica y descarta el bloque si no coincide (HTTP 422) |
| `X-Pipeline` | DataNodes **siguientes**, separados por coma (`dn3:50051,dn4:50051`): cada nodo reenvía al primero de su lista y le pasa el resto |

La respuesta trae `storedOn`, la lista de nodos que confirmaron la escritura; con ella el
cliente sabe si alcanzó el quórum antes de confirmar la versión.

**Lectura.** `GET /blocks/{blockId}` devuelve los bytes en streaming, con `X-Block-Sha256`
y `X-Block-Size` en las cabeceras. Con `offset`/`length` —o con `Range`, que responde
HTTP 206— el cliente baja solo el pedazo que necesita, que es lo que hace posible el
acceso aleatorio de RF3 sin descargar el archivo completo.

**Errores.** `403` token ausente, mal firmado, de otro bloque, de otro modo o expirado ·
`404` el bloque no está en ese nodo · `416` rango fuera del bloque · `422` el checksum no
coincide.

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

Se reutiliza el mismo `PUT /blocks/{blockId}`, en dos situaciones:

1. **Pipeline de escritura.** El cliente envía a `dn1` con `X-Pipeline: dn3,dn4`; `dn1`
   guarda el bloque y lo reenvía a `dn3` con `X-Pipeline: dn4`, y `dn3` a `dn4` con el
   pipeline vacío. El cliente sube los datos **una sola vez** y aun así quedan 3 copias.
   Cada nodo devuelve en `storedOn` quién confirmó aguas abajo.
2. **Re-replicación.** Cuando el ControlNode detecta un bloque con menos de 3 réplicas,
   ordena (comando ④) al DataNode que lo tiene copiarlo al destino elegido, con el
   pipeline vacío. Es tráfico exclusivamente entre pares: el ControlNode sólo decide.

```mermaid
sequenceDiagram
    participant C as Cliente
    participant D1 as dn1
    participant D3 as dn3
    participant D4 as dn4
    C->>D1: PUT /blocks/{id} · X-Pipeline: dn3,dn4
    D1->>D3: PUT /blocks/{id} · X-Pipeline: dn4
    D3->>D4: PUT /blocks/{id} · X-Pipeline: (vacio)
    D4-->>D3: 200 storedOn=[dn4]
    D3-->>D1: 200 storedOn=[dn3,dn4]
    D1-->>C: 200 storedOn=[dn1,dn3,dn4]
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
        C->>D: PUT /blocks/{id} → replicación en cadena
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
        C->>DA: GET /blocks/{id} + token
        DA-->>C: bytes del bloque
        C->>C: verifica SHA-256
    and
        C->>DA: GET /blocks/{otro}
        DA--xC: nodo caído / checksum inválido
        C->>DB: GET /blocks/{otro} (siguiente réplica)
        DB-->>C: bytes del bloque
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
    D2->>D4: PUT /blocks/{id} — copia P2P
    D4->>CN: BlockReceived
    CN->>CN: el bloque vuelve a 3 réplicas
```

## 10. Límites y códigos de error

| Parámetro de red | Valor |
|---|---|
| Tamaño de bloque | 4 MiB (`DFSHA_BLOCK_SIZE`) |
| Trozo de lectura/escritura | 1 MiB (`DFSHA_CHUNK_SIZE`) |
| Máximo mensaje gRPC (solo plano de control) | 8 MiB |
| Bloques en paralelo por cliente | 4 (`--threads`, `DFSHA_THREADS`) |
| Timeout de una transferencia de bloque | 180 s |
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
| `TOKEN_INVALIDO` | 403 (DataNode) | Token de bloque ausente, mal firmado, de otro bloque, de otro modo o expirado |
| `CHECKSUM_INVALIDO` | 422 (DataNode) | El SHA-256 recibido no coincide: el DataNode descarta el bloque |
| `BLOQUE_NO_ENCONTRADO` | 404 (DataNode) | El DataNode no tiene ese bloque |
| `RANGO_INVALIDO` | 416 (DataNode) | El desplazamiento pedido cae fuera del bloque |

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

# ② HTTP de datos: estado de un DataNode y lectura de un rango de un bloque
curl -s localhost:50051/status | python3 -m json.tool
curl -s "localhost:50051/blocks/<blockId>?token=<token>&offset=0&length=16" | xxd | head -2

# ③④ control: ver registro, heartbeats y comandos en el log del ControlNode
docker compose logs -f cn1 | grep -E "registrado|MUERTO|Re-replicando"

# Suite completa (levanta el clúster, prueba y lo apaga)
python3 tests/e2e_datanode.py
```
