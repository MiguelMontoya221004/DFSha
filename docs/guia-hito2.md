# DFSha — Guía del Hito 2: qué hay en el repositorio y a qué corresponde

ST0263 / SI3007 · 2026-2 · Semanas 2-3 · 64 archivos · 6.211 líneas

El cronograma pide para este hito: **«Diseño e implementación de arquitectura distribuida para
opción 1 / opción 2, especificación de comunicaciones»**. Este documento explica, archivo por
archivo, qué hace cada pieza y a cuál de esas tres cosas responde.

## 1. Los tres entregables del hito

| Lo que pide el enunciado | Con qué se responde |
|---|---|
| **Diseño** de la arquitectura distribuida (opción 1: C/S con distribución del servicio) | Arquitectura híbrida en dos planos: **plano de control** cliente/servidor (ControlNode: metadatos, permisos, decisiones de colocación) y **plano de datos** entre pares (los bytes van cliente↔DataNode y DataNode↔DataNode; el ControlNode nunca los transporta). Documentada en el README y en `docs/comunicaciones.md` |
| **Implementación** de esa arquitectura | Tres programas que corren y se comunican: ControlNode (Java 21, Spring Boot + gRPC), DataNode (Python + gRPC) y cliente (Python, CLI + SDK), más Docker Compose con 1 ControlNode + 4 DataNodes en 3 zonas y una suite end-to-end de 23 pruebas |
| **Especificación de comunicaciones** | `docs/comunicaciones.md` (matriz de las 5 interacciones, mensajes, tiempos, reintentos, códigos de error y flujos), `proto/dfsha.proto` (contrato gRPC) y `docs/openapi.yaml` (contrato REST) |

Las cinco comunicaciones, tal como se numeran en `docs/comunicaciones.md`:

| # | Comunicación | Protocolo | Puerto |
|---|---|---|---|
| ① | Cliente → ControlNode | REST/JSON + JWT | 8080 |
| ② | Cliente ↔ DataNode | gRPC streaming + token de bloque | 50051 |
| ③ | DataNode → ControlNode | gRPC (registro, heartbeats, reportes) | 9090 |
| ④ | ControlNode → DataNode | comandos dentro de la respuesta del heartbeat | 9090 |
| ⑤ | DataNode ↔ DataNode | gRPC streaming (pipeline y re-replicación) | 50051 |

## 2. Contratos — la fuente de verdad

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `proto/dfsha.proto` | 156 | Define los servicios gRPC `DataTransfer` (WriteBlock, ReadBlock, NodeStatus) y `NodeControl` (Register, Heartbeat, BlockReport, BlockReceived, ReportCorrupt) y todos sus mensajes, incluidos los comandos Replicate y Delete | Especificación de comunicaciones ②③④⑤ |
| `docs/openapi.yaml` | 320 | Las 14 rutas REST del ControlNode en OpenAPI 3, con ejemplos y códigos de error | Especificación de comunicaciones ① |

Del `.proto` se generan las clases de Java (Maven, al compilar) y los stubs de Python
(`scripts/gen_proto.sh`), así que los dos lenguajes no pueden desincronizarse.

## 3. ControlNode — Java 21 + Spring Boot + gRPC (28 archivos)

### Raíz del módulo

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `controlnode/pom.xml` | 123 | Dependencias y el plugin que genera las clases Java desde el `.proto` | Infraestructura |
| `controlnode/Dockerfile` | 18 | Imagen en dos etapas (Maven → JRE 21) con healthcheck sobre `/api/v1/health` | Entorno Docker |
| `src/main/resources/application.yml` | 35 | Configuración: bloque de 4 MiB, replicación 3, quórum 2, tiempos de heartbeat, secretos | Escalabilidad, seguridad |
| `ControlNodeApplication.java` | 23 | Arranque de Spring Boot; activa las tareas programadas (vigilante cada 2 s) | Arquitectura |
| `config/ControlNodeProperties.java` | 23 | Tipa los parámetros de configuración | Infraestructura |

### `model/` — modelo de datos del sistema de archivos (6 clases, 177 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `INode.java` | 45 | Directorio o archivo: dueño, grupo, permisos `rwxr-x---`, hijos, versión actual, historial, lease | RF1 namespace, ACL |
| `FileVersion.java` | 34 | Versión inmutable con estado DRAFT/COMMITTED/ABORTED y el mapa índice→bloque; habilita la publicación atómica | RNF3 consistencia |
| `BlockMeta.java` | 20 | Bloque: id, índice, tamaño, SHA-256. No guarda réplicas: son estado blando reconstruido con los reportes | RNF4 particionamiento |
| `DataNodeInfo.java` | 36 | Estado de un DataNode y `loadScore()` (60 % disco + 40 % transferencias) usado al colocar réplicas | Escalabilidad |
| `Lease.java` | 20 | Lock de escritura con expiración (120 s): un solo escritor por archivo | RNF3 consistencia |
| `User.java` | 22 | Usuario con hash bcrypt, rol y grupo | RF4 |

### `metadata/` — el punto donde entrará Raft (2 archivos, 129 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `MetadataStore.java` | 44 | Interfaz del almacén: toda mutación pasa por `write(...)`, toda consulta por `read(...)`. **Punto de extensión del Hito 3**: la implementación con Apache Ratis convertirá cada `write` en una entrada del log de Raft | Arquitectura, prepara RNF2 |
| `InMemoryMetadataStore.java` | 85 | Implementación de este hito: árbol en memoria con `ReadWriteLock` (orden total) y normalización de rutas | RNF3 consistencia |

### `service/` — la lógica del servicio (4 clases, 854 líneas: el núcleo del hito)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `BlockManager.java` | 295 | Registro y vigilancia de DataNodes, mapa bloque→réplicas, **algoritmo de colocación** (power of two choices con zonas distintas), detección de nodos muertos y **re-replicación P2P** por comandos | RNF1 escalabilidad, RNF4, RNF2 |
| `FileService.java` | 253 | Ciclo de vida: `create` (borrador + lease) → `allocateBlock` (pipeline + token) → `commit` (quórum 2 de 3 y publicación atómica) / `abort`; `open` para lectura; recolección de versiones viejas | RF2, RF3, RNF3 |
| `NamespaceService.java` | 232 | `ls`, `stat`, `mkdir -p`, `rm -r`, `mv` y comprobación de ACL; creación de `/home/<usuario>` y `/shared` | RF1, RNF6 |
| `UserService.java` | 74 | Registro y login (bcrypt coste 12, emisión de JWT, usuario admin inicial) | RF4 |

### `security/` — autenticación y autorización (4 archivos, 176 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `BlockTokenService.java` | 70 | Emite tokens de bloque `blockId|usuario|modo|expiración` firmados con HMAC-SHA256: llevan el control de acceso al plano de datos | RNF6, comunicaciones ②⑤ |
| `JwtService.java` | 50 | Firma y valida los JWT de sesión (HS256, 1 h) | RF4 |
| `JwtAuthFilter.java` | 50 | Valida el token en cada petición REST; sólo `/auth/*` y `/health` son públicos | RNF6 |
| `Principal.java` | 6 | Identidad autenticada (usuario, rol) | Soporte |

### `api/` — la cara REST, comunicación ① (7 archivos, 313 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `FilesController.java` | 65 | `/files/create`, `/versions/{id}/blocks`, `/commit`, `/abort`, `/files/open`. Ninguna ruta transporta bytes | Comunicaciones ①, RF2, RF3 |
| `FsController.java` | 59 | `/fs/list`, `/stat`, `/mkdir`, `/delete`, `/move` | Comunicaciones ①, RF1 |
| `ClusterController.java` | 62 | `/cluster/status` y `/health`: nodos vivos, capacidad, carga y bloques sub-replicados | RF5 monitoreo |
| `AuthController.java` | 34 | Registro e inicio de sesión | RF4 |
| `DfshaException.java` | 39 | Códigos de error estables (`LOCKED`, `NOT_ENOUGH_REPLICAS`, `NO_REPLICA_AVAILABLE`, …) compartidos con el cliente Python | Comunicaciones · errores |
| `ApiExceptionHandler.java` | 29 | Formato único de error `{"error":..., "message":...}` | Comunicaciones · errores |
| `dto/Dtos.java` | 25 | Cuerpos JSON de las peticiones | Comunicaciones ① |

### `grpc/` — la cara hacia los DataNodes, comunicaciones ③④ (2 archivos, 121 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `NodeControlServiceImpl.java` | 75 | Atiende registro, heartbeats (y **devuelve los comandos** de replicar o borrar), reportes de bloques y avisos de corrupción | Comunicaciones ③④, RNF2 |
| `GrpcServerRunner.java` | 46 | Levanta el servidor gRPC en el puerto 9090 (en el Hito 3 aquí se activa mTLS) | Comunicaciones ③ |

## 4. DataNode — Python + gRPC (11 archivos)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `dfsha_datanode/service.py` | 212 | Servicio `DataTransfer`: `WriteBlock` (verifica token, escribe a temporal, comprueba SHA-256, renombra) con `_PipelineForwarder` que **reenvía el stream al siguiente DataNode**, y `ReadBlock` por rangos en chunks de 1 MiB | Comunicaciones ②⑤, replicación, RNF5 |
| `dfsha_datanode/controlnode.py` | 176 | Registro con reintentos, heartbeat cada 3 s, reporte completo de bloques, aviso `BlockReceived` y ejecución de comandos (incluida la **copia P2P** al re-replicar) | Comunicaciones ③④⑤, RNF2 |
| `dfsha_datanode/storage.py` | 154 | Bloques en disco (`.blk` + `.meta` en dos niveles), escritura atómica por temporal, SHA-256 al pasar, lectura por rangos, inventario y verificación | RNF4, integridad |
| `dfsha_datanode/tokens.py` | 45 | Verifica los tokens de bloque emitidos por el ControlNode (contraparte de `BlockTokenService.java`) | RNF6 |
| `dfsha_datanode/__main__.py` | 59 | Arranque: servidor gRPC, registro y hilo de heartbeats | Infraestructura |
| `dfsha_datanode/config.py` | 33 | Configuración por variables de entorno (id, puerto, dirección anunciada, zona, datos, capacidad) | Infraestructura |
| `dfsha_datanode/pb/*` (3) | 541 | Stubs **generados** del `.proto`; versionados para que el repo corra sin pasos extra | Contrato generado |
| `__init__.py`, `requirements.txt`, `Dockerfile` | 21 | Paquete, dependencias e imagen (regenera los stubs en el build) | Entorno Docker |

## 5. Cliente — CLI y SDK en Python (12 archivos)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `dfsha/client.py` | 362 | El SDK, donde se ve el diseño de dos planos: namespace por REST y bloques por gRPC en 4 hilos. SHA-256 por bloque, reintentos con pipeline nuevo, **failover** entre réplicas, `DfshaFile` con `seek`/`read` y `write_at` con **copy-on-write** | RF2, RF3, RNF5, RNF2 |
| `dfsha/cli.py` | 309 | Comandos `register`, `login`, `ls -l`, `cd`, `pwd`, `mkdir -p`, `rm -r`, `mv`, `stat`, `put`, `get`, `read`, `write`, `status` y `shell`. `stat` muestra las réplicas y zonas de cada bloque | RF1, RF2, RF3, RF5, RNF7 |
| `dfsha/session.py` | 51 | Sesión local (ControlNode, token, directorio de trabajo) y resolución de rutas relativas | RNF7 transparencia |
| `dfsha/pb/*` (3) | 541 | Stubs generados del `.proto` | Contrato generado |
| `__init__.py`, `__main__.py`, `requirements.txt`, `Dockerfile` | 26 | Empaquetado (`python -m dfsha`) e imagen con el comando `dfsha` | Entorno Docker |

## 6. Despliegue y scripts (6 archivos, 195 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `deploy/docker-compose.yml` | 122 | 1 ControlNode + **4 DataNodes en 3 zonas** + cliente, cada DataNode con su volumen, secretos compartidos y puertos publicados | Entorno Docker, RNF4 |
| `scripts/run-local.sh` / `stop-local.sh` | 53 | Levantar y apagar el mismo clúster como procesos nativos, sin Docker | Entorno nativo |
| `scripts/gen_proto.sh` | 18 | Regenera los stubs de Python desde el `.proto` | Contrato |
| `deploy/datos/README.txt` | 1 | Carpeta montada en el contenedor cliente para archivos de prueba | Soporte |

## 7. Pruebas y documentación (6 archivos, 1.039 líneas)

| Archivo | Líneas | Qué hace | Corresponde a |
|---|---|---|---|
| `tests/e2e_test.py` | 308 | Levanta el clúster, ejecuta **23 pruebas** (auth, namespace, particionamiento, réplicas, integridad, acceso aleatorio, copy-on-write, ACL, tokens, lease, quórum, **caída de un DataNode con failover y re-replicación**, borrado) y lo apaga | Evidencia de todo el hito |
| `docs/comunicaciones.md` | 360 | **Entregable central de la especificación de comunicaciones**: matriz de las 5 interacciones, decisiones y alternativas, detalle de cada servicio, tiempos, límites, reintentos, códigos de error y flujos en diagramas de secuencia | Comunicaciones (15 %) |
| `docs/pruebas-hito2.md` | 118 | Qué prueba cada caso, salida de la última ejecución, cifras de rendimiento y pruebas manuales para el video | Pruebas e informe |
| `README.md` | 211 | Definición del servicio, problemática, arquitectura, RF/RNF, estado por hito, estructura, cómo ejecutar (Docker y nativo), variables de entorno y cronograma | Diseño (15 %), repo documentado |
| `docs/guia-hito2.md` | este | Guía archivo por archivo (este documento) | Documentación |
| `.gitignore` | 22 | Excluye `target/`, `__pycache__`, logs, sesiones y datos de prueba | Soporte |

## 8. Recorrido de un `put`: qué archivo hace qué

Útil como guion del video: seguir un archivo de 12 MiB desde el comando hasta las tres copias.

1. **`dfsha put prueba.bin /home/ana/prueba.bin`** — `client/dfsha/cli.py` resuelve la ruta con la sesión y llama al SDK.
2. **Abrir para escritura** — `client.py` → `POST /files/create` → `api/FilesController.java` → `service/FileService.java` crea la versión borrador y otorga el lease.
3. **Pedir destino de cada bloque** — `POST /versions/{id}/blocks` → `FileService` pide a `service/BlockManager.java` tres DataNodes en zonas distintas; `security/BlockTokenService.java` firma el token.
4. **Enviar los bytes** — `client.py` calcula el SHA-256 y abre un stream `WriteBlock` contra `datanode/service.py`, que escribe con `storage.py` y valida el token con `tokens.py`.
5. **Replicación en cadena** — `_PipelineForwarder` de `datanode/service.py` reenvía el mismo stream al segundo DataNode y ese al tercero: el cliente sube los datos una sola vez.
6. **Aviso de réplica** — `datanode/controlnode.py` llama `BlockReceived` → `grpc/NodeControlServiceImpl.java` → `BlockManager` actualiza el mapa, *antes* de responder al cliente.
7. **Commit** — `POST /versions/{id}/commit` → `FileService` verifica ≥2 réplicas por bloque, publica la versión atómicamente y libera el lease. Recién ahí el archivo aparece en `ls`.
8. **Si un DataNode se cae** — el vigilante de `BlockManager` lo detecta a los 15 s, emite `ReplicateCommand` y `datanode/controlnode.py` copia el bloque a otro DataNode: vuelve a haber tres copias sin intervención del cliente.

## 9. Advertencia sobre Docker

Todo lo anterior se probó con los cinco procesos nativos corriendo y comunicándose. Las imágenes
Docker **no se han construido aún** (el entorno de desarrollo usado no tenía demonio de Docker):
el `docker-compose.yml` está validado con `docker compose config`, pero la primera ejecución real
de `docker compose up -d --build` conviene hacerla con tiempo.

## 10. Qué queda para el Hito 3 y dónde se conecta

| Pendiente | Dónde se conecta |
|---|---|
| 3 ControlNodes con Raft (Apache Ratis) | Nueva implementación de `metadata/MetadataStore.java`; los DataNodes enviarán heartbeats a los tres y el cliente reintentará contra la lista de ControlNodes |
| TLS en el cliente y mTLS entre nodos | `grpc/GrpcServerRunner.java`, el servidor gRPC de `datanode/__main__.py`, los canales de `client.py` y un script de CA y certificados |
| 2FA TOTP y API keys | `service/UserService.java` y `api/AuthController.java` |
| Cifrado AES-256-GCM en reposo | Del lado del cliente en `client.py`, con la clave por archivo entregada por el ControlNode al abrir |
| Mediciones de rendimiento (RNF5) | Hito 4: despliegue en 3 EC2 de AWS Academy |
