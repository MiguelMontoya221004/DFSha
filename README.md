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

| Comunicación | Protocolo | Seguridad |
|---|---|---|
| Cliente ↔ ControlNode | REST sobre HTTPS (JSON) | TLS 1.3 + JWT / API key |
| Cliente ↔ DataNode | gRPC streaming | TLS 1.3 + token de bloque firmado (HMAC) |
| ControlNode ↔ ControlNode | Raft (Apache Ratis sobre gRPC) | mTLS |
| ControlNode ↔ DataNode | gRPC (registro, heartbeats, comandos) | mTLS |
| DataNode ↔ DataNode | gRPC streaming (replicación) | mTLS |

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

## 5. Entorno de ejecución

- Cada nodo corre en un contenedor **Docker**. Entorno local: `docker compose` con 3 ControlNodes, 4 DataNodes y 1 cliente.
- Despliegue en **AWS Academy**: 3 instancias EC2 (una por zona), cada una con 1 ControlNode y 1–2 DataNodes; el cliente accede por Internet (HTTPS 8443 y gRPC 50051+).

## 6. Documentación
- Contrato gRPC: `proto/dfsha.proto` · API REST: `docs/openapi.yaml`

## 7. Cronograma

| Semana | Hito |
|---|---|
| 1 | **Hito 1** — Especificación definitiva, contratos (`.proto` y OpenAPI), esqueleto del repositorio |
| 2–3 | **Hito 2** — Arquitectura distribuida y comunicaciones: namespace, bloques, replicación en pipeline, lectura paralela, despliegue en AWS |
| 4–5 | **Hito 3** — Alta disponibilidad (Raft), re-replicación, consistencia (versiones y locks) y seguridad (mTLS, ACL, 2FA, cifrado) |
| 6 | **Hito 4** — Pruebas de rendimiento, informe técnico y video |
