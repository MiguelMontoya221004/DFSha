# DFSha — ControlNode

Plano de control de DFSha: 3 ControlNodes replicados con Raft (Apache Ratis)
que guardan el espacio de nombres, los usuarios, los permisos, las versiones de
cada archivo, en que DataNodes esta cada bloque y los locks.

Por aqui **no pasan los bytes de los archivos**: el ControlNode solo dice en que
DataNodes esta cada bloque y entrega un token firmado para poder leerlo o
escribirlo.

Hecho en Java 21 con Spring Boot, Apache Ratis y H2.

## Como correrlo

Solo hace falta **Java 21 o superior**; Maven lo baja el wrapper.

```bash
./mvnw clean package -DskipTests
```

(en CMD de Windows: `mvnw.cmd clean package -DskipTests`)

Y en tres terminales distintas:

```bash
java -jar target/controlnode-1.0.0.jar --dfsha.node-id=n1 --server.port=8443 --dfsha.grpc-port=50051
```

```bash
java -jar target/controlnode-1.0.0.jar --dfsha.node-id=n2 --server.port=8444 --dfsha.grpc-port=50052
```

```bash
java -jar target/controlnode-1.0.0.jar --dfsha.node-id=n3 --server.port=8445 --dfsha.grpc-port=50053
```

Cada nodo guarda su log de Raft y su base de datos H2 en `./data/<node-id>/`.
Para empezar de cero se borra la carpeta `data`.

Para ver que el cluster quedo arriba y quien es el lider:

```bash
curl http://localhost:8443/cluster
```

## API REST (para el cliente)

Todo lo que no sea `/auth/*`, `/health`, `/cluster` o `/datanodes/*` necesita el
encabezado `Authorization: Bearer <jwt>`.

### Usuarios

| Metodo | Ruta | Para que |
|---|---|---|
| POST | `/auth/register` | `{"username","password","group"?}` — crea el usuario y su `/home/<usuario>` |
| POST | `/auth/login` | `{"username","password"}` — devuelve el JWT |

### Namespace

| Metodo | Ruta | Para que |
|---|---|---|
| GET | `/fs/ls?path=/home/sebas` | Listar |
| GET | `/fs/stat?path=...` | Metadatos de una ruta |
| POST | `/fs/mkdir` | `{"path","perms"?}` |
| DELETE | `/fs/rmdir?path=...` | Borrar un directorio vacio |
| DELETE | `/fs/rm?path=...` | Borrar un archivo |
| POST | `/fs/mv` | `{"from","to"}` |
| POST | `/fs/chmod` | `{"path","perms"}` — permisos POSIX, ej. `755` |

### Subir y bajar archivos

Escribir un archivo son tres pasos; la version solo se publica en el commit:

| Metodo | Ruta | Para que |
|---|---|---|
| POST | `/files/write/begin` | `{"path"}` → `upload_id`, `version`, `block_size` y toma el lease |
| POST | `/files/write/allocate` | `{"upload_id"}` → `block_id`, los 3 DataNodes y el `token` |
| POST | `/files/write/renew` | `{"upload_id"}` — estira el lease si el archivo es grande |
| POST | `/files/write/commit` | `{"upload_id","blocks":[{index, block_id, size, checksum, nodes}]}` |
| POST | `/files/write/abort` | `{"upload_id"}` |
| GET | `/files/read?path=...&version=` | Lista de bloques con sus replicas vivas y el token |
| POST | `/files/lock` / `/files/unlock` | `{"path"}` |

El cliente le manda el bloque al **primer** DataNode de `nodes` y ese lo replica
en pipeline a los otros dos.

El `begin_write` toma un **lease** de 60 s sobre el archivo (un solo escritor).
Si la subida va a tardar mas, hay que llamar a `/files/write/renew` cada tanto;
si el lease se vence, el `allocate` y el `commit` fallan con 409 y la escritura
hay que empezarla de nuevo.

### Monitoreo

| Metodo | Ruta | Para que |
|---|---|---|
| GET | `/health` | Vivo / caido |
| GET | `/cluster` | Lider de Raft, term, DataNodes y resumen de metadatos |
| GET | `/datanodes` | Estado de todos los DataNodes |
| GET | `/admin/under-replicated` | Bloques con menos de 3 replicas vivas |

## API gRPC (para los DataNodes)

El contrato esta en [`src/dfsha.proto`](src/dfsha.proto) y el servidor escucha
en el puerto `dfsha.grpc-port` (50051 por defecto). Las clases Java se generan
solas al compilar, no se guardan en el repo.

| RPC | Para que |
|---|---|
| `Register` | Alta del DataNode: id, host, zona, capacidad, puertos |
| `Heartbeat` | Cada 3 s; la respuesta trae los comandos pendientes |
| `BlockReport` | Que bloques tiene de verdad; se compara contra los metadatos |
| `BlockReceived` | Ya tengo esta replica (el ControlNode la escribe en Raft) |
| `BlockRemoved` | Perdi un bloque: checksum malo o disco dañado |

Dos cosas importantes del contrato:

- Igual que en HDFS HA, **el DataNode se registra y manda el heartbeat a los
  tres ControlNodes**, no solo al lider: asi cualquiera de los tres sabe quien
  esta vivo y puede atender al cliente sin preguntar. Los seguidores responden
  `is_leader = false` y sin comandos.
- **Los comandos viajan en la respuesta del heartbeat**; el ControlNode nunca
  abre una conexion hacia el DataNode (evita lios de NAT y security groups).

Los mismos mensajes estan expuestos tambien como REST en `/datanodes/register`,
`/datanodes/heartbeat` y `/datanodes/block-received`, con los mismos nombres de
campo, por si les sirve para probar sin gRPC.

## Configuracion

Todo sale de `src/application.properties` y se puede cambiar por linea de
comandos: puertos, lista de los 3 ControlNodes, tamaño de bloque (4 MiB),
replicas por bloque (3), duracion del lease y los secretos del JWT y de los
tokens de bloque. El TLS esta comentado ahi mismo, listo para activarlo cuando
esten los certificados.
