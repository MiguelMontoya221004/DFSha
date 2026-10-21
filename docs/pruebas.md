# DFSha — Pruebas del sistema

`tests/e2e_sistema.py` levanta el sistema completo —3 ControlNodes con Raft y 4 DataNodes
en 3 zonas—, usa **el cliente de verdad** (`client/dfs_client.py`) para subir y bajar
archivos, comprueba lo que solo se ve por dentro y apaga todo al terminar.

```bash
cd controlnode && ./mvnw package -DskipTests && cd ..
pip install -r datanode/requirements.txt -r client/requirements.txt
python3 tests/e2e_sistema.py            # --keep deja el clúster arriba
```

## Qué prueba cada caso

| Grupo | Caso | Requisito |
|---|---|---|
| DataNode | `/status` responde con su zona y dice en cuántos ControlNodes está registrado | RF5 |
| Usuarios y namespace | register · login (deja al usuario en su `/home`) · mkdir · ls | RF1, RF4 |
| Subida | `put` sube el archivo completo · se parte en bloques de 4 MiB · 3 réplicas por bloque · réplicas en zonas distintas | RF2, RNF4 |
| Descarga | `get` reconstruye el archivo · es idéntico al original (SHA-256) | RF2, RNF3 |
| Acceso | lectura con `offset`/`length` · lectura con cabecera `Range` (HTTP 206) | RF3 |
| Seguridad | sin token se rechaza · token mal firmado · token de otro bloque · token de lectura usado para escribir | RNF6 |
| Integridad | el DataNode rechaza un bloque cuyo checksum no coincide (HTTP 422) | RNF3 |
| Alta disponibilidad | se sigue bajando con una réplica caída · el archivo sigue intacto · los bloques vuelven a 3 réplicas solos · se puede seguir subiendo con un DataNode menos | RNF2 |

## Última ejecución

```
Carpeta de trabajo: /tmp/dfsha-e2e-zkiki5k7

== Levantando 3 ControlNodes (Raft) + 4 DataNodes ==
  Raft arriba · lider: n2
  todo arriba

== El DataNode responde por HTTP ==
  [PASS] GET /status del DataNode — zona zona-a · registrado en 3 ControlNodes

== Usuario y namespace (cliente -> ControlNode) ==
  [PASS] register crea el usuario — Usuario ana creado.
  [PASS] login devuelve el JWT y deja al usuario en su home — /home/ana
  [PASS] mkdir crea la carpeta — Carpeta creada: /home/ana/datos
  [PASS] ls lista el home — {'entries': [{'path': '/home/ana/datos', 'name': 'datos', 'is_dir': Tr

== Subir un archivo: bloques, pipeline y replicas ==
  [PASS] put sube el archivo entero — Subido /home/ana/datos/prueba.bin · 3 bloque(s) · DataNodes: dn-1, dn-3, dn-4
  [PASS] el archivo quedo partido en bloques de 4 MiB — 3 bloques
  [PASS] cada bloque tiene 3 replicas — replicas: [3, 3, 3]
  [PASS] las replicas estan en zonas distintas

== Bajar el archivo ==
  [PASS] get reconstruye el archivo — Descargado /tmp/dfsha-e2e-zkiki5k7/bajado.bin · 3 bloque(s) · leído de: dn-1
  [PASS] el archivo bajado es identico al original

== Lectura por rangos (RF3) ==
  [PASS] lectura con offset y length — 64 bytes
  [PASS] lectura con cabecera Range (HTTP 206) — bytes 100-199/4194304

== Seguridad: el DataNode exige el token del ControlNode ==
  [PASS] sin token se rechaza
  [PASS] un token mal firmado se rechaza
  [PASS] un token de otro bloque no sirve
  [PASS] un token de lectura no autoriza escribir

== Integridad: checksum que no cuadra ==
  [PASS] el DataNode rechaza un bloque corrupto — HTTP 422

== Caida de un DataNode: failover y re-replicacion ==
  DataNode dn-1 detenido a la fuerza
  [PASS] el archivo se sigue bajando con una replica caida — Descargado /tmp/dfsha-e2e-zkiki5k7/bajado2.bin · 3 bloque(s) · leído de: dn-3
  [PASS] y sigue siendo identico
  [PASS] los bloques vuelven a 3 replicas (re-replicacion entre DataNodes) — 0 bloques pendientes
  [PASS] se puede seguir subiendo con un DataNode menos — Subido /home/ana/datos/segundo.bin · 3 bloque(s) · DataNodes: dn-2, dn-3, dn-4

== Resumen: 22/22 pruebas pasaron ==
```

## Pruebas manuales para el video

```bash
# 1. Quién es el líder de Raft y qué DataNodes hay
curl -s localhost:8443/cluster | python3 -m json.tool

# 2. Subir un archivo y ver en qué DataNodes quedó cada bloque
cd client && python3 main.py put ../archivo.bin
curl -s "localhost:8443/files/read?path=/home/ana/archivo.bin" \
     -H "Authorization: Bearer $JWT" | python3 -m json.tool

# 3. Alta disponibilidad del plano de control: matar al líder
#    (el cliente sigue funcionando contra otro ControlNode)
docker compose -f deploy/docker-compose.yml stop cn1
curl -s localhost:8444/cluster | python3 -m json.tool

# 4. Alta disponibilidad del plano de datos: matar un DataNode
docker compose -f deploy/docker-compose.yml stop dn2
curl -s localhost:8444/admin/under-replicated -H "Authorization: Bearer $JWT"
#    a los pocos segundos vuelve a salir vacío: ya se repusieron las réplicas

# 5. Lectura por rangos (RF3) sin bajar el bloque completo
curl -s "localhost:9001/blocks/$BLOCK?token=$BT&offset=0&length=32" | xxd | head -2
```

## Qué falta probar

- **Failover del líder de Raft con escritura en curso**: la suite mata DataNodes, no
  ControlNodes. Vale la pena hacerlo a mano en el video.
- **Rendimiento (RNF5)**: throughput contra número de DataNodes y tamaño de archivo. Se
  hará en AWS; el cliente hoy sube los bloques uno tras otro, así que subir en paralelo es
  la primera mejora si se quiere mostrar una curva.
- **Docker**: el `docker-compose.yml` está validado con `docker compose config` pero las
  imágenes no se han construido en una máquina con Docker. Hay que correr
  `docker compose up -d --build` con tiempo antes de grabar.
