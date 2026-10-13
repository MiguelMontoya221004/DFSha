# DFSha — Pruebas del Hito 2

La suite `tests/e2e_datanode.py` levanta un clúster completo (1 ControlNode + 4 DataNodes
en 3 zonas), ejecuta las pruebas y lo apaga. No necesita Docker: usa el jar del ControlNode
y procesos Python para los DataNodes.

Todas las llamadas se hacen por HTTP con `requests`, igual que el cliente, así que la
suite también sirve de **referencia de uso de la API** para `client/dfs_client.py`.

```bash
cd controlnode && mvn -q package -DskipTests && cd ..
pip install -r datanode/requirements.txt
python3 tests/e2e_datanode.py            # --keep deja el clúster arriba
```

## Qué prueba cada caso

| Grupo | Caso | Requisito |
|---|---|---|
| API del DataNode | `/health` responde · `/status` trae zona, capacidad y bloques | RF5 |
| Autenticación | login contra el ControlNode devuelve JWT | RF4 |
| Transferencia | el pipeline HTTP deja 3 copias de cada bloque · el archivo se parte en bloques de 4 MiB · 3 réplicas por bloque · réplicas en zonas distintas | RF2, RNF4 |
| Descarga | el archivo descargado es idéntico al original (SHA-256) | RF2, RNF3 |
| Acceso | lectura de un rango con `offset`/`length` · lectura con cabecera `Range` (HTTP 206) | RF3 |
| Seguridad | sin token se rechaza · token mal firmado se rechaza · token de otro bloque no sirve · token de lectura no autoriza escribir | RNF6 |
| Integridad | el DataNode rechaza un bloque cuyo checksum no coincide (HTTP 422) | RNF3 |
| Alta disponibilidad | la descarga funciona con una réplica caída · el ControlNode detecta el nodo muerto · los bloques vuelven a 3 réplicas copiándose entre DataNodes · se puede seguir subiendo con un DataNode menos | RNF2 |

## Última ejecución

```
Carpeta de trabajo: /tmp/dfsha-dn-reghwk0s

== Levantando el cluster (1 ControlNode + 4 DataNodes) ==
  listo

== El DataNode responde por HTTP ==
  [PASS] GET /health responde — nodo dn1
  [PASS] GET /status trae el estado del nodo — zona zona-a · capacidad 8 GB

== Sesion contra el ControlNode (REST) ==
  [PASS] login devuelve un JWT — rol user

== Subida: bloques repartidos y replicados ==
  [PASS] el pipeline HTTP deja 3 copias de cada bloque — nodos que confirmaron: ['dn1', 'dn2', 'dn3', 'dn4']
  [PASS] el archivo se partio en bloques de 4 MiB — 3 bloques
  [PASS] cada bloque tiene 3 replicas — replicas: [3, 3, 3]
  [PASS] las replicas estan en zonas distintas

== Descarga y lectura por rango ==
  [PASS] el archivo descargado es identico al original — leido de: ['dn1', 'dn3']
  [PASS] lectura de un rango con offset y length — 64 bytes
  [PASS] lectura con cabecera Range (HTTP 206) — bytes 100-199/4194304

== Seguridad: el DataNode exige el token del ControlNode ==
  [PASS] sin token se rechaza la lectura — HTTP 403
  [PASS] un token mal firmado se rechaza
  [PASS] un token de otro bloque no sirve
  [PASS] un token de lectura no autoriza escribir

== Integridad: checksum incorrecto ==
  [PASS] el DataNode rechaza un bloque con checksum que no cuadra — HTTP 422

== Caida de un DataNode: lectura y re-replicacion ==
  DataNode dn3 detenido a la fuerza
  [PASS] la descarga sigue funcionando con una replica caida — leido de: ['dn1', 'dn2']
  [PASS] el ControlNode detecta la caida de dn3 — muertos: ['dn3']
  [PASS] los bloques vuelven a 3 replicas copiandose entre DataNodes — pendientes: 0
  [PASS] se puede seguir subiendo con un DataNode menos — nodos: ['dn1', 'dn2', 'dn4']

== Resumen: 19/19 pruebas pasaron ==
```

## Pruebas manuales sugeridas para el video

```bash
# 1. Estado del clúster y de un DataNode
curl -s localhost:8080/api/v1/cluster/status -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
curl -s localhost:50051/status | python3 -m json.tool

# 2. Ver un bloque viajando: subirlo al primer nodo del pipeline y comprobar
#    que aparece en los tres (cada /meta responde con el mismo sha256)
curl -s "localhost:50052/blocks/$BLOCK/meta?token=$BT" | python3 -m json.tool
curl -s "localhost:50053/blocks/$BLOCK/meta?token=$BT" | python3 -m json.tool

# 3. Matar un DataNode en vivo y ver la re-replicación en el log del ControlNode
docker compose -f deploy/docker-compose.yml stop dn2
docker compose -f deploy/docker-compose.yml logs -f cn1 | grep -E "MUERTO|Re-replicando"

# 4. Lectura por rango (RF3) sin bajar el bloque completo
curl -s "localhost:50051/blocks/$BLOCK?token=$BT&offset=0&length=32" | xxd | head -2
```

## Qué falta medir

Las cifras de rendimiento del RNF5 (throughput contra número de DataNodes y de hilos del
cliente) se harán en AWS durante el Hito 4, cuando el cliente nuevo pueda subir bloques en
paralelo. El DataNode ya está listo para eso: cada bloque es una petición HTTP
independiente y el servidor atiende varias a la vez.
