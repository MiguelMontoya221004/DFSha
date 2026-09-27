# DFSha — Pruebas del Hito 2

La suite `tests/e2e_test.py` levanta un clúster completo (1 ControlNode + 4 DataNodes en
3 zonas), ejecuta las pruebas y lo apaga. No necesita Docker: usa el jar del ControlNode
y procesos Python para los DataNodes.

```bash
cd controlnode && mvn -q package -DskipTests && cd ..
python3 tests/e2e_test.py            # --keep deja el clúster arriba
```

## Qué prueba cada caso

| Grupo | Caso | Requisito |
|---|---|---|
| Autenticación | login devuelve JWT · sin token se rechaza · contraseña incorrecta se rechaza | RF4, RNF6 |
| Namespace | `mkdir -p` crea la jerarquía · el home del usuario es privado | RF1, RNF6 |
| Transferencia | `put` divide en bloques de 4 MiB · los bloques se reparten entre ≥3 DataNodes · 3 réplicas por bloque · réplicas en zonas distintas · `get` devuelve el archivo intacto (SHA-256) | RF2, RNF4, RNF5 |
| Acceso | lectura aleatoria con `seek` · copy-on-write reescribe sólo el bloque afectado · la escritura parcial es visible | RF3, RNF3 |
| Seguridad | otro usuario no puede leer el archivo · el DataNode exige token de bloque válido | RNF6 |
| Consistencia | un solo escritor por archivo (lease) · no se confirma sin quórum de 2 réplicas | RNF3 |
| Alta disponibilidad | la lectura funciona con una réplica caída · el ControlNode detecta el nodo muerto · los bloques vuelven a 3 réplicas por re-replicación P2P · se sigue escribiendo con un DataNode menos | RNF2 |
| Borrado | `rm` borra el archivo y libera bloques · `mv` renombra sin mover datos | RF1 |

## Última ejecución

```
Directorio de trabajo: /tmp/dfsha-e2e-3tqonqiq

== Levantando el clúster (1 ControlNode + 4 DataNodes) ==
  clúster listo

== RF4: autenticación y usuarios ==
  [PASS] login devuelve un JWT — rol user
  [PASS] sin token se rechaza — UNAUTHORIZED
  [PASS] contraseña incorrecta se rechaza — UNAUTHORIZED

== RF1: gestión del sistema de archivos ==
  [PASS] mkdir -p crea la jerarquía
  [PASS] el home del usuario es privado

== RF2: transferencia y particionamiento ==
  [PASS] put divide el archivo en bloques — 3 bloques de 4 MiB
  [PASS] los bloques se reparten entre varios DataNodes — nodos usados: ['dn1', 'dn3', 'dn4']
  [PASS] cada bloque tiene 3 réplicas — réplicas por bloque: [3, 3, 3]
  [PASS] las réplicas están en zonas distintas
  [PASS] get reconstruye el archivo intacto — 142.4 MB/s con 4 hilos

== RF3: acceso a archivos (open/seek/read y escritura parcial) ==
  [PASS] lectura aleatoria con seek
  [PASS] copy-on-write sólo reescribe el bloque afectado — bloques nuevos: [1]
  [PASS] la escritura parcial es visible

== RNF6: control de acceso y tokens de bloque ==
  [PASS] otro usuario no puede leer el archivo — FORBIDDEN
  [PASS] el DataNode exige un token de bloque válido — PERMISSION_DENIED

== RNF3: consistencia (lease y quórum) ==
  [PASS] un solo escritor por archivo — Otro cliente está escribiendo el archivo (lease de ana, expira en 119 s)
  [PASS] no se confirma sin quórum de réplicas — El bloque 0 sólo tiene 0 réplica(s); se exigen 2

== RNF2: caída de un DataNode, failover y re-replicación ==
  DataNode dn1 detenido a la fuerza
Réplica dn1 falló para el bloque 5c00141b-7415-46e7-a97d-40fd0b467d1d (<_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: Failed to connect to remote host: Connection refused"
	debug_error_string = "UNAVAILABLE:failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: Failed to connect to remote host: Connection refused"
>); probando la siguiente
Réplica dn1 falló para el bloque 43b4c67f-e487-41d2-b285-5f02a2bcf5a3 (<_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: Failed to connect to remote host: Connection refused"
	debug_error_string = "UNAVAILABLE:failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: Failed to connect to remote host: Connection refused"
>); probando la siguiente
Réplica dn1 falló para el bloque ea94ff8f-7a17-4626-96ec-c1d840c87ff9 (<_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: Failed to connect to remote host: Connection refused"
	debug_error_string = "UNAVAILABLE:failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:50051: Failed to connect to remote host: Connection refused"
>); probando la siguiente
  [PASS] la lectura sigue funcionando con una réplica caída
  [PASS] el ControlNode detecta la caída de dn1 — muertos: ['dn1']
  [PASS] los bloques vuelven a 3 réplicas por re-replicación P2P — pendientes: 0
  [PASS] se puede seguir escribiendo con un DataNode menos — nodos: ['dn2', 'dn3', 'dn4']

== RF1: borrado y liberación de bloques ==
  [PASS] rm borra el archivo — NOT_FOUND
  [PASS] mv renombra sin mover datos

== Resumen: 23/23 pruebas pasaron ==
```

## Observaciones de rendimiento (entorno de desarrollo, un solo host)

Las cifras salen del mismo entorno donde corren los 5 procesos, así que miden el
paralelismo del cliente y la ruta de datos, no la red: con 4 hilos, un archivo de 12 MiB
se sube a ~27 MB/s (incluye escribir 3 copias de cada bloque) y se baja a ~110-125 MB/s.
Las mediciones reales del RNF5 (throughput contra número de DataNodes y de hilos) se
harán en AWS durante el Hito 4.

## Pruebas manuales sugeridas para el video

```bash
# 1. Estado del clúster y distribución de bloques
python3 -m dfsha status
python3 -m dfsha stat /home/ana/prueba.bin     # muestra réplicas por bloque y su zona

# 2. Matar un DataNode en vivo y ver la re-replicación
docker compose -f deploy/docker-compose.yml stop dn2
docker compose -f deploy/docker-compose.yml logs -f cn1 | grep -E "MUERTO|Re-replicando"
python3 -m dfsha get /home/ana/prueba.bin ./copia.bin   # sigue funcionando

# 3. Dos escritores sobre el mismo archivo (lease)
python3 -m dfsha put grande.bin /home/ana/x.bin &
python3 -m dfsha put grande.bin /home/ana/x.bin -f      # responde LOCKED

# 4. Escritura parcial con copy-on-write
python3 -m dfsha write /home/ana/prueba.bin --offset 5242880 --text "HOLA"
python3 -m dfsha read  /home/ana/prueba.bin --offset 5242880 --count 4
```
