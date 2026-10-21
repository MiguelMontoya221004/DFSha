#!/usr/bin/env bash
# Levanta el sistema completo en esta maquina, sin Docker:
# 3 ControlNodes (Raft) + N DataNodes.
#
#   ./scripts/run-local.sh [numero_de_datanodes]
#
# Para apagarlo todo:  ./scripts/stop-local.sh
set -euo pipefail
cd "$(dirname "$0")/.."
RAIZ=$(pwd)
DATANODES=${1:-4}
RUN_DIR=${DFSHA_RUN_DIR:-$RAIZ/.run}
SECRETO=${DFSHA_BLOCK_SECRET:-clave-hmac-compartida-con-los-datanodes}

mkdir -p "$RUN_DIR"

JAR=$(ls controlnode/target/controlnode-*.jar 2>/dev/null | head -1 || true)
if [[ -z "$JAR" ]]; then
  echo "Compilando el ControlNode..."
  (cd controlnode && ./mvnw -q -B package -DskipTests)
  JAR=$(ls controlnode/target/controlnode-*.jar | head -1)
fi

# ---------------------------- ControlNodes ----------------------------
puerto_rest=8443
for i in 1 2 3; do
  java -jar "$JAR" \
      --dfsha.node-id="n$i" \
      --server.port="$puerto_rest" \
      --dfsha.grpc-port="$((50050 + i))" \
      --dfsha.storage-dir="$RUN_DIR/raft" \
      --spring.datasource.url="jdbc:h2:file:$RUN_DIR/raft/n$i/meta;DB_CLOSE_DELAY=-1" \
      > "$RUN_DIR/n$i.log" 2>&1 &
  echo $! > "$RUN_DIR/n$i.pid"
  echo "ControlNode n$i: REST http://localhost:$puerto_rest · gRPC :$((50050 + i))"
  puerto_rest=$((puerto_rest + 1))
done

echo "Esperando a que Raft elija lider..."
for _ in $(seq 1 60); do
  if curl -s http://localhost:8443/health 2>/dev/null | grep -q '"es_lider"'; then break; fi
  sleep 2
done
curl -s http://localhost:8443/cluster | head -c 200; echo

# ------------------------------ DataNodes ------------------------------
ZONAS=(zona-a zona-a zona-b zona-c zona-b zona-c)
for i in $(seq 1 "$DATANODES"); do
  puerto=$((9000 + i))
  DFSHA_NODE_ID="dn-$i" \
  DFSHA_HOST="127.0.0.1" \
  DFSHA_CLIENT_PORT="$puerto" \
  DFSHA_ZONE="${ZONAS[$(( (i-1) % ${#ZONAS[@]} ))]}" \
  DFSHA_DATA_DIR="$RUN_DIR/dn-$i" \
  DFSHA_CONTROLNODES="127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053" \
  DFSHA_BLOCK_SECRET="$SECRETO" \
  python3 "$RAIZ/datanode/main.py" > "$RUN_DIR/dn-$i.log" 2>&1 &
  echo $! > "$RUN_DIR/dn-$i.pid"
  echo "DataNode dn-$i: HTTP http://localhost:$puerto · zona ${ZONAS[$(( (i-1) % ${#ZONAS[@]} ))]}"
done

echo
echo "Logs en $RUN_DIR"
echo "Cliente:  cd client && python3 main.py register ana && python3 main.py login ana"
echo "          python3 main.py put ./archivo.bin && python3 main.py ls"
