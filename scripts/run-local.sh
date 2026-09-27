#!/usr/bin/env bash
# Levanta un clúster DFSha en la máquina local, sin Docker (útil para desarrollo).
#   ./scripts/run-local.sh [numero_de_datanodes]
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
DATANODES=${1:-4}
RUN_DIR=${DFSHA_RUN_DIR:-$ROOT/.run}
export DFSHA_BLOCK_SECRET=${DFSHA_BLOCK_SECRET:-dfsha-block-secret-local}
export DFSHA_JWT_SECRET=${DFSHA_JWT_SECRET:-dfsha-jwt-secret-local-0123456789}

mkdir -p "$RUN_DIR"
JAR=$(ls controlnode/target/dfsha-controlnode-*.jar 2>/dev/null | head -1 || true)
if [[ -z "$JAR" ]]; then
  echo "Compilando el ControlNode…"
  (cd controlnode && mvn -q -B package -DskipTests)
  JAR=$(ls controlnode/target/dfsha-controlnode-*.jar | head -1)
fi

echo "ControlNode: REST http://localhost:8080 · gRPC localhost:9090"
java -jar "$JAR" > "$RUN_DIR/cn1.log" 2>&1 &
echo $! > "$RUN_DIR/cn1.pid"

ZONES=(zona-a zona-a zona-b zona-c zona-b zona-c)
for i in $(seq 1 "$DATANODES"); do
  port=$((50050 + i))
  DFSHA_NODE_ID="dn$i" \
  DFSHA_DN_PORT="$port" \
  DFSHA_DN_ADVERTISE="localhost:$port" \
  DFSHA_ZONE="${ZONES[$(( (i-1) % ${#ZONES[@]} ))]}" \
  DFSHA_DATA_DIR="$RUN_DIR/dn$i" \
  DFSHA_CONTROLNODE="localhost:9090" \
  PYTHONPATH="$ROOT/datanode" \
  python3 -m dfsha_datanode > "$RUN_DIR/dn$i.log" 2>&1 &
  echo $! > "$RUN_DIR/dn$i.pid"
  echo "DataNode dn$i: gRPC localhost:$port · zona ${ZONES[$(( (i-1) % ${#ZONES[@]} ))]}"
done

echo
echo "Logs en $RUN_DIR · para detener todo: ./scripts/stop-local.sh"
echo "Cliente:  export PYTHONPATH=$ROOT/client DFSHA_CONTROLNODE_URL=http://localhost:8080"
echo "          python3 -m dfsha register ana && python3 -m dfsha login ana"
