#!/usr/bin/env bash
# Genera los stubs de Python del DataNode a partir del contrato del ControlNode.
#
# Hay que correrlo cada vez que cambie controlnode/src/dfsha.proto: los stubs
# estan versionados en datanode/pb/ y, si quedan viejos, el DataNode no arranca
# ("module 'pb.dfsha_pb2_grpc' has no attribute ...").
set -euo pipefail
cd "$(dirname "$0")/.."

destino=datanode/pb
mkdir -p "$destino"
python3 -m grpc_tools.protoc -I controlnode/src \
    --python_out="$destino" --grpc_python_out="$destino" controlnode/src/dfsha.proto
sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' "$destino/dfsha_pb2_grpc.py"
cat > "$destino/__init__.py" <<'PY'
"""Stubs gRPC generados desde controlnode/src/dfsha.proto con scripts/gen_proto.sh."""
from . import dfsha_pb2, dfsha_pb2_grpc  # noqa: F401
PY
echo "stubs generados en $destino"
