#!/usr/bin/env bash
# Genera los stubs de Python para el DataNode y el Cliente a partir de proto/dfsha.proto.
# Java se genera automáticamente con Maven (protobuf-maven-plugin).
set -euo pipefail
cd "$(dirname "$0")/.."

for target in datanode/dfsha_datanode/pb client/dfsha/pb; do
  mkdir -p "$target"
  python3 -m grpc_tools.protoc -I proto \
      --python_out="$target" --grpc_python_out="$target" proto/dfsha.proto
  # El stub generado hace "import dfsha_pb2"; lo volvemos import relativo del paquete.
  sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' "$target/dfsha_pb2_grpc.py"
  cat > "$target/__init__.py" <<'PY'
"""Stubs gRPC generados desde proto/dfsha.proto (no editar a mano)."""
from . import dfsha_pb2, dfsha_pb2_grpc  # noqa: F401
PY
  echo "stubs generados en $target"
done
