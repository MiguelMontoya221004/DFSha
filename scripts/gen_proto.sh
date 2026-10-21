#!/usr/bin/env bash
# Genera los stubs de Python del DataNode a partir del contrato del ControlNode.
#
# El contrato lo manda el ControlNode: controlnode/src/dfsha.proto (servicio
# ControlService). El ControlNode genera sus clases Java solo al compilar; aqui
# generamos las de Python.
set -euo pipefail
cd "$(dirname "$0")/.."

destino=datanode/pb
mkdir -p "$destino"
python3 -m grpc_tools.protoc -I controlnode/src \
    --python_out="$destino" --grpc_python_out="$destino" controlnode/src/dfsha.proto
# El stub generado hace "import dfsha_pb2"; lo volvemos import relativo del paquete
sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' "$destino/dfsha_pb2_grpc.py"
cat > "$destino/__init__.py" <<'PY'
"""Stubs gRPC generados desde controlnode/src/dfsha.proto con scripts/gen_proto.sh.

No se editan a mano: si el ControlNode cambia el contrato, se vuelven a generar.
"""
from . import dfsha_pb2, dfsha_pb2_grpc  # noqa: F401
PY
echo "stubs generados en $destino"
