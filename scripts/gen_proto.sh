#!/usr/bin/env bash
# Genera los stubs de Python del DataNode a partir de proto/dfsha.proto.
#
# El ControlNode genera los suyos solo al compilar con Maven, y el cliente ya no
# necesita stubs: habla HTTP con los DataNodes y REST con el ControlNode.
set -euo pipefail
cd "$(dirname "$0")/.."

destino=datanode/pb
mkdir -p "$destino"
python3 -m grpc_tools.protoc -I proto \
    --python_out="$destino" --grpc_python_out="$destino" proto/dfsha.proto
# El stub generado hace "import dfsha_pb2"; lo volvemos import relativo del paquete
sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' "$destino/dfsha_pb2_grpc.py"
echo "stubs generados en $destino"
