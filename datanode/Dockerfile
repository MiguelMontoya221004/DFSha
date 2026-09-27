# DataNode de DFSha — construir desde la raíz del repositorio:
#   docker build -f datanode/Dockerfile -t dfsha/datanode .
FROM python:3.12-slim
WORKDIR /app
COPY datanode/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY proto /app/proto
COPY datanode/dfsha_datanode /app/dfsha_datanode
# Los stubs gRPC se generan durante la imagen: así el contrato .proto es la única fuente de verdad.
RUN python -m grpc_tools.protoc -I proto \
        --python_out=dfsha_datanode/pb --grpc_python_out=dfsha_datanode/pb proto/dfsha.proto \
 && sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' dfsha_datanode/pb/dfsha_pb2_grpc.py
ENV DFSHA_DATA_DIR=/var/lib/dfsha/blocks DFSHA_DN_PORT=50051 PYTHONUNBUFFERED=1
VOLUME ["/var/lib/dfsha/blocks"]
EXPOSE 50051
ENTRYPOINT ["python", "-m", "dfsha_datanode"]
