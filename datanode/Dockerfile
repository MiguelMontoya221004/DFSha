# DataNode de DFSha — construir desde la raiz del repositorio:
#   docker build -f datanode/Dockerfile -t dfsha/datanode .
FROM python:3.12-slim
WORKDIR /app

COPY datanode/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY proto /app/proto
COPY datanode/*.py /app/
COPY datanode/pb/__init__.py /app/pb/__init__.py

# Los stubs de gRPC se generan durante el build: el .proto es la unica fuente de verdad
RUN python -m grpc_tools.protoc -I proto --python_out=pb --grpc_python_out=pb proto/dfsha.proto \
 && sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' pb/dfsha_pb2_grpc.py

ENV DFSHA_DATA_DIR=/var/lib/dfsha/blocks DFSHA_DN_PORT=50051 PYTHONUNBUFFERED=1
VOLUME ["/var/lib/dfsha/blocks"]
EXPOSE 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD python -c "import httpx,os,sys; sys.exit(0 if httpx.get(f'http://127.0.0.1:{os.getenv(\"DFSHA_DN_PORT\",\"50051\")}/health', timeout=2).status_code==200 else 1)"

CMD ["python", "main.py"]
