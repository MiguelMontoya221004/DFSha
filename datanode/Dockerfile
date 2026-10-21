# DataNode de DFSha — construir desde la raiz del repositorio:
#   docker build -f datanode/Dockerfile -t dfsha/datanode .
FROM python:3.12-slim
WORKDIR /app

COPY datanode/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# El contrato con el ControlNode es su .proto; los stubs se generan en el build
COPY controlnode/src/dfsha.proto /app/proto/dfsha.proto
COPY datanode/*.py /app/
RUN mkdir -p /app/pb \
 && python -m grpc_tools.protoc -I proto --python_out=pb --grpc_python_out=pb proto/dfsha.proto \
 && sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' pb/dfsha_pb2_grpc.py \
 && printf 'from . import dfsha_pb2, dfsha_pb2_grpc  # noqa: F401\n' > pb/__init__.py

ENV DFSHA_DATA_DIR=/var/lib/dfsha/blocks DFSHA_CLIENT_PORT=9001 PYTHONUNBUFFERED=1
VOLUME ["/var/lib/dfsha/blocks"]
EXPOSE 9001

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD python -c "import httpx,os,sys; sys.exit(0 if httpx.get(f'http://127.0.0.1:{os.getenv(\"DFSHA_CLIENT_PORT\",\"9001\")}/health', timeout=2).status_code==200 else 1)"

CMD ["python", "main.py"]
