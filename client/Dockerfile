# Cliente de DFSha — construir desde la raíz del repositorio:
#   docker build -f client/Dockerfile -t dfsha/client .
FROM python:3.12-slim
WORKDIR /app
COPY client/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY proto /app/proto
COPY client/dfsha /app/dfsha
RUN python -m grpc_tools.protoc -I proto \
        --python_out=dfsha/pb --grpc_python_out=dfsha/pb proto/dfsha.proto \
 && sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' dfsha/pb/dfsha_pb2_grpc.py \
 && printf '#!/bin/sh\nexec python -m dfsha "$@"\n' > /usr/local/bin/dfsha && chmod +x /usr/local/bin/dfsha
ENV DFSHA_CONTROLNODE_URL=http://cn1:8080 PYTHONUNBUFFERED=1
ENTRYPOINT ["/bin/sh"]
