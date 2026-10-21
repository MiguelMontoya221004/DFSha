# Cliente de DFSha — construir desde la raiz del repositorio:
#   docker build -f client/Dockerfile -t dfsha/client .
FROM python:3.12-slim
WORKDIR /app
COPY client/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY client/*.py /app/
RUN printf '#!/bin/sh\nexec python /app/main.py "$@"\n' > /usr/local/bin/dfsha && chmod +x /usr/local/bin/dfsha
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/bin/sh"]
