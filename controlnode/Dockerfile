# ControlNode de DFSha — construir desde la raiz del repositorio:
#   docker build -f controlnode/Dockerfile -t dfsha/controlnode .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY controlnode/pom.xml pom.xml
RUN mvn -B -q dependency:go-offline -DskipTests || true
COPY controlnode/src src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/controlnode-*.jar /app/controlnode.jar
# REST para el cliente · gRPC para los DataNodes · Raft entre ControlNodes
EXPOSE 8443 50051 9501
VOLUME ["/app/data"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=10 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8443 && echo -e "GET /health HTTP/1.0\r\n\r" >&3 && head -1 <&3 | grep -q 200'
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/controlnode.jar"]
