# ControlNode de DFSha — construir desde la raíz del repositorio:
#   docker build -f controlnode/Dockerfile -t dfsha/controlnode .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY controlnode/pom.xml controlnode/pom.xml
COPY proto proto
RUN cd controlnode && mvn -B -q dependency:go-offline -DskipTests || true
COPY controlnode/src controlnode/src
RUN cd controlnode && mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/controlnode/target/dfsha-controlnode-*.jar /app/controlnode.jar
ENV DFSHA_REST_PORT=8080 DFSHA_GRPC_PORT=9090
EXPOSE 8080 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${DFSHA_REST_PORT} && echo -e "GET /api/v1/health HTTP/1.0\r\n\r" >&3 && head -1 <&3 | grep -q 200'
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/controlnode.jar"]
