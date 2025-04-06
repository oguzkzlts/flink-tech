FROM maven:3.9.6-eclipse-temurin-11 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM flink:1.20.1

# Install ping and curl utilities
RUN apt-get update && \
    apt-get install -y inetutils-ping curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/FlinkTech-1.0-SNAPSHOT.jar /opt/flink/usrlib/
RUN chmod +x /opt/flink/usrlib/FlinkTech-1.0-SNAPSHOT.jar