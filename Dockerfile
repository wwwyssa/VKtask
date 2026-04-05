FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app

COPY pom.xml ./
COPY src ./src

RUN mvn -q -DskipTests package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/deps

FROM eclipse-temurin:17-jre
WORKDIR /app

COPY --from=builder /app/target/grpc-kv-tarantool-*.jar /app/app.jar
COPY --from=builder /app/target/deps /app/deps

EXPOSE 9090

ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/deps/*", "com.example.kv.Application"]
