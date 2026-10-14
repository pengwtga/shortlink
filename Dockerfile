# syntax=docker/dockerfile:1
# ---------- 构建阶段 ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# 先拷贝 pom 利用 Docker 缓存下载依赖
COPY pom.xml .
RUN mvn -B dependency:go-offline -q || true

COPY src ./src
RUN mvn -B clean package -DskipTests -q

# ---------- 运行阶段 ----------
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/shortlink-1.0.0.jar app.jar

ENV JAVA_OPTS="-Xms256m -Xmx512m"
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
