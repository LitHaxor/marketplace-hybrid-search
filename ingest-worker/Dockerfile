# syntax=docker/dockerfile:1
# Build from the project root (the worker shares the Gradle wrapper and agent versions with it):
#   docker build -f ingest-worker/Dockerfile -t carmarketplace-ingest-worker:dev .

# ---------- Stage 1: build ----------
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY embedding ./embedding
COPY ingest-worker ./ingest-worker
RUN --mount=type=cache,target=/root/.gradle ./gradlew :ingest-worker:bootJar :embedding:downloadModel :downloadAgents --no-daemon --console=plain

# ---------- Stage 2: run ----------
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /workspace/ingest-worker/build/libs/ingest-worker.jar app.jar
COPY --from=build /workspace/build/agents /app/agents
# The embedding model (23 MB, int8): in the image, so pods start without downloading anything
COPY --from=build /workspace/embedding/build/model /app/model
USER 1000:1000
EXPOSE 8080
# JVM flags come from JAVA_TOOL_OPTIONS in deployment.yaml
ENTRYPOINT ["java", "-jar", "app.jar"]
