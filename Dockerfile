# syntax=docker/dockerfile:1

# ---------- Stage 1: build ----------
# A full JDK (compiler + tools) to turn the Kotlin source into one runnable jar.
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace

# Build files first, source last: Docker caches each step, so editing code doesn't redo the setup.
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY ingest-worker/build.gradle.kts ./ingest-worker/build.gradle.kts
COPY embedding ./embedding
COPY src ./src

# The cache mount keeps Gradle and downloaded libraries between builds (outside the image).
# :bootJar = the API (the ":" means the root project only; plain "bootJar" would also build ingest-worker)
# :downloadAgents = the tracing and profiling agents (versions live in build.gradle.kts)
RUN --mount=type=cache,target=/root/.gradle ./gradlew :bootJar :downloadAgents :embedding:downloadModel --no-daemon --console=plain

# ---------- Stage 2: run ----------
# Only a JRE (runtime, no compiler) and the jar: no Gradle, no source code, a much smaller image.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /workspace/build/libs/kotlin-hybrid-search.jar app.jar

# Java agents: they hook into the JVM at startup, no code changes. Turned on with -javaagent in deployment.yaml.
COPY --from=build /workspace/build/agents /app/agents
# The embedding model, for turning search queries into vectors
COPY --from=build /workspace/embedding/build/model /app/model

# Don't run as root inside the container.
USER 1000:1000
EXPOSE 8080

# One container = one JVM process. JVM flags come from JAVA_TOOL_OPTIONS in deployment.yaml,
# so you can try other heap sizes or garbage collectors without rebuilding the image.
ENTRYPOINT ["java", "-jar", "app.jar"]
