// The ingest worker: a separate program (its own JVM) that consumes vehicle events from Kafka.
// Plugin versions come from the root build.gradle.kts.
plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-webmvc") // only to serve /actuator (health, metrics)
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.springframework.boot:spring-boot-starter-elasticsearch") // lexical index (BM25)
    implementation("io.qdrant:client:1.20.0") // vector index, over gRPC
    implementation(project(":embedding")) // runs the embedding model inside this JVM
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
}

// `gradle :ingest-worker:bootRun`: runs on your Mac, reads Kafka on localhost:29092,
// and reports to the monitoring stack as "ingest-worker-local".
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    dependsOn(rootProject.tasks.named("downloadAgents"), project(":embedding").tasks.named("downloadModel"))
    val agents: File = rootProject.layout.buildDirectory.dir("agents").get().asFile
    jvmArgs(
        "-XX:+UseG1GC",
        "--enable-native-access=ALL-UNNAMED",
        "-Xlog:gc:stdout:time,level",
        "-javaagent:$agents/opentelemetry-javaagent.jar",
        "-javaagent:$agents/pyroscope.jar",
    )
    environment(
        mapOf(
            "SERVER_PORT" to "8082", // 8081 is the API's local run
            "MODEL_DIR" to project(":embedding").layout.buildDirectory.dir("model").get().asFile.path,
            "OTEL_SERVICE_NAME" to "ingest-worker-local",
            "OTEL_EXPORTER_OTLP_ENDPOINT" to "http://localhost:4318",
            "OTEL_METRICS_EXPORTER" to "none",
            "OTEL_LOGS_EXPORTER" to "none",
            "OTEL_TRACES_SAMPLER" to "parentbased_traceidratio",
            "OTEL_TRACES_SAMPLER_ARG" to "0.01", // trace 1% of events: 426k events would mean 426k traces
            "PYROSCOPE_APPLICATION_NAME" to "ingest-worker-local",
            "PYROSCOPE_SERVER_ADDRESS" to "http://localhost:4040",
            "PYROSCOPE_FORMAT" to "jfr",
            "PYROSCOPE_PROFILER_EVENT" to "itimer",
            "PYROSCOPE_PROFILER_ALLOC" to "512k",
            "PYROSCOPE_PROFILER_LOCK" to "10ms",
        ),
    )
}

// Java 22+ warns (and will later refuse) when a library loads native code. ONNX Runtime and the profiler do.
tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    manifest { attributes("Enable-Native-Access" to "ALL-UNNAMED") }
}
