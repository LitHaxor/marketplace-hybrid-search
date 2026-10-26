plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    id("org.springframework.boot") version "4.1.2"
    id("io.spring.dependency-management") version "1.1.7"
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
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("de.siegmar:fastcsv:4.4.0")
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-elasticsearch") // search: BM25
    implementation("io.qdrant:client:1.20.0") // search: vectors, over gRPC
    implementation(project(":embedding")) // search: query text → vector
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus") // /actuator/prometheus for Prometheus to scrape


    testImplementation("org.liquibase:liquibase-core")
}

// Java 22+ warns (and will later refuse) when a library loads native code. ONNX Runtime and the profiler do.
tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    manifest { attributes("Enable-Native-Access" to "ALL-UNNAMED") }
}

// ---------- Observability agents ----------
// One place for the versions: the Docker image and `gradle bootRun` both use build/agents.
//   OpenTelemetry: a trace per request (HTTP, SQL, outgoing calls) → Tempo
//   Pyroscope:     CPU, allocation and lock profiles (async-profiler) → Pyroscope
val agentDownloads: Map<String, String> = mapOf(
    "opentelemetry-javaagent.jar" to
        "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.33.0/opentelemetry-javaagent.jar",
    "pyroscope.jar" to
        "https://github.com/grafana/pyroscope-java/releases/download/v2.9.2/pyroscope.jar",
)
val agentsDir: Provider<Directory> = layout.buildDirectory.dir("agents")

val downloadAgents: TaskProvider<Task> = tasks.register("downloadAgents") {
    group = "observability"
    description = "Download the OpenTelemetry and Pyroscope Java agents into build/agents"
    inputs.property("urls", agentDownloads) // a new version in the map → downloads again
    outputs.dir(agentsDir)
    doLast {
        val dir: File = agentsDir.get().asFile
        dir.deleteRecursively()
        dir.mkdirs()
        agentDownloads.forEach { (fileName: String, url: String) ->
            uri(url).toURL().openStream().use { input -> File(dir, fileName).outputStream().use { input.copyTo(it) } }
        }
    }
}

// `gradle bootRun`: the app runs on your Mac but reports to the monitoring stack in Kubernetes,
// which OrbStack publishes on localhost. It shows up as "api-local" in Grafana.
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    dependsOn(downloadAgents, project(":embedding").tasks.named("downloadModel"))
    val agents: File = agentsDir.get().asFile
    jvmArgs(
        "-XX:+UseG1GC", // same collector as in Kubernetes (see deployment.yaml)
        "--enable-native-access=ALL-UNNAMED",
        "-Xlog:gc:stdout:time,level", // one log line per garbage collection
        "-javaagent:$agents/opentelemetry-javaagent.jar",
        "-javaagent:$agents/pyroscope.jar",
    )
    environment(
        mapOf(
            "SERVER_PORT" to "8081", // 8080 is taken by the API running in Kubernetes
            "MODEL_DIR" to project(":embedding").layout.buildDirectory.dir("model").get().asFile.path,
            "OTEL_SERVICE_NAME" to "api-local",
            "OTEL_EXPORTER_OTLP_ENDPOINT" to "http://localhost:4318",
            "OTEL_METRICS_EXPORTER" to "none", // metrics go through Prometheus instead
            "OTEL_LOGS_EXPORTER" to "none",
            "PYROSCOPE_APPLICATION_NAME" to "api-local",
            "PYROSCOPE_SERVER_ADDRESS" to "http://localhost:4040",
            "PYROSCOPE_FORMAT" to "jfr",
            "PYROSCOPE_PROFILER_EVENT" to "itimer",
            "PYROSCOPE_PROFILER_ALLOC" to "512k",
            "PYROSCOPE_PROFILER_LOCK" to "10ms",
        ),
    )
}

// Migrations, run by hand like the TypeORM CLI. Code: src/test/kotlin/no/carmarketplace/Migrations.kt

tasks.register<JavaExec>("migrate-generate") {
    group = "migration"
    description = "Diff entity classes against the database and create a new migration file (typeorm migration:generate)"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "no.carmarketplace.MigrationsKt"
    args("generate")
}

tasks.register<JavaExec>("migrate-up") {
    group = "migration"
    description = "Apply all pending migrations (typeorm migration:run)"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "no.carmarketplace.MigrationsKt"
    args("up")
}

tasks.register<JavaExec>("migrate-down") {
    group = "migration"
    description = "Undo the last applied migration (typeorm migration:revert)"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "no.carmarketplace.MigrationsKt"
    args("down")
}
