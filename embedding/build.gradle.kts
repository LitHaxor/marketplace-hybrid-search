// Shared library: text → 384-number vector with all-MiniLM-L6-v2, running inside the JVM through ONNX Runtime.
// No Spring here; the API and the ingest worker each create a TextEmbedder.
plugins {
    kotlin("jvm")
    id("io.spring.dependency-management") // test library versions from Spring Boot's list
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(25)
}

dependencyManagement {
    imports { mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES) }
}

dependencies {
    implementation("com.microsoft.onnxruntime:onnxruntime:1.31.0")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// ---------- Embedding model: all-MiniLM-L6-v2 (384 dimensions), int8-quantized build for ARM CPUs ----------
// Pinned to one commit of the Hugging Face repo, so every build gets exactly the same model.
val modelCommit: String = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
val modelFiles: Map<String, String> = mapOf(
    "model.onnx" to "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/$modelCommit/onnx/model_qint8_arm64.onnx",
    "vocab.txt" to "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/$modelCommit/vocab.txt",
)
val modelDir: Provider<Directory> = layout.buildDirectory.dir("model")

val downloadModel: TaskProvider<Task> = tasks.register("downloadModel") {
    group = "embedding"
    description = "Download the embedding model and its vocabulary into build/model"
    inputs.property("urls", modelFiles)
    outputs.dir(modelDir)
    doLast {
        val dir: File = modelDir.get().asFile
        dir.deleteRecursively()
        dir.mkdirs()
        modelFiles.forEach { (fileName: String, url: String) ->
            uri(url).toURL().openStream().use { input -> File(dir, fileName).outputStream().use { input.copyTo(it) } }
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    dependsOn(downloadModel)
    environment("MODEL_DIR", modelDir.get().asFile.path)
}
