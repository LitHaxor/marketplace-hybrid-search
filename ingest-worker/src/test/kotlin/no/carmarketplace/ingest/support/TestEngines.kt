package no.carmarketplace.ingest.support

import io.qdrant.client.QdrantClient
import io.qdrant.client.QdrantGrpcClient
import java.time.Duration
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

// Real Kafka, Elasticsearch and Qdrant for the worker's integration tests (Testcontainers: Docker must run).
// Same versions as k8s/dev. Started once per test run, on first use, all three in parallel.
object TestEngines {

    val kafka: KafkaContainer = KafkaContainer("apache/kafka:4.4.0")

    val elasticsearch: EngineContainer = EngineContainer("elasticsearch:9.5.5")
        .withEnv("discovery.type", "single-node")
        .withEnv("xpack.security.enabled", "false")
        .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false") // CI disks are often nearly full
        .withExposedPorts(9200)
        .waitingFor(Wait.forHttp("/_cluster/health").forPort(9200).withStartupTimeout(Duration.ofMinutes(3)))

    val qdrant: EngineContainer = EngineContainer("qdrant/qdrant:v1.20.0")
        .withExposedPorts(QDRANT_HTTP, QDRANT_GRPC)
        .waitingFor(Wait.forHttp("/readyz").forPort(QDRANT_HTTP))

    init {
        Startables.deepStart(kafka, elasticsearch, qdrant).join()
        createLaneTopics()
    }

    val elasticsearchUrl: String
        get() = "http://${elasticsearch.host}:${elasticsearch.getMappedPort(9200)}"

    // For tests to look at what the worker wrote
    val qdrantClient: QdrantClient by lazy {
        QdrantClient(QdrantGrpcClient.newBuilder(qdrant.host, qdrant.getMappedPort(QDRANT_GRPC), false).build())
    }

    // Created up front (1 partition each), so the listeners never start on a missing topic
    private fun createLaneTopics() {
        val config: Map<String, Any> = mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
        AdminClient.create(config).use { admin: AdminClient ->
            admin.createTopics(LANE_TOPICS.map { topic: String -> NewTopic(topic, 1, 1.toShort()) }).all().get()
        }
    }
}

// GenericContainer's type parameter is the class itself, which Kotlin can't infer: name it once here
class EngineContainer(image: String) : GenericContainer<EngineContainer>(DockerImageName.parse(image))

val LANE_TOPICS: List<String> = listOf("vehicle.priority", "vehicle.changes", "vehicle.bulk")
const val QDRANT_HTTP: Int = 6333
const val QDRANT_GRPC: Int = 6334
