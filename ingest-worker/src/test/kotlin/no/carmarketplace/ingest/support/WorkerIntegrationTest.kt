package no.carmarketplace.ingest.support

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.GetResponse
import io.micrometer.core.instrument.MeterRegistry
import io.qdrant.client.PointIdFactory.id
import io.qdrant.client.grpc.Points.RetrievedPoint
import java.util.UUID
import no.carmarketplace.ingest.search.VehicleIndex
import no.carmarketplace.ingest.search.VehicleVectors
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

// Base class for the worker's integration tests: the whole worker (listeners, model, index setup)
// against TestEngines. Every subclass shares one Spring context. Tests use fresh vehicle ids, so they
// never see each other's data.
@SpringBootTest
abstract class WorkerIntegrationTest {

    @Autowired
    protected lateinit var elasticsearch: ElasticsearchClient

    @Autowired
    protected lateinit var meterRegistry: MeterRegistry

    // The Elasticsearch document, or null. Get-by-id is real-time: no refresh needed.
    protected fun document(id: UUID): Map<String, Any?>? {
        @Suppress("UNCHECKED_CAST")
        val response: GetResponse<Map<String, Any?>> = elasticsearch.get(
            { g -> g.index(VehicleIndex.INDEX).id(id.toString()) },
            Map::class.java as Class<Map<String, Any?>>,
        )
        return if (response.found()) response.source() else null
    }

    // The Qdrant point with its payload, or null
    protected fun point(id: UUID): RetrievedPoint? =
        TestEngines.qdrantClient.retrieveAsync(VehicleVectors.COLLECTION, listOf(id(id)), true, false, null).get().firstOrNull()

    // ingest_events_total{lane, result}
    protected fun events(lane: String, result: String): Double =
        meterRegistry.find("ingest.events").tags("lane", lane, "result", result).counter()?.count() ?: 0.0

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun engines(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { TestEngines.kafka.bootstrapServers }
            registry.add("spring.elasticsearch.uris") { TestEngines.elasticsearchUrl }
            registry.add("qdrant.host") { TestEngines.qdrant.host }
            registry.add("qdrant.port") { TestEngines.qdrant.getMappedPort(QDRANT_GRPC) }
        }
    }
}
