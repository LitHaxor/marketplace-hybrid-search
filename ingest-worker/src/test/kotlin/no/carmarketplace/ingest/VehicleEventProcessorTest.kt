package no.carmarketplace.ingest

import io.qdrant.client.PointIdFactory.id
import io.qdrant.client.grpc.Points.RetrievedPoint
import java.util.UUID
import no.carmarketplace.ingest.search.VehicleVectors
import no.carmarketplace.ingest.support.TestEngines
import no.carmarketplace.ingest.support.WorkerIntegrationTest
import no.carmarketplace.ingest.support.eventJson
import no.carmarketplace.ingest.support.record
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

// The worker's core rules, against real Elasticsearch and Qdrant and the real model.
// Batches are handed to the processor directly (no Kafka), so each step is synchronous and exact.
class VehicleEventProcessorTest : WorkerIntegrationTest() {

    @Autowired
    private lateinit var processor: VehicleEventProcessor

    @Test
    fun `an active car is indexed for BM25 and gets a vector with its filter fields`() {
        val id: UUID = UUID.randomUUID()

        processor.process("changes", listOf(record("vehicle.changes", eventJson(id))))

        val document: Map<String, Any?> = assertNotNull(document(id))
        assertEquals("ACTIVE", document["status"])
        assertEquals("2016 toyota rav4 xle", document["title"])
        assertEquals(mapOf("lat" to 40.73, "lon" to -73.99), document["location"])
        val point: RetrievedPoint = assertNotNull(point(id))
        assertEquals("toyota", point.payloadMap.getValue("manufacturer").stringValue)
        assertEquals(12500L, point.payloadMap.getValue("price").integerValue)
        assertEquals(2016L, point.payloadMap.getValue("year").integerValue)
    }

    @Test
    fun `a sold car stays in Elasticsearch as SOLD and leaves Qdrant`() {
        val id: UUID = UUID.randomUUID()
        processor.process("changes", listOf(record("vehicle.changes", eventJson(id, version = 0))))

        processor.process("priority", listOf(record("vehicle.priority", eventJson(id, status = "SOLD", version = 1), "VehicleSold")))

        assertEquals("SOLD", document(id)?.get("status"))
        assertNull(point(id))
    }

    @Test
    fun `a late older event cannot bring a sold car back`() {
        val id: UUID = UUID.randomUUID()
        processor.process("changes", listOf(record("vehicle.changes", eventJson(id, version = 0))))
        processor.process("priority", listOf(record("vehicle.priority", eventJson(id, status = "SOLD", version = 1), "VehicleSold")))
        val staleBefore: Double = events("changes", "stale")

        // The "created" event arrives again after the sale (e.g. a slow lane, or a redelivery)
        processor.process("changes", listOf(record("vehicle.changes", eventJson(id, version = 0))))

        assertEquals("SOLD", document(id)?.get("status"))
        assertNull(point(id), "a sold car must not get its vector back")
        assertEquals(staleBefore + 1, events("changes", "stale"))
    }

    @Test
    fun `a redelivered event repairs a vector that never got written`() {
        val id: UUID = UUID.randomUUID()
        val created: ConsumerRecord<String, String> = record("vehicle.bulk", eventJson(id, version = 0))
        processor.process("bulk", listOf(created))
        // A crash after Elasticsearch but before Qdrant: the vector is missing
        TestEngines.qdrantClient.deleteAsync(VehicleVectors.COLLECTION, listOf(id(id))).get()

        processor.process("bulk", listOf(created)) // Kafka delivers the same event again

        assertNotNull(point(id), "the same version is accepted again (external_gte), so the vector comes back")
    }

    @Test
    fun `an invalid message is skipped without dropping the valid events in its batch`() {
        val id: UUID = UUID.randomUUID()
        val invalidBefore: Double = events("changes", "invalid")

        processor.process(
            "changes",
            listOf(record("vehicle.changes", "not json"), record("vehicle.changes", eventJson(id))),
        )

        assertNotNull(document(id))
        assertNotNull(point(id))
        assertEquals(invalidBefore + 1, events("changes", "invalid"))
    }
}
