package no.carmarketplace.ingest

import java.time.Duration
import java.util.UUID
import no.carmarketplace.ingest.support.TestEngines
import no.carmarketplace.ingest.support.WorkerIntegrationTest
import no.carmarketplace.ingest.support.eventJson
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

// End to end through Kafka: events published the way Debezium publishes them (key = vehicle id,
// value = event JSON, header eventType) are consumed from each lane topic and land in both indexes.
class LaneListenersTest : WorkerIntegrationTest() {

    @Test
    fun `events on every lane topic reach Elasticsearch and Qdrant`() {
        val fromChanges: UUID = UUID.randomUUID()
        val fromBulk: UUID = UUID.randomUUID()

        publish("vehicle.changes", fromChanges, eventJson(fromChanges), "VehicleCreated")
        publish("vehicle.bulk", fromBulk, eventJson(fromBulk), "VehicleCreated")
        await().atMost(TIMEOUT).untilAsserted {
            assertNotNull(point(fromChanges))
            assertNotNull(point(fromBulk))
        }

        publish("vehicle.priority", fromChanges, eventJson(fromChanges, status = "SOLD", version = 1), "VehicleSold")
        await().atMost(TIMEOUT).untilAsserted {
            assertEquals("SOLD", document(fromChanges)?.get("status"))
            assertNull(point(fromChanges))
        }

        // Freshness is measured per lane and event type: the "sold within 2 s" panel reads this
        assertNotNull(meterRegistry.find("ingest.freshness").tags("lane", "priority", "type", "VehicleSold").timer())
    }

    private fun publish(topic: String, key: UUID, json: String, type: String) {
        val config: Map<String, Any> = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to TestEngines.kafka.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        )
        KafkaProducer<String, String>(config).use { producer: KafkaProducer<String, String> ->
            producer.send(ProducerRecord(topic, null, key.toString(), json, listOf(RecordHeader("eventType", type.toByteArray())))).get()
        }
    }

    companion object {
        // Includes the consumer groups joining on the first test
        private val TIMEOUT: Duration = Duration.ofSeconds(60)
    }
}
