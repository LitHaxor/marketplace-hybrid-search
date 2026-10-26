package no.carmarketplace.ingest.support

import java.time.Instant
import java.util.UUID
import org.apache.kafka.clients.consumer.ConsumerRecord

// The JSON the API writes to its outbox, which Debezium forwards to Kafka unchanged (expand.json.payload).
// Same fields as the API's VehicleEvent: the API's VehicleApiTest checks its side of this contract.
fun eventJson(
    id: UUID,
    status: String = "ACTIVE",
    version: Long = 0,
    occurredAt: Instant = Instant.now(),
): String = """
    {"id": "$id", "price": 12500, "year": 2016, "manufacturer": "toyota", "model": "rav4 xle",
     "description": "One owner, full service history", "lat": 40.73, "lon": -73.99,
     "status": "$status", "version": $version, "occurredAt": "$occurredAt"}
""".trimIndent()

// A record as the listener receives it, with Debezium's eventType header
fun record(topic: String, value: String, type: String = "VehicleCreated"): ConsumerRecord<String, String> =
    ConsumerRecord<String, String>(topic, 0, 0L, null, value).apply { headers().add("eventType", type.toByteArray()) }
