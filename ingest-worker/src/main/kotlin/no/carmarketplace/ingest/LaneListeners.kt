package no.carmarketplace.ingest

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

// One Kafka consumer group per lane. Each has its own threads and its own position in its own topic,
// so a backlog of 400,000 bulk events can't delay a "sold" event.
@Component
class LaneListeners(private val processor: VehicleEventProcessor) {

    // Sold/deleted: small batches, short waits, nothing to embed. Built for latency.
    @KafkaListener(
        id = "priority",
        topics = ["vehicle.priority"],
        groupId = "ingest-priority",
        concurrency = "1",
        batch = "true",
        properties = ["max.poll.records=50", "fetch.max.wait.ms=50"],
    )
    fun priority(records: List<ConsumerRecord<String, String>>) {
        processor.process("priority", records)
    }

    // Users creating or editing ads: one consumer thread per partition (the topic has 3).
    @KafkaListener(
        id = "changes",
        topics = ["vehicle.changes"],
        groupId = "ingest-changes",
        concurrency = "3",
        batch = "true",
        properties = ["max.poll.records=32"],
    )
    fun changes(records: List<ConsumerRecord<String, String>>) {
        processor.process("changes", records)
    }

    // Seed, imports, backfills: bigger batches = fewer model calls. Built for throughput.
    @KafkaListener(
        id = "bulk",
        topics = ["vehicle.bulk"],
        groupId = "ingest-bulk",
        concurrency = "3",
        batch = "true",
        properties = ["max.poll.records=64"],
    )
    fun bulk(records: List<ConsumerRecord<String, String>>) {
        processor.process("bulk", records)
    }
}
