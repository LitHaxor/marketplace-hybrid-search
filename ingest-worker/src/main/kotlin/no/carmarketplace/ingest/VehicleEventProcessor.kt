package no.carmarketplace.ingest

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.time.Instant
import no.carmarketplace.embedding.TextEmbedder
import no.carmarketplace.ingest.search.VehicleIndex
import no.carmarketplace.ingest.search.VehicleVectors
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper

// Turns a batch of events from one lane into search-index updates.
@Component
class VehicleEventProcessor(
    private val jsonMapper: JsonMapper,
    private val index: VehicleIndex,
    private val vectors: VehicleVectors,
    private val embedder: TextEmbedder,
    private val meterRegistry: MeterRegistry,
) {

    private val log: Logger = LoggerFactory.getLogger(VehicleEventProcessor::class.java)

    private data class Received(val type: String, val event: VehicleEvent)

    fun process(lane: String, records: List<ConsumerRecord<String, String>>) {
        // Invalid messages are dropped one by one. Throwing here would fail the whole batch, and the error
        // handler would then skip the valid events in it too.
        val received: List<Received> = records.mapNotNull { record: ConsumerRecord<String, String> -> parse(record) }
        count(lane, "invalid", records.size - received.size)

        // 1. Elasticsearch decides, by version, which events are current. Stale ones stop here,
        //    so a late "created" can't bring back a car that was already sold.
        val written: VehicleIndex.WriteResult = index.write(received.map { r: Received -> r.event })
        val current: List<Received> = received.filter { r: Received -> r.event.id in written.accepted }

        // 2. Qdrant: active cars get a vector (the slow part: running the model), sold cars are removed.
        val active: List<VehicleEvent> = current.map { r: Received -> r.event }.filter { e: VehicleEvent -> e.status == "ACTIVE" }
        if (active.isNotEmpty()) {
            val embeddings: List<FloatArray> = embeddingTimer(lane).recordCallable {
                embedder.embed(active.map { e: VehicleEvent -> "${e.title}. ${e.description.orEmpty()}" })
            }!!
            vectors.upsert(active, embeddings)
        }
        vectors.delete(current.filter { r: Received -> r.event.status == "SOLD" }.map { r: Received -> r.event.id })

        // 3. Metrics. Freshness = from the database commit until both indexes have the change
        //    (Elasticsearch shows it in search after its next refresh, at most 1 s later).
        val now: Instant = Instant.now()
        for (r: Received in current) {
            freshnessTimer(lane, r.type).record(Duration.between(r.event.occurredAt, now))
        }
        count(lane, "indexed", current.size)
        count(lane, "stale", written.stale)
        count(lane, "failed", written.failed)
    }

    // null = not a valid vehicle event (bad JSON, missing fields): it would never succeed, so it's skipped
    private fun parse(record: ConsumerRecord<String, String>): Received? =
        try {
            Received(
                type = record.headers().lastHeader("eventType")?.value()?.decodeToString() ?: "unknown",
                event = jsonMapper.readValue(record.value(), VehicleEvent::class.java),
            )
        } catch (e: JacksonException) {
            log.warn("Skipping invalid event {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), e.originalMessage)
            null
        }

    // ingest_freshness_seconds{lane, type}: a histogram, so Grafana can show p95 and "% within 2 s"
    private fun freshnessTimer(lane: String, type: String): Timer =
        Timer.builder("ingest.freshness")
            .tags("lane", lane, "type", type)
            .serviceLevelObjectives(
                Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5),
                Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofMinutes(30),
            )
            .register(meterRegistry)

    // ingest_embedding_seconds{lane}: how long the model takes per batch
    private fun embeddingTimer(lane: String): Timer =
        Timer.builder("ingest.embedding").tags("lane", lane).register(meterRegistry)

    // ingest_events_total{lane, result}
    private fun count(lane: String, result: String, amount: Int) {
        if (amount > 0) meterRegistry.counter("ingest.events", "lane", lane, "result", result).increment(amount.toDouble())
    }
}
