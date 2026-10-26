package no.carmarketplace.ingest.search

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.VersionType
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping
import co.elastic.clients.elasticsearch.core.BulkResponse
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem
import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant
import java.util.UUID
import no.carmarketplace.ingest.VehicleEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component

// The lexical (BM25) side of hybrid search: the Elasticsearch index "vehicles".
// It also guards ordering: each document is written with the event's version as an external version.
// ExternalGte: Elasticsearch rejects an *older* version, but accepts the *same* version again. That matters:
// Kafka delivers at least once, so after a crash the same event comes back, and it must be processed again
// (e.g. its Qdrant vector may never have been written). Plain External would call that duplicate "stale".
@Component
class VehicleIndex(private val client: ElasticsearchClient) : ApplicationRunner {

    private val log: Logger = LoggerFactory.getLogger(VehicleIndex::class.java)

    // On startup: create the index with our mapping if it doesn't exist yet.
    override fun run(args: ApplicationArguments) {
        if (client.indices().exists { e -> e.index(INDEX) }.value()) return
        client.indices().create { c ->
            c.index(INDEX)
                .settings { s ->
                    s.numberOfShards("1") // one node: one shard
                        .numberOfReplicas("0") // a replica could never be placed on a single node
                        .refreshInterval { t -> t.time("1s") } // new documents become searchable within 1 s
                }
                .mappings { m ->
                    m.dynamic(DynamicMapping.Strict) // unknown fields are an error, not a silent new mapping
                        .properties("title") { p -> p.text { t -> t.analyzer("english") } }
                        .properties("description") { p -> p.text { t -> t.analyzer("english") } } // "cars" matches "car"
                        .properties("manufacturer") { p -> p.keyword { k -> k.fields("text") { f -> f.text { t -> t } } } }
                        .properties("model") { p -> p.keyword { k -> k.fields("text") { f -> f.text { t -> t } } } }
                        .properties("price") { p -> p.integer { i -> i } }
                        .properties("year") { p -> p.integer { i -> i } }
                        .properties("location") { p -> p.geoPoint { g -> g.ignoreMalformed(true) } } // some CSV coordinates are invalid
                        .properties("status") { p -> p.keyword { k -> k } }
                        .properties("occurred_at") { p -> p.date { d -> d } }
                }
        }
        log.info("Created Elasticsearch index '{}'", INDEX)
    }

    // One bulk request for the whole batch. Returns the ids Elasticsearch accepted;
    // the others were stale (an older version than what the index already has) or failed.
    fun write(events: List<VehicleEvent>): WriteResult {
        if (events.isEmpty()) return WriteResult(emptySet(), 0, 0)
        val response: BulkResponse = client.bulk { b ->
            for (event: VehicleEvent in events) {
                b.operations { op ->
                    op.index<VehicleDocument> { i ->
                        i.index(INDEX)
                            .id(event.id.toString())
                            .version(event.version)
                            .versionType(VersionType.ExternalGte)
                            .document(VehicleDocument.from(event))
                    }
                }
            }
            b
        }
        val accepted: MutableSet<UUID> = HashSet()
        var stale: Int = 0
        var failed: Int = 0
        for (item: BulkResponseItem in response.items()) {
            when {
                item.error() == null -> accepted.add(UUID.fromString(item.id()))
                item.status() == 409 -> stale++ // version conflict: a newer version is already indexed
                else -> {
                    failed++
                    log.warn("Indexing {} failed: {}", item.id(), item.error()?.reason())
                }
            }
        }
        return WriteResult(accepted, stale, failed)
    }

    data class WriteResult(val accepted: Set<UUID>, val stale: Int, val failed: Int)

    companion object {
        const val INDEX: String = "vehicles"
    }
}

// What Elasticsearch stores: the search document, shaped for search rather than copied from the table.
data class VehicleDocument(
    val title: String,
    val description: String?,
    val manufacturer: String?,
    val model: String?,
    val price: Int?,
    val year: Int?,
    val location: Location?,
    val status: String,
    @JsonProperty("occurred_at") val occurredAt: Instant,
) {
    data class Location(val lat: Double, val lon: Double)

    companion object {
        fun from(event: VehicleEvent): VehicleDocument = VehicleDocument(
            title = event.title,
            description = event.description,
            manufacturer = event.manufacturer,
            model = event.model,
            price = event.price,
            year = event.year,
            location = if (event.lat != null && event.lon != null) Location(event.lat, event.lon) else null,
            status = event.status,
            occurredAt = event.occurredAt,
        )
    }
}
