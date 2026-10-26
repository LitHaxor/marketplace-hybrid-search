package no.carmarketplace.search

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch.core.MgetResponse
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem
import co.elastic.clients.elasticsearch.core.search.Hit
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qdrant.client.ConditionFactory
import io.qdrant.client.QdrantClient
import io.qdrant.client.WithPayloadSelectorFactory
import io.qdrant.client.grpc.Common.Condition
import io.qdrant.client.grpc.Common.Filter
import io.qdrant.client.grpc.Common.Range
import io.qdrant.client.grpc.Points.ScoredPoint
import io.qdrant.client.grpc.Points.SearchPoints
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import no.carmarketplace.embedding.TextEmbedder
import org.springframework.stereotype.Service

// Hybrid search: the same question asked two ways, then the answers merged.
//   BM25 (Elasticsearch): matching words. Great for "toyota camry 2015", weak for "cheap car for a student".
//   Vectors (Qdrant):     matching meaning. Great for "cheap car for a student", fuzzy on exact model names.
//   RRF:                  merge both ranked lists by position, so neither score scale has to be trusted.
@Service
class SearchService(
    private val elasticsearch: ElasticsearchClient,
    private val qdrant: QdrantClient,
    private val embedder: TextEmbedder,
    private val meterRegistry: MeterRegistry,
) {

    fun search(request: SearchRequest): SearchResult {
        val timings: MutableMap<String, Long> = linkedMapOf()
        val total: Long = System.nanoTime()

        // BM25 and vector search don't depend on each other: run them at the same time, each on its own
        // virtual thread. The request takes as long as the slower of the two, not the sum.
        val (lexical: List<String>, semantic: List<String>) =
            Executors.newVirtualThreadPerTaskExecutor().use { executor: ExecutorService ->
                val bm25: Future<List<String>> = executor.submit<List<String>> {
                    if (request.mode == SearchMode.VECTOR || request.text.isBlank()) emptyList()
                    else timed("bm25", timings) { bm25(request) }
                }
                val vector: Future<List<String>> = executor.submit<List<String>> {
                    if (request.mode == SearchMode.BM25 || request.text.isBlank()) emptyList()
                    else {
                        val queryVector: FloatArray = timed("embed", timings) { embedder.embed(listOf(request.text)).first() }
                        timed("vector", timings) { vectorSearch(queryVector, request) }
                    }
                }
                bm25.get() to vector.get()
            }

        // No text at all (only filters): just list matching cars from Elasticsearch
        val ranked: List<Ranked> = timed("fusion", timings) {
            if (request.text.isBlank()) filterOnly(request).mapIndexed { i: Int, id: String -> Ranked(id, 1.0 / (RRF_K + i + 1), setOf("filter")) }
            else reciprocalRankFusion(lexical, semantic).take(request.size)
        }
        val cars: Map<String, Map<String, Any?>> = timed("hydrate", timings) { hydrate(ranked.map { r: Ranked -> r.id }) }

        timings["total"] = (System.nanoTime() - total) / 1_000_000
        stageTimer("total").record(System.nanoTime() - total, java.util.concurrent.TimeUnit.NANOSECONDS)
        return SearchResult(
            query = request.text,
            mode = request.mode,
            results = ranked.mapNotNull { r: Ranked -> cars[r.id]?.let { car -> SearchHit.from(r, car) } },
            candidates = mapOf("bm25" to lexical.size, "vector" to semantic.size),
            timingsMs = timings,
        )
    }

    private fun bm25(request: SearchRequest): List<String> {
        val response: SearchResponse<Void> = elasticsearch.search({ s ->
            s.index(INDEX).size(CANDIDATES).source { src -> src.fetch(false) }
                .query { q ->
                    q.bool { b: BoolQuery.Builder ->
                        b.must { m ->
                            m.multiMatch { mm ->
                                // title counts 3x, make/model 2x: a word in the title says more than one in a long text
                                mm.query(request.text).fields("title^3", "manufacturer.text^2", "model.text^2", "description")
                            }
                        }
                        filters(b, request)
                    }
                }
        }, Void::class.java)
        return response.hits().hits().map { h: Hit<Void> -> h.id()!! }
    }

    private fun filterOnly(request: SearchRequest): List<String> {
        val response: SearchResponse<Void> = elasticsearch.search({ s ->
            s.index(INDEX).size(request.size).source { src -> src.fetch(false) }
                .query { q -> q.bool { b: BoolQuery.Builder -> filters(b, request) } }
        }, Void::class.java)
        return response.hits().hits().map { h: Hit<Void> -> h.id()!! }
    }

    // Filters don't change the score, they only remove cars (and Elasticsearch can cache them)
    private fun filters(b: BoolQuery.Builder, request: SearchRequest): BoolQuery.Builder {
        b.filter { f -> f.term { t -> t.field("status").value("ACTIVE") } }
        request.maxPrice?.let { p: Int -> b.filter { f -> f.range { r -> r.number { n -> n.field("price").lte(p.toDouble()) } } } }
        request.minYear?.let { y: Int -> b.filter { f -> f.range { r -> r.number { n -> n.field("year").gte(y.toDouble()) } } } }
        request.manufacturer?.let { m: String -> b.filter { f -> f.term { t -> t.field("manufacturer").value(m.lowercase()) } } }
        if (request.lat != null && request.lon != null && request.radiusKm != null) {
            b.filter { f ->
                f.geoDistance { g ->
                    g.field("location").distance("${request.radiusKm}km")
                        .location { l -> l.latlon { ll -> ll.lat(request.lat).lon(request.lon) } }
                }
            }
        }
        return b
    }

    private fun vectorSearch(vector: FloatArray, request: SearchRequest): List<String> {
        // The same filters, as Qdrant conditions on the payload (only ACTIVE cars are in Qdrant at all)
        val must: MutableList<Condition> = mutableListOf()
        request.maxPrice?.let { p: Int -> must.add(ConditionFactory.range("price", Range.newBuilder().setLte(p.toDouble()).build())) }
        request.minYear?.let { y: Int -> must.add(ConditionFactory.range("year", Range.newBuilder().setGte(y.toDouble()).build())) }
        request.manufacturer?.let { m: String -> must.add(ConditionFactory.matchKeyword("manufacturer", m.lowercase())) }
        if (request.lat != null && request.lon != null && request.radiusKm != null) {
            must.add(ConditionFactory.geoRadius("location", request.lat, request.lon, (request.radiusKm * 1000).toFloat()))
        }
        val points: List<ScoredPoint> = qdrant.searchAsync(
            SearchPoints.newBuilder()
                .setCollectionName(COLLECTION)
                .addAllVector(vector.toList())
                .setFilter(Filter.newBuilder().addAllMust(must))
                .setLimit(CANDIDATES.toLong())
                .setWithPayload(WithPayloadSelectorFactory.enable(false))
                .build(),
        ).get()
        return points.map { p: ScoredPoint -> p.id.uuid }
    }

    // RRF: each list votes 1 / (60 + rank). A car ranked high in both lists beats one ranked high in only one.
    // Positions, not scores: BM25 scores (0..30) and cosine similarities (0..1) can't be compared directly.
    private fun reciprocalRankFusion(lexical: List<String>, semantic: List<String>): List<Ranked> {
        val scores: MutableMap<String, Double> = HashMap()
        val sources: MutableMap<String, MutableSet<String>> = HashMap()
        lexical.forEachIndexed { rank: Int, id: String ->
            scores.merge(id, 1.0 / (RRF_K + rank + 1), Double::plus)
            sources.getOrPut(id) { mutableSetOf() }.add("bm25")
        }
        semantic.forEachIndexed { rank: Int, id: String ->
            scores.merge(id, 1.0 / (RRF_K + rank + 1), Double::plus)
            sources.getOrPut(id) { mutableSetOf() }.add("vector")
        }
        return scores.entries
            .sortedByDescending { e: Map.Entry<String, Double> -> e.value }
            .map { e: Map.Entry<String, Double> -> Ranked(e.key, e.value, sources.getValue(e.key)) }
    }

    // The final page's details, in one request
    private fun hydrate(ids: List<String>): Map<String, Map<String, Any?>> {
        if (ids.isEmpty()) return emptyMap()
        @Suppress("UNCHECKED_CAST")
        val response: MgetResponse<Map<*, *>> = elasticsearch.mget({ m ->
            m.index(INDEX).ids(ids).sourceIncludes(listOf("title", "price", "year", "manufacturer", "model", "location"))
        }, Map::class.java as Class<Map<*, *>>)
        return response.docs().mapNotNull { item: MultiGetResponseItem<Map<*, *>> ->
            val result = item.result()
            @Suppress("UNCHECKED_CAST")
            if (result.found()) result.id() to (result.source() as Map<String, Any?>) else null
        }.toMap()
    }

    // search_stage_seconds{stage}: a histogram per stage, so Grafana shows where the time goes under load
    private fun <T> timed(stage: String, timings: MutableMap<String, Long>, block: () -> T): T {
        val start: Long = System.nanoTime()
        try {
            return block()
        } finally {
            val elapsed: Long = System.nanoTime() - start
            stageTimer(stage).record(elapsed, java.util.concurrent.TimeUnit.NANOSECONDS)
            synchronized(timings) { timings[stage] = elapsed / 1_000_000 }
        }
    }

    private fun stageTimer(stage: String): Timer =
        Timer.builder("search.stage").tag("stage", stage).publishPercentileHistogram().register(meterRegistry)

    data class Ranked(val id: String, val score: Double, val sources: Set<String>)

    companion object {
        private const val INDEX: String = "vehicles"
        private const val COLLECTION: String = "vehicles"
        private const val CANDIDATES: Int = 50 // per list, before fusion
        private const val RRF_K: Double = 60.0 // the usual constant: dampens the difference between rank 1 and 2
    }
}
