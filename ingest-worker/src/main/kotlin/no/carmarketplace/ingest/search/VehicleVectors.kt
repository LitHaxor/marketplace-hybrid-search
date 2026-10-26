package no.carmarketplace.ingest.search

import io.qdrant.client.PointIdFactory.id
import io.qdrant.client.QdrantClient
import io.qdrant.client.QdrantGrpcClient
import io.qdrant.client.ValueFactory.value
import io.qdrant.client.VectorsFactory.vectors
import io.qdrant.client.grpc.Collections.CreateCollection
import io.qdrant.client.grpc.Collections.Distance
import io.qdrant.client.grpc.Collections.HnswConfigDiff
import io.qdrant.client.grpc.Collections.PayloadSchemaType
import io.qdrant.client.grpc.Collections.QuantizationConfig
import io.qdrant.client.grpc.Collections.QuantizationType
import io.qdrant.client.grpc.Collections.ScalarQuantization
import io.qdrant.client.grpc.Collections.VectorParams
import io.qdrant.client.grpc.Collections.VectorsConfig
import io.qdrant.client.grpc.JsonWithInt.Value
import io.qdrant.client.grpc.Common.PointId
import io.qdrant.client.grpc.Points.PointStruct
import java.util.UUID
import no.carmarketplace.ingest.VehicleEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value as Property
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component

// The semantic side of hybrid search: the Qdrant collection "vehicles". Only ACTIVE cars live here.
@Component
class VehicleVectors(
    @Property($$"${qdrant.host:localhost}") host: String,
    @Property($$"${qdrant.port:6334}") port: Int,
) : ApplicationRunner, AutoCloseable {

    private val log: Logger = LoggerFactory.getLogger(VehicleVectors::class.java)
    private val client: QdrantClient = QdrantClient(QdrantGrpcClient.newBuilder(host, port, false).build())

    // On startup: create the collection and its payload indexes if they don't exist yet.
    override fun run(args: ApplicationArguments) {
        if (client.collectionExistsAsync(COLLECTION).get()) return
        val collection: CreateCollection = CreateCollection.newBuilder()
            .setCollectionName(COLLECTION)
            .setVectorsConfig(
                VectorsConfig.newBuilder().setParams(
                    VectorParams.newBuilder().setSize(DIMENSIONS).setDistance(Distance.Cosine),
                ),
            )
            .setHnswConfig(HnswConfigDiff.newBuilder().setM(16).setEfConstruct(100)) // the search graph
            .setQuantizationConfig(
                QuantizationConfig.newBuilder().setScalar(
                    // 1 byte per number instead of 4: ~4x less memory, originals kept on disk for rescoring
                    ScalarQuantization.newBuilder().setType(QuantizationType.Int8).setAlwaysRam(true),
                ),
            )
            .build()
        client.createCollectionAsync(collection).get()

        // Payload indexes make filters fast during vector search: "similar cars, under 10k, nearby"
        mapOf(
            "manufacturer" to PayloadSchemaType.Keyword,
            "price" to PayloadSchemaType.Integer,
            "year" to PayloadSchemaType.Integer,
            "location" to PayloadSchemaType.Geo,
        ).forEach { (field: String, type: PayloadSchemaType) ->
            client.createPayloadIndexAsync(COLLECTION, field, type, null, true, null, null).get()
        }
        log.info("Created Qdrant collection '{}' ({} dimensions, cosine, int8)", COLLECTION, DIMENSIONS)
    }

    fun upsert(events: List<VehicleEvent>, embeddings: List<FloatArray>) {
        if (events.isEmpty()) return
        val points: List<PointStruct> = events.zip(embeddings).map { (event: VehicleEvent, vector: FloatArray) ->
            PointStruct.newBuilder()
                .setId(id(event.id)) // the vehicle's UUID: upserting the same car twice just overwrites it
                .setVectors(vectors(*vector))
                .putAllPayload(payload(event))
                .build()
        }
        client.upsertAsync(COLLECTION, points).get()
    }

    // Sold cars leave vector search completely
    fun delete(ids: List<UUID>) {
        if (ids.isEmpty()) return
        val points: List<PointId> = ids.map { uuid: UUID -> id(uuid) }
        client.deleteAsync(COLLECTION, points).get()
    }

    private fun payload(event: VehicleEvent): Map<String, Value> {
        val fields: MutableMap<String, Value> = mutableMapOf("title" to value(event.title))
        event.manufacturer?.let { fields["manufacturer"] = value(it) }
        event.price?.let { fields["price"] = value(it.toLong()) }
        event.year?.let { fields["year"] = value(it.toLong()) }
        if (event.lat != null && event.lon != null) {
            fields["location"] = value(mapOf("lat" to value(event.lat), "lon" to value(event.lon)))
        }
        return fields
    }

    override fun close() {
        client.close()
    }

    companion object {
        const val COLLECTION: String = "vehicles"
        const val DIMENSIONS: Long = 384 // all-MiniLM-L6-v2's vector size
    }
}
