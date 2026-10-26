package no.carmarketplace.search

import io.qdrant.client.QdrantClient
import io.qdrant.client.QdrantGrpcClient
import no.carmarketplace.embedding.TextEmbedder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SearchConfig {

    // Turns the search text into a vector. One short query at a time; calls are bounded because
    // the model's memory is native (outside the heap) and it competes for CPU with everything else.
    @Bean(destroyMethod = "close")
    fun textEmbedder(
        @Value($$"${embedding.model-dir}") modelDir: String,
        @Value($$"${embedding.max-tokens:128}") maxTokens: Int,
        @Value($$"${embedding.threads:1}") threads: Int,
        @Value($$"${embedding.max-concurrent-calls:4}") maxConcurrentCalls: Int,
    ): TextEmbedder = TextEmbedder(modelDir, maxTokens, threads, maxConcurrentCalls, batchSize = 1)

    @Bean(destroyMethod = "close")
    fun qdrantClient(
        @Value($$"${qdrant.host:localhost}") host: String,
        @Value($$"${qdrant.port:6334}") port: Int,
    ): QdrantClient = QdrantClient(QdrantGrpcClient.newBuilder(host, port, false).build())
}
