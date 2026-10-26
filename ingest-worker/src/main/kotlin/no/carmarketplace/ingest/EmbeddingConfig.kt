package no.carmarketplace.ingest

import no.carmarketplace.embedding.TextEmbedder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class EmbeddingConfig {

    // Batches of car texts. Bounded calls and sizes: the model's memory is native, outside the heap.
    @Bean(destroyMethod = "close")
    fun textEmbedder(
        @Value($$"${embedding.model-dir}") modelDir: String,
        @Value($$"${embedding.max-tokens:128}") maxTokens: Int,
        @Value($$"${embedding.threads:2}") threads: Int,
        @Value($$"${embedding.max-concurrent-calls:2}") maxConcurrentCalls: Int,
        @Value($$"${embedding.batch-size:16}") batchSize: Int,
    ): TextEmbedder = TextEmbedder(modelDir, maxTokens, threads, maxConcurrentCalls, batchSize)
}
