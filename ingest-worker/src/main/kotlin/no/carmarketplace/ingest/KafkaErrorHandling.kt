package no.carmarketplace.ingest

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.ExponentialBackOff
import tools.jackson.core.JacksonException

// What happens when a batch fails.
// Spring Kafka's default retries 10 times with no pause, then SKIPS the batch: a 30-second Elasticsearch
// restart would silently drop events. For a search index that's wrong, so:
@Configuration
class KafkaErrorHandling {

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        // Elasticsearch or Qdrant down: retry the same batch after 1 s, 2 s, 4 s, ... at most 30 s apart,
        // with no limit. Kafka keeps the events, so nothing is lost; the lane waits and its lag grows.
        val backOff: ExponentialBackOff = ExponentialBackOff(1_000, 2.0).apply { maxInterval = 30_000 }
        return DefaultErrorHandler(backOff).apply {
            // Invalid messages are skipped one by one in VehicleEventProcessor. If a JSON error still gets here,
            // retrying can't fix it: skip the batch instead of blocking the lane forever.
            addNotRetryableExceptions(JacksonException::class.java)
        }
    }
}
