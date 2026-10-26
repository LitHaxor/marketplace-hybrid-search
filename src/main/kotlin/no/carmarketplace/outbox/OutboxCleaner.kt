package no.carmarketplace.outbox

import java.time.Duration
import java.time.Instant
import java.util.UUID
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.jdbc.core.JdbcAggregateTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// Outbox rows are only needed until Debezium has read them from the WAL (usually within a second).
// Without this, the table grows by ~1 GB per full seed. Debezium ignores these deletes (skipped.operations).
@Component
class OutboxCleaner(private val template: JdbcAggregateTemplate) {

    private val log: Logger = LoggerFactory.getLogger(OutboxCleaner::class.java)

    @Scheduled(fixedDelay = 60_000) // every minute
    fun deleteOldEvents() {
        val cutoff: Instant = Instant.now().minus(Duration.ofMinutes(5))
        var deleted: Int = 0
        // In batches of 1,000, so one run never loads a huge result into memory
        while (true) {
            val old: Query = Query.query(Criteria.where("createdAt").lessThan(cutoff)).limit(BATCH)
            val ids: List<UUID> = template.findAll(old, OutboxEvent::class.java).map { e: OutboxEvent -> e.id }
            if (ids.isEmpty()) break
            template.deleteAllById(ids, OutboxEvent::class.java)
            deleted += ids.size
        }
        if (deleted > 0) log.info("Deleted {} outbox events older than {}", deleted, cutoff)
    }

    companion object {
        private const val BATCH: Int = 1_000
    }
}
