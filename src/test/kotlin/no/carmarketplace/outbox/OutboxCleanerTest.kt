package no.carmarketplace.outbox

import java.time.Duration
import java.time.Instant
import java.util.UUID
import no.carmarketplace.support.IntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.Test
import kotlin.test.assertEquals

class OutboxCleanerTest : IntegrationTest() {

    @Autowired
    private lateinit var cleaner: OutboxCleaner

    @Test
    fun `deletes events older than 5 minutes, more than one batch of them, and keeps recent ones`() {
        val now: Instant = Instant.now()
        val old: List<OutboxEvent> = (1..1_500).map { event(createdAt = now.minus(Duration.ofMinutes(10))) } // batches are 1,000
        val recent: List<OutboxEvent> = (1..3).map { event(createdAt = now.minus(Duration.ofMinutes(1))) }
        template.insertAll(old + recent)

        cleaner.deleteOldEvents()

        val left: Set<UUID> = template.findAll(OutboxEvent::class.java).map { e: OutboxEvent -> e.id }.toSet()
        assertEquals(recent.map { e: OutboxEvent -> e.id }.toSet(), left)
    }

    private fun event(createdAt: Instant): OutboxEvent = OutboxEvent(
        id = UUID.randomUUID(),
        topic = EventLane.CHANGES.topic,
        aggregateType = "vehicle",
        aggregateId = UUID.randomUUID().toString(),
        type = "VehicleCreated",
        payload = "{}",
        createdAt = createdAt,
    )
}
