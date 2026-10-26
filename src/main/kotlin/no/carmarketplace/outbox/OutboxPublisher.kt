package no.carmarketplace.outbox

import java.time.Instant
import java.util.UUID
import org.springframework.data.jdbc.core.JdbcAggregateTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper

@Component
class OutboxPublisher(
    private val template: JdbcAggregateTemplate,
    private val jsonMapper: JsonMapper,
) {

    // Call this inside the same transaction as the change it describes.
    fun publish(lane: EventLane, aggregateType: String, aggregateId: UUID, type: String, payload: Any, at: Instant) {
        val event = OutboxEvent(
            id = UUID.randomUUID(),
            topic = lane.topic,
            aggregateType = aggregateType,
            aggregateId = aggregateId.toString(),
            type = type,
            payload = jsonMapper.writeValueAsString(payload),
            createdAt = at,
        )
        // insert(), not save(): the id is already set, and an outbox row is never updated
        template.insert(event)
    }
}
