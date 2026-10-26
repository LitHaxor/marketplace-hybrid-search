package no.carmarketplace.outbox

import java.time.Instant
import java.util.UUID
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table

// One row = one event for Kafka. Debezium reads new rows from Postgres's change log (WAL)
// and publishes them; the app never talks to Kafka itself.
@Table("outbox")
data class OutboxEvent(
    @Id
    @Column("id")
    val id: UUID,

    // Which Kafka topic (lane) the event goes to. Debezium routes on this column.
    @Column("topic")
    val topic: String,

    // What changed, e.g. "vehicle"
    @Column("aggregate_type")
    val aggregateType: String,

    // Which one: the vehicle id. Becomes the Kafka message key, so one vehicle's events stay in order.
    @Column("aggregate_id")
    val aggregateId: String,

    // What happened, e.g. "VehicleCreated". Becomes the Kafka header "eventType".
    @Column("type")
    val type: String,

    // The event itself, as JSON
    @Column("payload")
    val payload: String,

    @Column("created_at")
    val createdAt: Instant,
)
