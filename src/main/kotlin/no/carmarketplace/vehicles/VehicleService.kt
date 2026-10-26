package no.carmarketplace.vehicles

import java.time.Instant
import java.util.UUID
import no.carmarketplace.outbox.EventLane
import no.carmarketplace.outbox.OutboxPublisher
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.Sort
import org.springframework.data.jdbc.core.JdbcAggregateTemplate
import org.springframework.data.relational.core.query.Query
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException

@Service
class VehicleService(
    private val repository: VehicleRepository,
    private val template: JdbcAggregateTemplate,
    private val outbox: OutboxPublisher,
    // false = "before" mode for the experiment: sold events share the bulk lane and wait behind it
    @Value($$"${events.priority-lane-enabled:true}") private val priorityLaneEnabled: Boolean,
) {

    // Both rows (vehicle + outbox event) are saved, or neither is.
    // Without the transaction, a crash between the two inserts would leave a car Kafka never hears about.
    @Transactional
    fun create(vehicle: Vehicle, lane: EventLane = EventLane.CHANGES): Vehicle {
        val saved: Vehicle = repository.save(vehicle)
        publish(saved, "VehicleCreated", lane)
        return saved
    }

    @Transactional
    fun markSold(id: UUID): Vehicle {
        val vehicle: Vehicle = repository.findById(id)
            .orElseThrow { ResponseStatusException(HttpStatus.NOT_FOUND, "No vehicle $id") }
        if (vehicle.status == VehicleStatus.SOLD) return vehicle // already sold: nothing new to tell

        val sold: Vehicle = repository.save(vehicle.copy(status = VehicleStatus.SOLD)) // version + 1
        val lane: EventLane = if (priorityLaneEnabled) EventLane.PRIORITY else EventLane.BULK
        publish(sold, "VehicleSold", lane)
        return sold
    }

    fun findPage(offset: Long, size: Int): List<Vehicle> {
        val query: Query = Query.empty()
            .sort(Sort.by("id"))
            .offset(offset)
            .limit(size)
        return template.findAll(query, Vehicle::class.java)
    }

    fun count(): Long {
        return repository.count()
    }

    private fun publish(vehicle: Vehicle, type: String, lane: EventLane) {
        val now: Instant = Instant.now()
        outbox.publish(
            lane = lane,
            aggregateType = "vehicle",
            aggregateId = requireNotNull(vehicle.id),
            type = type,
            payload = VehicleEvent.from(vehicle, occurredAt = now),
            at = now,
        )
    }
}
