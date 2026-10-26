package no.carmarketplace.vehicles

import java.time.Instant
import java.util.UUID

// The vehicle as Kafka consumers (the ingest worker) see it: always the full current state.
// Its own class, so changing the table or the HTTP API doesn't silently change the event.
data class VehicleEvent(
    val id: UUID,
    val price: Int?,
    val year: Int?,
    val manufacturer: String?,
    val model: String?,
    val description: String?,
    val lat: Double?,
    val lon: Double?,
    val status: VehicleStatus,
    val version: Long, // the index keeps the highest version it has seen and ignores older events
    val occurredAt: Instant, // when the change was committed: the start of the freshness clock
) {
    companion object {
        fun from(vehicle: Vehicle, occurredAt: Instant): VehicleEvent = VehicleEvent(
            id = requireNotNull(vehicle.id) { "a saved vehicle always has an id" },
            price = vehicle.price, year = vehicle.year, manufacturer = vehicle.manufacturer,
            model = vehicle.model, description = vehicle.description, lat = vehicle.lat, lon = vehicle.lon,
            status = vehicle.status,
            version = requireNotNull(vehicle.version) { "a saved vehicle always has a version" },
            occurredAt = occurredAt,
        )
    }
}
