package no.carmarketplace.ingest

import java.time.Instant
import java.util.UUID

// The event as this worker reads it from Kafka: always the full current state of the vehicle.
// The worker keeps its own copy instead of importing the API's class: producer and consumer only share
// the JSON format, so either one can be changed and deployed without rebuilding the other.
data class VehicleEvent(
    val id: UUID,
    val price: Int?,
    val year: Int?,
    val manufacturer: String?,
    val model: String?,
    val description: String?,
    val lat: Double?,
    val lon: Double?,
    val status: String, // ACTIVE or SOLD
    val version: Long,
    val occurredAt: Instant,
) {
    // The CSV has no title, so the search document gets one: "2015 volvo v70"
    val title: String
        get() = listOfNotNull(year?.toString(), manufacturer, model).joinToString(" ")
}
