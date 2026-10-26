package no.carmarketplace.vehicles.dto

import no.carmarketplace.vehicles.Vehicle
import no.carmarketplace.vehicles.VehicleStatus

import java.util.UUID

// What the client sends. No id: the server decides it.
data class CreateVehicleRequest(
    val price: Int?,
    val year: Int?,
    val manufacturer: String?,
    val model: String?,
    val description: String?,
    val lat: Double?,
    val lon: Double?,
) {
    fun toEntity(): Vehicle = Vehicle(
        price = price, year = year, manufacturer = manufacturer, model = model,
        description = description, lat = lat, lon = lon,
    )
}

// What the client gets back
data class VehicleResponse(
    val id: UUID,
    val price: Int?,
    val year: Int?,
    val manufacturer: String?,
    val model: String?,
    val description: String?,
    val lat: Double?,
    val lon: Double?,
    val status: VehicleStatus,
    val version: Long?,
) {
    companion object {
        fun from(vehicle: Vehicle): VehicleResponse = VehicleResponse(
            id = requireNotNull(vehicle.id) { "a saved vehicle always has an id" },
            price = vehicle.price, year = vehicle.year, manufacturer = vehicle.manufacturer,
            model = vehicle.model, description = vehicle.description, lat = vehicle.lat, lon = vehicle.lon,
            status = vehicle.status, version = vehicle.version,
        )
    }
}

data class VehiclePageResponse(
    val items: List<VehicleResponse>,
    val total: Long,
    val offset: Long,
    val size: Int,
)
