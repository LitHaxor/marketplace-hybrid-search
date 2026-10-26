package no.carmarketplace.vehicles

import no.carmarketplace.vehicles.dto.CreateVehicleRequest
import no.carmarketplace.vehicles.dto.VehicleResponse
import no.carmarketplace.vehicles.dto.VehiclePageResponse
import org.springframework.http.HttpStatus
import java.util.UUID
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/vehicles")
class VehicleController(private val vehicleService: VehicleService) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun createVehicle(@RequestBody request: CreateVehicleRequest): VehicleResponse {
        val saved: Vehicle = vehicleService.create(request.toEntity())
        return VehicleResponse.from(saved)
    }

    // The seller marked the car as sold: it must disappear from search within ~2 s (priority lane)
    @PostMapping("/{id}/sold")
    fun markSold(@PathVariable id: UUID): VehicleResponse {
        return VehicleResponse.from(vehicleService.markSold(id))
    }

    @GetMapping
    fun getVehicles(
        @RequestParam(defaultValue = "0") offset: Long,
        @RequestParam(defaultValue = "20") size: Int,
    ): VehiclePageResponse {
        val safeOffset: Long = offset.coerceAtLeast(0)
        val safeSize: Int = size.coerceIn(1, 100)
        val vehicles: List<Vehicle> = vehicleService.findPage(safeOffset, safeSize)
        return VehiclePageResponse(
            items = vehicles.map(VehicleResponse::from),
            total = vehicleService.count(),
            offset = safeOffset,
            size = safeSize,
        )
    }
}
