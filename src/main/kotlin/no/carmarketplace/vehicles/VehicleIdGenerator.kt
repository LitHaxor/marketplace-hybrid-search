package no.carmarketplace.vehicles

import java.util.UUID
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback
import org.springframework.stereotype.Component

// A new Vehicle has id = null. Right before Spring Data saves it, give it a UUID.
// (null id is also how save() knows it must INSERT instead of UPDATE.)
@Component
class VehicleIdGenerator : BeforeConvertCallback<Vehicle> {
    override fun onBeforeConvert(vehicle: Vehicle): Vehicle {
        if (vehicle.id != null) return vehicle
        return vehicle.copy(id = UUID.randomUUID())
    }
}
