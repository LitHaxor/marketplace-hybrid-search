package no.carmarketplace.vehicles

import java.util.UUID
import org.springframework.data.repository.ListCrudRepository
import org.springframework.data.repository.ListPagingAndSortingRepository

interface VehicleRepository : ListCrudRepository<Vehicle, UUID>, ListPagingAndSortingRepository<Vehicle, UUID> {

    // Spring writes the query from the name: SELECT ... WHERE source_id = ? (uses the unique index)
    fun existsBySourceId(sourceId: Long): Boolean
}
