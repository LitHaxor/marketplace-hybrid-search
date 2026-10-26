package no.carmarketplace.vehicles

import java.util.UUID
import no.carmarketplace.db.Index
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table

@Table("vehicles")
data class Vehicle(

    @Id
    @Column("id")
    val id: UUID? = null,

    @Column("price")
    val price: Int?,

    @Column("year")
    val year: Int?,

    @Column("manufacturer")
    val manufacturer: String?,

    @Column("model")
    val model: String?,

    @Column("description")
    val description: String?,

    @Column("lat")
    val lat: Double?,

    @Column("lon")
    val lon: Double?,

    @Column("status")
    val status: VehicleStatus = VehicleStatus.ACTIVE,

    // The listing's id in the source data (the CSV's "id" column); null for cars created through the API.
    // Unique, so loading the same file twice can't create duplicates, and an interrupted seed can resume.
    @Index(unique = true)
    @Column("source_id")
    val sourceId: Long? = null,

    // Spring Data raises it on every save (and refuses a save from a stale copy: optimistic locking).
    // Events carry it, so the search index can ignore an older event that arrives after a newer one.
    @Version
    @Column("version")
    val version: Long? = null,
)

enum class VehicleStatus { ACTIVE, SOLD }
