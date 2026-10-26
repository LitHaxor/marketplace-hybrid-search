package no.carmarketplace.ingest

import java.time.Instant
import java.util.UUID
import no.carmarketplace.ingest.search.VehicleDocument
import no.carmarketplace.ingest.support.eventJson
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

// Unit tests, no Docker: reading the API's event JSON, and shaping it into the search document.
class VehicleEventJsonTest {

    private val jsonMapper: JsonMapper = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()

    @Test
    fun `reads the event JSON the API writes`() {
        val id: UUID = UUID.randomUUID()
        val at: Instant = Instant.parse("2026-10-26T12:00:00.123456Z")

        val event: VehicleEvent = jsonMapper.readValue(eventJson(id, status = "SOLD", version = 3, occurredAt = at), VehicleEvent::class.java)

        assertEquals(
            VehicleEvent(
                id = id, price = 12500, year = 2016, manufacturer = "toyota", model = "rav4 xle",
                description = "One owner, full service history", lat = 40.73, lon = -73.99,
                status = "SOLD", version = 3, occurredAt = at,
            ),
            event,
        )
    }

    @Test
    fun `an event without an id is rejected`() {
        assertFailsWith<JacksonException> {
            jsonMapper.readValue("""{"status": "ACTIVE", "version": 0}""", VehicleEvent::class.java)
        }
    }

    @Test
    fun `the title is year, make and model, skipping missing parts`() {
        assertEquals("2016 toyota rav4 xle", event().title)
        assertEquals("toyota rav4 xle", event(year = null).title)
        assertEquals("2016", event(manufacturer = null, model = null).title)
    }

    @Test
    fun `the search document has a location only when both coordinates exist`() {
        assertEquals(VehicleDocument.Location(40.73, -73.99), VehicleDocument.from(event()).location)
        assertNull(VehicleDocument.from(event(lat = null)).location)
        assertNull(VehicleDocument.from(event(lon = null)).location)
    }

    @Test
    fun `the search document carries status and the fields search filters on`() {
        val document: VehicleDocument = VehicleDocument.from(event())

        assertEquals("ACTIVE", document.status)
        assertEquals(12500, document.price)
        assertEquals(2016, document.year)
        assertEquals("toyota", document.manufacturer)
    }

    private fun event(
        year: Int? = 2016,
        manufacturer: String? = "toyota",
        model: String? = "rav4 xle",
        lat: Double? = 40.73,
        lon: Double? = -73.99,
    ): VehicleEvent = VehicleEvent(
        id = UUID.randomUUID(), price = 12500, year = year, manufacturer = manufacturer, model = model,
        description = null, lat = lat, lon = lon, status = "ACTIVE", version = 0, occurredAt = Instant.now(),
    )
}
