package no.carmarketplace.vehicles

import java.time.Instant
import java.util.UUID
import no.carmarketplace.outbox.OutboxEvent
import no.carmarketplace.outbox.OutboxPublisher
import no.carmarketplace.support.IntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// The write path over HTTP: every change saves the car AND queues its event in the same transaction,
// on the right lane. Debezium and Kafka aren't involved: the outbox row is the API's whole job.
class VehicleApiTest : IntegrationTest() {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    private lateinit var jsonMapper: JsonMapper

    @Autowired
    private lateinit var vehicleService: VehicleService

    @Autowired
    private lateinit var repository: VehicleRepository

    @Autowired
    private lateinit var outbox: OutboxPublisher

    @Test
    fun `creating a car saves it and queues VehicleCreated on the changes lane`() {
        val result: MvcResult = mockMvc.post("/vehicles") {
            contentType = MediaType.APPLICATION_JSON
            content = RAV4_JSON
        }.andExpect {
            status { isCreated() }
            jsonPath("$.id") { exists() }
            jsonPath("$.status") { value("ACTIVE") }
            jsonPath("$.manufacturer") { value("toyota") }
        }.andReturn()
        val created: JsonNode = jsonMapper.readTree(result.response.contentAsString)
        val id: String = created.get("id").asString()

        val event: OutboxEvent = template.findAll(OutboxEvent::class.java).single()
        assertEquals("vehicle.changes", event.topic)
        assertEquals("VehicleCreated", event.type)
        assertEquals("vehicle", event.aggregateType)
        assertEquals(id, event.aggregateId)

        // The payload is the contract with the ingest worker (its VehicleEventJsonTest reads this shape)
        val payload: JsonNode = jsonMapper.readTree(event.payload)
        assertEquals(EVENT_FIELDS, payload.properties().map { field: Map.Entry<String, JsonNode> -> field.key }.toSet())
        assertEquals(id, payload.get("id").asString())
        assertEquals("ACTIVE", payload.get("status").asString())
        assertEquals(created.get("version").asLong(), payload.get("version").asLong())
        Instant.parse(payload.get("occurredAt").asString()) // an ISO-8601 string, not a number
    }

    @Test
    fun `selling a car bumps its version and queues VehicleSold on the priority lane`() {
        val car: Vehicle = vehicleService.create(rav4())
        val soldVersion: Long = car.version!! + 1

        mockMvc.post("/vehicles/${car.id}/sold").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("SOLD") }
            jsonPath("$.version") { value(soldVersion) }
        }

        val event: OutboxEvent = template.findAll(OutboxEvent::class.java).single { e: OutboxEvent -> e.type == "VehicleSold" }
        assertEquals("vehicle.priority", event.topic)
        val payload: JsonNode = jsonMapper.readTree(event.payload)
        assertEquals("SOLD", payload.get("status").asString())
        assertEquals(soldVersion, payload.get("version").asLong()) // the worker orders events by this
    }

    @Test
    fun `selling a car twice changes nothing the second time`() {
        val car: Vehicle = vehicleService.create(rav4())

        mockMvc.post("/vehicles/${car.id}/sold").andExpect { status { isOk() } }
        mockMvc.post("/vehicles/${car.id}/sold").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("SOLD") }
        }

        assertEquals(1, template.findAll(OutboxEvent::class.java).count { e: OutboxEvent -> e.type == "VehicleSold" })
        assertEquals(car.version!! + 1, repository.findById(car.id!!).get().version)
    }

    @Test
    fun `selling an unknown car is a 404 and queues nothing`() {
        mockMvc.post("/vehicles/${UUID.randomUUID()}/sold").andExpect { status { isNotFound() } }

        assertTrue(template.findAll(OutboxEvent::class.java).isEmpty())
    }

    @Test
    fun `a malformed body is a 400 and saves nothing`() {
        mockMvc.post("/vehicles") {
            contentType = MediaType.APPLICATION_JSON
            content = "{not json"
        }.andExpect { status { isBadRequest() } }

        assertEquals(0L, repository.count())
        assertTrue(template.findAll(OutboxEvent::class.java).isEmpty())
    }

    @Test
    fun `cars are listed page by page in id order, with the total`() {
        // Postgres orders UUIDs byte by byte, which is the same as their lowercase text order
        val ids: List<String> = (1..3).map { vehicleService.create(rav4()).id.toString() }.sorted()

        mockMvc.get("/vehicles?offset=1&size=1").andExpect {
            status { isOk() }
            jsonPath("$.total") { value(3) }
            jsonPath("$.offset") { value(1) }
            jsonPath("$.size") { value(1) }
            jsonPath("$.items.length()") { value(1) }
            jsonPath("$.items[0].id") { value(ids[1]) }
        }
    }

    @Test
    fun `out-of-range paging values are clamped`() {
        mockMvc.get("/vehicles?offset=-5&size=1000").andExpect {
            status { isOk() }
            jsonPath("$.offset") { value(0) }
            jsonPath("$.size") { value(100) }
        }
    }

    @Test
    fun `with the priority lane switched off, sold events wait in the bulk lane`() {
        // The "before" setup of the lane experiment (events.priority-lane-enabled=false)
        val beforeLanes: VehicleService = VehicleService(repository, template, outbox, priorityLaneEnabled = false)
        val car: Vehicle = beforeLanes.create(rav4())

        beforeLanes.markSold(car.id!!)

        val event: OutboxEvent = template.findAll(OutboxEvent::class.java).single { e: OutboxEvent -> e.type == "VehicleSold" }
        assertEquals("vehicle.bulk", event.topic)
    }

    private fun rav4(): Vehicle = Vehicle(
        price = 12500, year = 2016, manufacturer = "toyota", model = "rav4 xle",
        description = "One owner, full service history", lat = 40.73, lon = -73.99,
    )

    companion object {
        private val RAV4_JSON: String = """
            {"price": 12500, "year": 2016, "manufacturer": "toyota", "model": "rav4 xle",
             "description": "One owner, full service history", "lat": 40.73, "lon": -73.99}
        """.trimIndent()

        // Every field of VehicleEvent, null or not: the worker relies on all of them being there
        private val EVENT_FIELDS: Set<String> = setOf(
            "id", "price", "year", "manufacturer", "model", "description", "lat", "lon", "status", "version", "occurredAt",
        )
    }
}
