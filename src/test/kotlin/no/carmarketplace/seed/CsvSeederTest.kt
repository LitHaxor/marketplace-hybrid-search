package no.carmarketplace.seed

import java.time.Duration
import no.carmarketplace.outbox.OutboxEvent
import no.carmarketplace.support.IntegrationTest
import no.carmarketplace.vehicles.Vehicle
import no.carmarketplace.vehicles.VehicleRepository
import org.awaitility.Awaitility.await
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

// The seeder reads src/test/resources/seed/vehicles-sample.csv (set in IntegrationTest):
//   7001 a complete row, 7002 mostly empty cells, 7003 cells that aren't numbers
class CsvSeederTest : IntegrationTest() {

    @Autowired
    private lateinit var seeder: CsvSeeder

    @Autowired
    private lateinit var repository: VehicleRepository

    @Test
    fun `rows become cars, and empty or broken cells become null`() {
        val status: SeedStatus = seedAndWait(limit = null)

        assertEquals(3L, status.read)
        assertEquals(3L, status.saved)
        assertEquals(0L, status.failed)
        val cars: Map<Long?, Vehicle> = repository.findAll().associateBy { v: Vehicle -> v.sourceId }

        val complete: Vehicle = cars.getValue(7001L)
        assertEquals(12500, complete.price)
        assertEquals(2016, complete.year) // "2016.0" in the CSV
        assertEquals("toyota", complete.manufacturer)
        assertEquals("rav4 xle", complete.model)
        assertEquals("One owner, full service history", complete.description) // a quoted comma
        assertEquals(40.73, complete.lat)
        assertEquals(-73.99, complete.lon)

        val sparse: Vehicle = cars.getValue(7002L)
        assertEquals(0, sparse.price)
        assertNull(sparse.year)
        assertNull(sparse.model)
        assertNull(sparse.description)
        assertNull(sparse.lat)

        val broken: Vehicle = cars.getValue(7003L)
        assertNull(broken.price) // "abc"
        assertNull(broken.lat) // "not-a-number"
        assertEquals(-74.0, broken.lon)
    }

    @Test
    fun `seeded cars go to the bulk lane`() {
        seedAndWait(limit = null)

        val topics: List<String> = template.findAll(OutboxEvent::class.java).map { e: OutboxEvent -> e.topic }
        assertEquals(3, topics.size)
        assertTrue(topics.all { topic: String -> topic == "vehicle.bulk" })
    }

    @Test
    fun `seeding again skips cars that are already loaded`() {
        seedAndWait(limit = null)

        val again: SeedStatus = seedAndWait(limit = null)

        assertEquals(3L, again.read)
        assertEquals(0L, again.saved)
        assertEquals(3L, again.skipped)
        assertEquals(3L, repository.count())
    }

    @Test
    fun `the limit stops after that many rows`() {
        val status: SeedStatus = seedAndWait(limit = 2)

        assertEquals(2L, status.read)
        assertEquals(2L, repository.count())
    }

    // The seed runs on its own thread: start it, then wait until it's done
    private fun seedAndWait(limit: Long?): SeedStatus {
        seeder.start(limit)
        await().atMost(Duration.ofSeconds(30)).until { !seeder.status().running }
        return seeder.status()
    }
}
