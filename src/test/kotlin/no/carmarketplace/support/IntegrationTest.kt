package no.carmarketplace.support

import no.carmarketplace.outbox.OutboxEvent
import no.carmarketplace.vehicles.Vehicle
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.data.jdbc.core.JdbcAggregateTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

// Base class for API integration tests: the whole Spring app against TestPostgres, with MockMvc for HTTP.
// Every subclass has the same configuration, so Spring starts the app once and reuses it.
// Elasticsearch and Qdrant aren't needed: their clients only connect when a search runs.
@SpringBootTest(properties = ["seed.file=src/test/resources/seed/vehicles-sample.csv"])
@AutoConfigureMockMvc
abstract class IntegrationTest {

    @Autowired
    protected lateinit var template: JdbcAggregateTemplate

    // Each test starts with empty tables
    @BeforeEach
    fun emptyTables() {
        template.deleteAll(OutboxEvent::class.java)
        template.deleteAll(Vehicle::class.java)
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { TestPostgres.container.jdbcUrl }
            registry.add("spring.datasource.username") { TestPostgres.container.username }
            registry.add("spring.datasource.password") { TestPostgres.container.password }
        }
    }
}
