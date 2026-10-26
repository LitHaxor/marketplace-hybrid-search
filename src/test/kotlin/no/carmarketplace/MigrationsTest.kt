package no.carmarketplace

import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import liquibase.database.Database
import no.carmarketplace.support.POSTGRES_IMAGE
import no.carmarketplace.support.jdbcUrlWithCredentials
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty
import org.testcontainers.postgresql.PostgreSQLContainer
import kotlin.test.Test
import kotlin.test.assertTrue

// Every migration must apply to an empty database, roll back cleanly (migrate-down), and leave a schema
// that matches the entity classes. Its own Postgres, so rolling back doesn't disturb the other tests.
class MigrationsTest {

    @Test
    fun `all migrations apply, roll back completely, and apply again`() {
        withDatabase(url) { database: Database -> liquibase("update", database) }
        assertTrue(tables().containsAll(APP_TABLES))

        withDatabase(url) { database: Database -> liquibase("rollbackCount", database, mapOf("count" to migrationCount())) }
        assertTrue(tables().none { table: String -> table in APP_TABLES }, "rollback leaves no app tables behind")

        withDatabase(url) { database: Database -> liquibase("update", database) }
        assertTrue(tables().containsAll(APP_TABLES))
    }

    @Test
    fun `every entity property has a column`() {
        withDatabase(url) { database: Database -> liquibase("update", database) }
        val context: JdbcMappingContext = JdbcMappingContext()
        context.setInitialEntitySet(ENTITIES)
        context.afterPropertiesSet()

        for (type: Class<*> in ENTITIES) {
            val entity: RelationalPersistentEntity<*> = context.getRequiredPersistentEntity(type)
            val table: String = entity.tableName.reference
            val expected: Set<String> = entity.map { p: RelationalPersistentProperty -> p.columnName.reference }.toSet()
            val missing: Set<String> = expected - columns(table)
            assertTrue(missing.isEmpty(), "$table is missing columns $missing: run gradle migrate-generate")
        }
    }

    @Test
    fun `indexes declared with @Index exist`() {
        withDatabase(url) { database: Database -> liquibase("update", database) }
        assertTrue("ux_vehicles_source_id" in uniqueIndexes("vehicles"))
    }

    // Database metadata, no SQL: what tables, columns and indexes exist
    private fun tables(): Set<String> = metadata { c: Connection -> c.metaData.getTables(null, "public", "%", arrayOf("TABLE")) }
        .map { row: Map<String, String> -> row.getValue("TABLE_NAME") }.toSet()

    private fun columns(table: String): Set<String> = metadata { c: Connection -> c.metaData.getColumns(null, "public", table, null) }
        .map { row: Map<String, String> -> row.getValue("COLUMN_NAME") }.toSet()

    private fun uniqueIndexes(table: String): Set<String> =
        metadata { c: Connection -> c.metaData.getIndexInfo(null, "public", table, true, false) }
            .mapNotNull { row: Map<String, String> -> row["INDEX_NAME"] }.toSet()

    private fun metadata(query: (Connection) -> ResultSet): List<Map<String, String>> =
        DriverManager.getConnection(url).use { connection: Connection ->
            query(connection).use { rows: ResultSet ->
                val names: List<String> = (1..rows.metaData.columnCount).map { i: Int -> rows.metaData.getColumnLabel(i).uppercase() }
                generateSequence { if (rows.next()) names.associateWith { n: String -> rows.getString(n).orEmpty() } else null }.toList()
            }
        }

    // One migration file = one changeset
    private fun migrationCount(): Int =
        File("src/main/resources/db/changelog/migrations").listFiles { f: File -> f.extension == "sql" }!!.size

    companion object {
        private val APP_TABLES: Set<String> = setOf("vehicles", "outbox")
        private val postgres: PostgreSQLContainer = PostgreSQLContainer(POSTGRES_IMAGE)
        private val url: String by lazy { jdbcUrlWithCredentials(postgres) }

        @JvmStatic
        @BeforeAll
        fun start() {
            postgres.start()
        }

        @JvmStatic
        @AfterAll
        fun stop() {
            postgres.stop()
        }
    }
}
