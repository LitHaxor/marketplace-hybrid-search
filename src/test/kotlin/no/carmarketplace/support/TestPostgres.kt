package no.carmarketplace.support

import liquibase.database.Database
import no.carmarketplace.liquibase
import no.carmarketplace.withDatabase
import org.testcontainers.postgresql.PostgreSQLContainer

// One real Postgres for all API integration tests (Testcontainers, so Docker must be running).
// Same version as k8s/dev, started on first use, and migrated with the project's own migrations:
// the same code `gradle migrate-up` runs.
object TestPostgres {

    val container: PostgreSQLContainer = PostgreSQLContainer(POSTGRES_IMAGE)

    init {
        container.start()
        withDatabase(jdbcUrlWithCredentials(container)) { database: Database -> liquibase("update", database) }
    }
}

const val POSTGRES_IMAGE: String = "postgres:17.11"

// Migrations.kt connects with one URL that carries user and password, like DB_URL in .env
fun jdbcUrlWithCredentials(container: PostgreSQLContainer): String {
    val separator: String = if ("?" in container.jdbcUrl) "&" else "?"
    return "${container.jdbcUrl}${separator}user=${container.username}&password=${container.password}"
}
