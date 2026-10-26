package no.carmarketplace

import java.io.File
import java.sql.Connection
import java.lang.reflect.Field
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import liquibase.Scope
import liquibase.change.Change
import liquibase.change.ColumnConfig
import liquibase.change.core.AddColumnChange
import liquibase.change.core.CreateTableChange
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.command.CommandScope
import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.parser.ChangeLogParserFactory
import liquibase.resource.DirectoryResourceAccessor
import liquibase.sqlgenerator.SqlGeneratorFactory
import no.carmarketplace.db.Index
import no.carmarketplace.db.IndexMethod
import no.carmarketplace.db.SortOrder
import no.carmarketplace.outbox.OutboxEvent
import no.carmarketplace.vehicles.Vehicle
import org.springframework.core.io.FileSystemResource
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext
import org.springframework.data.jdbc.core.mapping.schema.DefaultSqlTypeMapping
import org.springframework.data.jdbc.core.mapping.schema.LiquibaseChangeSetWriter
import org.springframework.data.jdbc.core.mapping.schema.SqlTypeMapping
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty

// Every entity that should have a table. Add new entity classes here.
private val ENTITIES: Set<Class<*>> = setOf(Vehicle::class.java, OutboxEvent::class.java)

private const val RESOURCES: String = "src/main/resources"
private const val CHANGELOG: String = "db/changelog/db.changelog-master.yaml"
private const val MIGRATIONS: String = "db/changelog/migrations"

// The master changelog picks up every file in migrations/, in file-name (= timestamp) order.
private val MASTER_CHANGELOG: String = """
    |databaseChangeLog:
    |  - includeAll:
    |      path: $MIGRATIONS/
    |      errorIfMissingOrEmpty: false
    |""".trimMargin()

// Kotlin type -> Postgres type, where the default doesn't fit.
// String: the default VARCHAR(255) is too short for ad descriptions.
private val POSTGRES_TYPES: SqlTypeMapping = SqlTypeMapping { property: RelationalPersistentProperty ->
    when {
        property.type.isEnum -> "TEXT"
        else -> when (property.type) {
            String::class.java -> "TEXT"
            UUID::class.java -> "UUID"
            Instant::class.java -> "TIMESTAMP WITH TIME ZONE"
            else -> null
        }
    }
}

fun main(args: Array<String>) {
    ensureMasterChangelog()
    when (args.firstOrNull()) {
        "generate" -> generate()
        "up" -> up()
        "down" -> down()
        else -> error("Usage: generate | up | down")
    }
}

// typeorm migration:generate
private fun generate() {
    val context: JdbcMappingContext = JdbcMappingContext()
    context.setInitialEntitySet(ENTITIES)
    context.afterPropertiesSet()

    val writer: LiquibaseChangeSetWriter = LiquibaseChangeSetWriter(context)
    writer.setSqlTypeMapping(POSTGRES_TYPES.and(DefaultSqlTypeMapping()))

    val timestamp: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
    // Spring Data writes its diff as a YAML draft; we turn that draft into a SQL migration.
    val draft: File = File(System.getProperty("java.io.tmpdir"), "migration-$timestamp.yaml")

    withDatabase { database: Database ->
        writer.writeChangeSet(FileSystemResource(draft), database)
        val changes: List<Change> = parse(draft).changeSets.flatMap { changeSet: ChangeSet -> changeSet.changes }
        draft.delete()
        uuidIdsAreSetByTheApp(changes)
        val steps: List<Step> = changes.map { change: Change -> tableStep(change, database) } +
            indexSteps(context, database)

        if (steps.isEmpty()) {
            println("No changes: entities already match the database.")
            return@withDatabase
        }

        val migration: File = File(RESOURCES, "$MIGRATIONS/$timestamp.sql")
        migration.parentFile.mkdirs()
        migration.writeText(sqlMigration(timestamp, steps))
        println("Created ${migration.path}")
    }
}

// Spring Data marks every @Id column as auto-increment, but Postgres can only auto-increment numbers.
// UUID ids are set in Kotlin before insert (see VehicleIdGenerator), so switch it off for them.
private fun uuidIdsAreSetByTheApp(changes: List<Change>) {
    val columns: List<ColumnConfig> = changes.flatMap { change: Change ->
        when (change) {
            is CreateTableChange -> change.columns
            is AddColumnChange -> change.columns
            else -> emptyList()
        }
    }
    columns
        .filter { column: ColumnConfig -> column.type.equals("UUID", ignoreCase = true) }
        .forEach { column: ColumnConfig -> column.isAutoIncrement = null }
}

// One change in a migration: its SQL forwards (up) and backwards (down).
private data class Step(val up: List<String>, val down: List<String>)

private fun tableStep(change: Change, database: Database): Step {
    val sqlGenerator: SqlGeneratorFactory = SqlGeneratorFactory.getInstance()
    return Step(
        up = sqlGenerator.generateSql(change, database).map { sql -> sql.toSql() },
        down = sqlGenerator.generateSql(change.generateRollbackStatements(database), database).map { sql -> sql.toSql() },
    )
}

// Indexes are compared by name. The name encodes the whole definition (table, columns, order, method),
// so changing an @Index means: drop the old index, create the new one.
private data class IndexDefinition(val name: String, val createSql: String)

// Indexes created by migrate-generate. Anything else (primary keys, hand-made indexes) is left alone.
private val MANAGED_INDEX: Regex = Regex("^(idx|ux)_.+")

private fun indexSteps(context: JdbcMappingContext, database: Database): List<Step> {
    val schema: String = database.defaultSchemaName
    val entities: List<RelationalPersistentEntity<*>> = context.persistentEntities.toList()
    val wanted: Map<String, String> = entities
        .flatMap { entity: RelationalPersistentEntity<*> -> wantedIndexes(entity, schema) }
        .associate { index: IndexDefinition -> index.name to index.createSql }
    val existing: Map<String, String> = existingIndexes(
        database,
        schema,
        entities.map { entity: RelationalPersistentEntity<*> -> entity.tableName.reference },
    )

    val drops: List<Step> = (existing.keys - wanted.keys).map { name: String ->
        Step(up = listOf("DROP INDEX $schema.$name"), down = listOf(existing.getValue(name)))
    }
    val creates: List<Step> = (wanted.keys - existing.keys).map { name: String ->
        Step(up = listOf(wanted.getValue(name)), down = listOf("DROP INDEX $schema.$name"))
    }
    return drops + creates
}

private fun wantedIndexes(entity: RelationalPersistentEntity<*>, schema: String): List<IndexDefinition> {
    val table: String = entity.tableName.reference

    val onClass: List<IndexDefinition> = entity.type.getAnnotationsByType(Index::class.java).map { index: Index ->
        require(index.columns.isNotEmpty()) { "@Index on class ${entity.type.simpleName} needs column names" }
        indexDefinition(schema, table, index.columns.map { spec: String -> columnSpec(entity, spec) }, index)
    }

    val onFields: List<IndexDefinition> = entity.flatMap { property: RelationalPersistentProperty ->
        val field: Field? = property.field
        field?.getAnnotationsByType(Index::class.java).orEmpty().map { index: Index ->
            require(index.columns.isEmpty()) { "@Index on field ${property.name} takes no column names" }
            val order: String = if (index.order == SortOrder.DESC) " DESC" else ""
            indexDefinition(schema, table, listOf(property.columnName.reference + order), index)
        }
    }
    return onClass + onFields
}

// "year DESC NULLS LAST" -> same, with the property name replaced by its column name
private fun columnSpec(entity: RelationalPersistentEntity<*>, spec: String): String {
    val parts: List<String> = spec.trim().split(Regex("\\s+"))
    val column: String = entity.getRequiredPersistentProperty(parts.first()).columnName.reference
    val modifiers: String = parts.drop(1).joinToString(" ").uppercase()
    require(modifiers.matches(Regex("(ASC|DESC)?\\s?(NULLS (FIRST|LAST))?"))) {
        "@Index column \"$spec\": use \"year\", \"year DESC\" or \"year DESC NULLS LAST\""
    }
    return if (modifiers.isEmpty()) column else "$column $modifiers"
}

private fun indexDefinition(schema: String, table: String, columns: List<String>, index: Index): IndexDefinition {
    val sorted: Boolean = columns.any { column: String -> column.contains(' ') }
    require(index.method == IndexMethod.BTREE || !sorted) { "Only BTREE indexes can be sorted (ASC/DESC/NULLS)" }
    require(index.method == IndexMethod.BTREE || !index.unique) { "Postgres only supports unique BTREE indexes" }

    val method: String = index.method.name.lowercase()
    val prefix: String = if (index.unique) "ux" else "idx"
    val columnPart: String = columns.joinToString("_") { column: String -> column.lowercase().replace(' ', '_') }
    val methodPart: String = if (index.method == IndexMethod.BTREE) "" else "_$method"
    val name: String = indexName("${prefix}_${table}_$columnPart$methodPart")
    val unique: String = if (index.unique) "UNIQUE " else ""
    return IndexDefinition(name, "CREATE ${unique}INDEX $name ON $schema.$table USING $method (${columns.joinToString(", ")})")
}

// Postgres cuts names at 63 characters; keep long ones unique with a short hash.
private fun indexName(name: String): String {
    if (name.length <= 63) return name
    return name.take(54) + "_" + Integer.toHexString(name.hashCode()).padStart(8, '0')
}

// name -> definition, as Postgres reports it. The definition doubles as the "down" of a DROP INDEX.
private fun existingIndexes(database: Database, schema: String, tables: List<String>): Map<String, String> {
    val connection: Connection = (database.connection as JdbcConnection).underlyingConnection
    val query: String = "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = ? AND tablename = ANY (?)"
    val indexes: MutableMap<String, String> = mutableMapOf()
    connection.prepareStatement(query).use { statement: PreparedStatement ->
        statement.setString(1, schema)
        statement.setArray(2, connection.createArrayOf("text", tables.toTypedArray()))
        statement.executeQuery().use { rows: ResultSet ->
            while (rows.next()) {
                val name: String = rows.getString("indexname")
                if (MANAGED_INDEX.matches(name)) indexes[name] = rows.getString("indexdef")
            }
        }
    }
    return indexes
}

// Liquibase "formatted SQL": the statements are the up part, the --rollback lines the down part.
private fun sqlMigration(id: String, steps: List<Step>): String {
    return buildString {
        appendLine("--liquibase formatted sql")
        appendLine()
        appendLine("--changeset migrate-generate:$id")
        steps.flatMap { step: Step -> step.up }.forEach { statement: String -> appendLine("$statement;") }
        steps.reversed().flatMap { step: Step -> step.down }.forEach { statement: String -> appendLine("--rollback $statement;") }
    }
}

private fun ensureMasterChangelog() {
    val master: File = File(RESOURCES, CHANGELOG)
    if (!master.exists()) {
        master.parentFile.mkdirs()
        master.writeText(MASTER_CHANGELOG)
    }
}

// typeorm migration:run
private fun up() {
    withDatabase { database: Database -> liquibase("update", database) }
}

// typeorm migration:revert
private fun down() {
    withDatabase { database: Database -> liquibase("rollbackCount", database, mapOf("count" to 1)) }
}

private fun liquibase(command: String, database: Database, arguments: Map<String, Any> = emptyMap()) {
    Scope.child(Scope.Attr.resourceAccessor, DirectoryResourceAccessor(File(RESOURCES))) {
        val scope: CommandScope = CommandScope(command)
            .addArgumentValue("changelogFile", CHANGELOG)
            .addArgumentValue("database", database)
        arguments.forEach { (name: String, value: Any) -> scope.addArgumentValue(name, value) }
        scope.execute()
    }
}

private fun parse(changeLogFile: File): DatabaseChangeLog {
    val folder: DirectoryResourceAccessor = DirectoryResourceAccessor(changeLogFile.parentFile)
    return ChangeLogParserFactory.getInstance()
        .getParser(changeLogFile.name, folder)
        .parse(changeLogFile.name, ChangeLogParameters(), folder)
}

private fun withDatabase(block: (Database) -> Unit) {
    DriverManager.getConnection(dbUrl()).use { connection: Connection ->
        val database: Database = DatabaseFactory.getInstance()
            .findCorrectDatabaseImplementation(JdbcConnection(connection))
        block(database)
    }
}

// Same DB_URL the app uses: environment variable first, then .env
private fun dbUrl(): String {
    val fromEnvironment: String? = System.getenv("DB_URL")
    if (fromEnvironment != null) return fromEnvironment
    return File(".env").readLines()
        .first { line: String -> line.startsWith("DB_URL=") }
        .removePrefix("DB_URL=")
}
