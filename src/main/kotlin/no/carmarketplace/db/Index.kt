package no.carmarketplace.db

// A database index, created by `gradle migrate-generate` (like TypeORM's @Index).
//
// On a field:
//   @Index                              btree index on this column
//   @Index(order = SortOrder.DESC)      newest/highest first
//   @Index(method = IndexMethod.HASH)   another index type
//   @Index(unique = true)               no two rows with the same value
//
// On the class, for an index over several columns (order matters: leftmost first):
//   @Index("manufacturer", "year DESC")
//   Each entry is a property name, optionally followed by ASC/DESC and NULLS FIRST/LAST.
@Target(AnnotationTarget.CLASS, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
@Repeatable
annotation class Index(
    vararg val columns: String,
    val method: IndexMethod = IndexMethod.BTREE,
    val order: SortOrder = SortOrder.ASC,
    val unique: Boolean = false,
)

enum class IndexMethod {
    BTREE, // the default: equality, ranges (<, >, BETWEEN) and sorting
    HASH, // equality (=) only
    BRIN, // tiny index for huge tables where values follow insert order, e.g. timestamps
    GIN, // arrays and JSONB. Not plain text: full-text search belongs in Elasticsearch
    GIST, // geometric and range types
}

enum class SortOrder { ASC, DESC }
