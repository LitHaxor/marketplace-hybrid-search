rootProject.name = "kotlin-hybrid-search"

// A second program in this build: consumes vehicle events from Kafka (its own JVM, its own Docker image)
include("ingest-worker")

// Shared library: text → vector (all-MiniLM-L6-v2). Used by the API (search queries) and the worker (cars).
include("embedding")
