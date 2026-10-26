package no.carmarketplace.outbox

// Kafka keeps order only inside a partition, with no priorities. So urgent events get their own topic
// and their own consumers, and a big backlog in one lane can't delay another.
enum class EventLane(val topic: String) {
    PRIORITY("vehicle.priority"), // sold/deleted: must leave search within ~2 s
    CHANGES("vehicle.changes"), // a user creates or edits an ad: seconds
    BULK("vehicle.bulk"), // seed, dealer imports, backfills: minutes is fine
}
