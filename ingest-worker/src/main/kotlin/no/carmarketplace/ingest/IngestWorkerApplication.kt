package no.carmarketplace.ingest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class IngestWorkerApplication

fun main(args: Array<String>) {
    runApplication<IngestWorkerApplication>(*args)
}
