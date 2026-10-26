package no.carmarketplace

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling


@SpringBootApplication
@EnableScheduling // runs @Scheduled jobs, like the outbox cleaner
class HybridSearchApplication


fun main(args: Array<String>) {
    runApplication<HybridSearchApplication>(*args)
}