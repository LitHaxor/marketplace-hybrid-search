package no.carmarketplace.seed

import java.time.Instant
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

// Temporary admin endpoint to load the dataset. Remove it once the data is in.
//   POST /admin/seed?limit=1000   start (limit is optional: without it, the whole file)
//   GET  /admin/seed              progress
@RestController
@RequestMapping("/admin/seed")
class SeedController(private val seeder: CsvSeeder) {

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED) // 202: started, not finished
    fun start(@RequestParam(required = false) limit: Long?): SeedStatus {
        return seeder.start(limit)
    }

    @GetMapping
    fun status(): SeedStatus {
        return seeder.status()
    }
}

data class SeedStatus(
    val running: Boolean,
    val file: String,
    val read: Long,
    val saved: Long,
    val failed: Long,
    val skipped: Long,
    val perSecond: Double,
    val startedAt: Instant?,
    val finishedAt: Instant?,
    val lastError: String?,
)
