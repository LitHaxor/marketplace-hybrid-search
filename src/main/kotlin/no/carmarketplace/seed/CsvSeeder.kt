package no.carmarketplace.seed

import de.siegmar.fastcsv.reader.CsvReader
import de.siegmar.fastcsv.reader.NamedCsvRecord
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import no.carmarketplace.outbox.EventLane
import no.carmarketplace.vehicles.Vehicle
import no.carmarketplace.vehicles.VehicleRepository
import no.carmarketplace.vehicles.VehicleService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException

// Loads vehicles.csv through the normal write path (VehicleService.create), one car at a time,
// so every car also produces an outbox event → Debezium → Kafka → ingest worker.
@Service
class CsvSeeder(
    private val vehicleService: VehicleService,
    private val repository: VehicleRepository,
    meterRegistry: MeterRegistry,
    @Value($$"${seed.file}") private val file: String,
) {

    private val log: Logger = LoggerFactory.getLogger(CsvSeeder::class.java)

    // Read from the HTTP thread, written by the seeding thread: atomics/volatile keep that safe.
    private val running: AtomicBoolean = AtomicBoolean(false)
    private val read: AtomicLong = AtomicLong()
    private val saved: AtomicLong = AtomicLong()
    private val failed: AtomicLong = AtomicLong()
    private val skipped: AtomicLong = AtomicLong() // already in the database (same sourceId)
    @Volatile private var startedAt: Instant? = null
    @Volatile private var finishedAt: Instant? = null
    @Volatile private var lastError: String? = null

    // Prometheus: seed_vehicles_total{result="saved|failed"}, to watch the rate in Grafana
    private val savedCounter: Counter = meterRegistry.counter("seed.vehicles", "result", "saved")
    private val failedCounter: Counter = meterRegistry.counter("seed.vehicles", "result", "failed")

    fun start(limit: Long?): SeedStatus {
        if (!running.compareAndSet(false, true)) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "A seed is already running")
        }
        read.set(0)
        saved.set(0)
        failed.set(0)
        skipped.set(0)
        lastError = null
        startedAt = Instant.now()
        finishedAt = null

        // The HTTP request returns right away; the seeding runs on its own (virtual) thread.
        Thread.ofVirtual().name("csv-seeder").start { seed(limit) }
        return status()
    }

    fun status(): SeedStatus {
        val start: Instant? = startedAt
        val end: Instant = finishedAt ?: Instant.now()
        val seconds: Double = if (start == null) 0.0 else Duration.between(start, end).toMillis() / 1000.0
        return SeedStatus(
            running = running.get(),
            file = file,
            read = read.get(),
            saved = saved.get(),
            failed = failed.get(),
            skipped = skipped.get(),
            perSecond = if (seconds > 0) saved.get() / seconds else 0.0,
            startedAt = startedAt,
            finishedAt = finishedAt,
            lastError = lastError,
        )
    }

    private fun seed(limit: Long?) {
        log.info("Seeding from {} (limit: {})", file, limit ?: "none")
        try {
            CsvReader.builder().ofNamedCsvRecord(Path.of(file)).use { csv: CsvReader<NamedCsvRecord> ->
                for (record: NamedCsvRecord in csv) {
                    if (limit != null && read.get() >= limit) break
                    read.incrementAndGet()
                    try {
                        val vehicle: Vehicle = record.toVehicle()
                        if (vehicle.sourceId != null && repository.existsBySourceId(vehicle.sourceId)) {
                            skipped.incrementAndGet()
                            continue
                        }
                        vehicleService.create(vehicle, EventLane.BULK)
                        saved.incrementAndGet()
                        savedCounter.increment()
                    } catch (e: Exception) {
                        failed.incrementAndGet()
                        failedCounter.increment()
                        lastError = e.message
                    }
                }
            }
        } catch (e: Exception) {
            lastError = e.message
            log.error("Seeding stopped", e)
        } finally {
            finishedAt = Instant.now()
            running.set(false)
            log.info("Seeding finished: {}", status())
        }
    }
}

// One CSV row -> Vehicle. Empty or broken cells become null instead of crashing.
private fun NamedCsvRecord.toVehicle(): Vehicle = Vehicle(
    price = getField("price").toIntOrNull(),
    year = getField("year").toDoubleOrNull()?.toInt(),
    manufacturer = getField("manufacturer").ifBlank { null },
    model = getField("model").ifBlank { null },
    description = getField("description").ifBlank { null },
    lat = getField("lat").toDoubleOrNull(),
    lon = getField("long").toDoubleOrNull(),
    sourceId = getField("id").toLongOrNull(),
)
