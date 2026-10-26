package no.carmarketplace.search

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

// GET /search?q=family suv low miles&maxPrice=15000&minYear=2012&manufacturer=toyota&lat=40.7&lon=-74&radiusKm=50
//   mode=hybrid (default) | bm25 | vector   — to compare the two halves
@RestController
class SearchController(private val searchService: SearchService) {

    @GetMapping("/search")
    fun search(
        @RequestParam(defaultValue = "") q: String,
        @RequestParam(required = false) maxPrice: Int?,
        @RequestParam(required = false) minYear: Int?,
        @RequestParam(required = false) manufacturer: String?,
        @RequestParam(required = false) lat: Double?,
        @RequestParam(required = false) lon: Double?,
        @RequestParam(required = false) radiusKm: Int?,
        @RequestParam(defaultValue = "20") size: Int,
        @RequestParam(defaultValue = "HYBRID") mode: SearchMode,
    ): SearchResult {
        val request: SearchRequest = SearchRequest(
            text = q.trim(),
            maxPrice = maxPrice,
            minYear = minYear,
            manufacturer = manufacturer?.takeIf { m: String -> m.isNotBlank() },
            lat = lat,
            lon = lon,
            radiusKm = radiusKm,
            size = size.coerceIn(1, 50),
            mode = mode,
        )
        return searchService.search(request)
    }
}

enum class SearchMode { HYBRID, BM25, VECTOR }

data class SearchRequest(
    val text: String,
    val maxPrice: Int?,
    val minYear: Int?,
    val manufacturer: String?,
    val lat: Double?,
    val lon: Double?,
    val radiusKm: Int?,
    val size: Int,
    val mode: SearchMode,
)

data class SearchResult(
    val query: String,
    val mode: SearchMode,
    val results: List<SearchHit>,
    val candidates: Map<String, Int>, // how many came from each side before fusion
    val timingsMs: Map<String, Long>, // per stage: bm25, embed, vector, fusion, hydrate, total
)

data class SearchHit(
    val id: String,
    val title: String?,
    val price: Int?,
    val year: Int?,
    val manufacturer: String?,
    val model: String?,
    val score: Double,
    val foundBy: Set<String>, // bm25, vector or both
) {
    companion object {
        fun from(ranked: SearchService.Ranked, car: Map<String, Any?>): SearchHit = SearchHit(
            id = ranked.id,
            title = car["title"] as String?,
            price = (car["price"] as Number?)?.toInt(),
            year = (car["year"] as Number?)?.toInt(),
            manufacturer = car["manufacturer"] as String?,
            model = car["model"] as String?,
            score = ranked.score,
            foundBy = ranked.sources,
        )
    }
}
