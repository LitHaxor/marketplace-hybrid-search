package no.carmarketplace.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Pure logic: no Elasticsearch, no Qdrant. Each list votes 1 / (60 + rank), rank starting at 1.
class ReciprocalRankFusionTest {

    @Test
    fun `a car found by both searches beats a car ranked first by only one`() {
        val ranked: List<SearchService.Ranked> =
            SearchService.reciprocalRankFusion(lexical = listOf("a", "b"), semantic = listOf("c", "b"))

        val best: SearchService.Ranked = ranked.first()
        assertEquals("b", best.id)
        assertEquals(setOf("bm25", "vector"), best.sources)
        assertEquals(1.0 / 62 + 1.0 / 62, best.score, 1e-12) // rank 2 in both lists
        // a and c are each rank 1 in one list: a tie at 1/61, behind b
        assertEquals(setOf("a", "c"), ranked.drop(1).map { r: SearchService.Ranked -> r.id }.toSet())
    }

    @Test
    fun `the score of rank 1 is 1 over 61`() {
        val ranked: List<SearchService.Ranked> = SearchService.reciprocalRankFusion(lexical = listOf("a"), semantic = emptyList())

        assertEquals(1, ranked.size)
        assertEquals(1.0 / 61, ranked.single().score, 1e-12)
        assertEquals(setOf("bm25"), ranked.single().sources)
    }

    @Test
    fun `with one list empty the other list keeps its order`() {
        val ranked: List<SearchService.Ranked> =
            SearchService.reciprocalRankFusion(lexical = emptyList(), semantic = listOf("x", "y", "z"))

        assertEquals(listOf("x", "y", "z"), ranked.map { r: SearchService.Ranked -> r.id })
        assertTrue(ranked.all { r: SearchService.Ranked -> r.sources == setOf("vector") })
    }

    @Test
    fun `no candidates gives no results`() {
        assertTrue(SearchService.reciprocalRankFusion(lexical = emptyList(), semantic = emptyList()).isEmpty())
    }
}
