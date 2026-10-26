package no.carmarketplace.embedding

import java.nio.file.Path
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Uses the real model files (downloaded by the downloadModel Gradle task; MODEL_DIR points at them).
class TextEmbedderTest {

    private val modelDir: String = System.getenv("MODEL_DIR") ?: "build/model"

    @Test
    fun `tokenizer gives the same ids as the original BERT tokenizer`() {
        val tokenizer: WordPieceTokenizer = WordPieceTokenizer.fromVocabFile(Path.of(modelDir, "vocab.txt"), 128)
        // [CLS] hello , world ! [SEP] in bert-base-uncased
        assertContentEquals(intArrayOf(101, 7592, 1010, 2088, 999, 102), tokenizer.encode("Hello, world!"))
    }

    @Test
    fun `tokenizer cuts long text at the token limit`() {
        val tokenizer: WordPieceTokenizer = WordPieceTokenizer.fromVocabFile(Path.of(modelDir, "vocab.txt"), 8)
        val ids: IntArray = tokenizer.encode("one two three four five six seven eight nine ten")
        assertEquals(8, ids.size)
        assertEquals(101, ids.first())
        assertEquals(102, ids.last())
    }

    @Test
    fun `similar texts get closer vectors than unrelated texts`() {
        TextEmbedder(modelDir, maxTokens = 128, threads = 2, maxConcurrentCalls = 1, batchSize = 2).use { embedder: TextEmbedder ->
            val (camry: FloatArray, corolla: FloatArray, bread: FloatArray) =
                embedder.embed(listOf("2015 toyota camry sedan", "2016 toyota corolla", "banana bread recipe"))

            assertEquals(384, camry.size)
            assertTrue(abs(dot(camry, camry) - 1f) < 1e-3, "vectors are normalized to length 1")
            assertTrue(dot(camry, corolla) > dot(camry, bread) + 0.2f, "camry is closer to corolla than to bread")
        }
    }

    private fun dot(a: FloatArray, b: FloatArray): Float = a.indices.sumOf { i: Int -> (a[i] * b[i]).toDouble() }.toFloat()
}
