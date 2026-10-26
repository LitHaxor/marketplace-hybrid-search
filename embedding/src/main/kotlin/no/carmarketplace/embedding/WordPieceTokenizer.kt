package no.carmarketplace.embedding

import java.nio.file.Files
import java.nio.file.Path
import java.text.Normalizer

// Turns text into the token ids the model was trained on (BERT "uncased" WordPiece, as all-MiniLM-L6-v2 uses).
//   "Toyota Camry, low miles!" → [CLS] toyota camry , low miles ! [SEP] → [101, 11742, 2830, 2100, 1010, ...]
// Words that aren't in the vocabulary are split into known pieces: "camry" → "cam" + "##ry".
class WordPieceTokenizer(private val vocab: Map<String, Int>, private val maxTokens: Int) {

    private val cls: Int = vocab.getValue("[CLS]")
    private val sep: Int = vocab.getValue("[SEP]")
    private val unknown: Int = vocab.getValue("[UNK]")

    // Always [CLS] ... [SEP], at most maxTokens ids. Everything after that is cut off.
    fun encode(text: String): IntArray {
        val ids: MutableList<Int> = ArrayList(maxTokens)
        ids.add(cls)
        words@ for (word: String in basicTokens(text)) {
            for (piece: Int in wordPieces(word)) {
                if (ids.size == maxTokens - 1) break@words
                ids.add(piece)
            }
        }
        ids.add(sep)
        return ids.toIntArray()
    }

    // Lowercase, strip accents (é → e), split on whitespace, and make every punctuation mark its own token.
    private fun basicTokens(text: String): List<String> {
        val normalized: String = Normalizer.normalize(text.lowercase(), Normalizer.Form.NFD)
        val tokens: MutableList<String> = ArrayList()
        val current: StringBuilder = StringBuilder()
        fun flush() {
            if (current.isNotEmpty()) tokens.add(current.toString())
            current.setLength(0)
        }
        for (c: Char in normalized) {
            when {
                Character.getType(c) == Character.NON_SPACING_MARK.toInt() -> Unit // the accent part of é
                c.isWhitespace() -> flush()
                c.isISOControl() -> Unit
                isPunctuation(c) -> {
                    flush()
                    tokens.add(c.toString())
                }
                else -> current.append(c)
            }
        }
        flush()
        return tokens
    }

    // Greedy longest match: "camry" → is "camry" known? no. "camr"? no. "cam"? yes → then "##ry" for the rest.
    private fun wordPieces(word: String): List<Int> {
        if (word.length > 100) return listOf(unknown)
        val pieces: MutableList<Int> = ArrayList()
        var start: Int = 0
        while (start < word.length) {
            var end: Int = word.length
            var match: Int? = null
            while (start < end) {
                val candidate: String = (if (start > 0) "##" else "") + word.substring(start, end)
                match = vocab[candidate]
                if (match != null) break
                end--
            }
            if (match == null) return listOf(unknown)
            pieces.add(match)
            start = end
        }
        return pieces
    }

    private fun isPunctuation(c: Char): Boolean {
        val code: Int = c.code
        if (code in 33..47 || code in 58..64 || code in 91..96 || code in 123..126) return true
        return when (Character.getType(c).toByte()) {
            Character.CONNECTOR_PUNCTUATION, Character.DASH_PUNCTUATION, Character.START_PUNCTUATION,
            Character.END_PUNCTUATION, Character.INITIAL_QUOTE_PUNCTUATION, Character.FINAL_QUOTE_PUNCTUATION,
            Character.OTHER_PUNCTUATION -> true
            else -> false
        }
    }

    companion object {
        // vocab.txt has one token per line; the line number is the token's id
        fun fromVocabFile(path: Path, maxTokens: Int): WordPieceTokenizer {
            val vocab: Map<String, Int> = Files.readAllLines(path).withIndex().associate { (id, token) -> token to id }
            return WordPieceTokenizer(vocab, maxTokens)
        }
    }
}
