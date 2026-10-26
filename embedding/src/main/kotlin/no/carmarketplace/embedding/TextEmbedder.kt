package no.carmarketplace.embedding

import ai.onnxruntime.OnnxTensor
import ai.onnxruntime.OrtEnvironment
import ai.onnxruntime.OrtSession
import java.nio.LongBuffer
import java.nio.file.Path
import java.util.concurrent.Semaphore
import kotlin.math.sqrt

// Text → 384 numbers that capture its meaning, using all-MiniLM-L6-v2 through ONNX Runtime.
// Similar texts get similar vectors: "toyota camry" lands near "toyota corolla", far from "banana bread".
//
// The model runs in native (C++) memory outside the Java heap, which the JVM can't see or limit.
// Each call needs temporary memory roughly proportional to batch x tokens x tokens, so this class bounds
// how many calls run at once and how big each one is. Without that, 6 parallel calls of 64 texts
// got the container OOMKilled at 1 GiB while the heap was only ~93 MB.
//
// A plain class, no Spring: the API (query vectors) and the ingest worker (car vectors) each create one.
class TextEmbedder(
    modelDir: String,
    maxTokens: Int = 128,
    threads: Int = 2,
    maxConcurrentCalls: Int = 2,
    private val batchSize: Int = 16,
) : AutoCloseable {

    private val tokenizer: WordPieceTokenizer =
        WordPieceTokenizer.fromVocabFile(Path.of(modelDir, "vocab.txt"), maxTokens)
    private val environment: OrtEnvironment = OrtEnvironment.getEnvironment()
    private val session: OrtSession = environment.createSession(
        Path.of(modelDir, "model.onnx").toString(),
        OrtSession.SessionOptions().apply {
            setIntraOpNumThreads(threads) // CPU threads per model call
            setCPUArenaAllocator(false) // give memory back after each call instead of keeping it for reuse
        },
    )

    // At most this many model calls at the same time, across all lanes and consumer threads.
    // A native call also pins its virtual thread to a carrier thread for its whole duration.
    private val calls: Semaphore = Semaphore(maxConcurrentCalls)

    // Kafka batches (up to 64) are split into model calls of batchSize texts.
    fun embed(texts: List<String>): List<FloatArray> =
        texts.chunked(batchSize).flatMap { chunk: List<String> ->
            calls.acquire()
            try {
                embedBatch(chunk)
            } finally {
                calls.release()
            }
        }

    // One model call for the whole batch: much faster than one call per text.
    private fun embedBatch(texts: List<String>): List<FloatArray> {
        if (texts.isEmpty()) return emptyList()
        val tokens: List<IntArray> = texts.map(tokenizer::encode)
        val batch: Int = tokens.size
        val length: Int = tokens.maxOf { ids: IntArray -> ids.size } // shorter texts are padded to this

        val inputIds: LongArray = LongArray(batch * length)
        val attentionMask: LongArray = LongArray(batch * length) // 1 = real token, 0 = padding
        tokens.forEachIndexed { row: Int, ids: IntArray ->
            ids.forEachIndexed { column: Int, id: Int ->
                inputIds[row * length + column] = id.toLong()
                attentionMask[row * length + column] = 1
            }
        }
        val shape: LongArray = longArrayOf(batch.toLong(), length.toLong())

        OnnxTensor.createTensor(environment, LongBuffer.wrap(inputIds), shape).use { ids: OnnxTensor ->
            OnnxTensor.createTensor(environment, LongBuffer.wrap(attentionMask), shape).use { mask: OnnxTensor ->
                OnnxTensor.createTensor(environment, LongBuffer.wrap(LongArray(batch * length)), shape).use { types: OnnxTensor ->
                    val inputs: Map<String, OnnxTensor> =
                        mapOf("input_ids" to ids, "attention_mask" to mask, "token_type_ids" to types)
                    session.run(inputs).use { result: OrtSession.Result ->
                        // One 384-number vector per token: [batch][token][384]
                        @Suppress("UNCHECKED_CAST")
                        val perToken: Array<Array<FloatArray>> = result.get(0).value as Array<Array<FloatArray>>
                        return perToken.mapIndexed { row: Int, vectors: Array<FloatArray> ->
                            meanOfRealTokens(vectors, tokens[row].size)
                        }
                    }
                }
            }
        }
    }

    // Sentence vector = average of its real tokens' vectors (no padding), scaled to length 1,
    // so cosine similarity is just a dot product.
    private fun meanOfRealTokens(vectors: Array<FloatArray>, realTokens: Int): FloatArray {
        val sum: FloatArray = FloatArray(vectors[0].size)
        for (token: Int in 0 until realTokens) {
            for (i: Int in sum.indices) sum[i] += vectors[token][i]
        }
        val length: Float = sqrt(sum.sumOf { v: Float -> (v * v).toDouble() }).toFloat()
        return FloatArray(sum.size) { i: Int -> sum[i] / length }
    }

    override fun close() {
        session.close()
    }
}
