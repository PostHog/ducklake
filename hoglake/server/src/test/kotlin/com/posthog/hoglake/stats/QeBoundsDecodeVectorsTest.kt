package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.UUID

/**
 * The decode side of the shared cross-language vectors
 * (pyhoglake/tests/vectors/bounds_vectors.json): for every vector not
 * marked `verify: encode_only`, the Kotlin decoder must satisfy
 * decode(unhex(hex)) == value and encode(decode(unhex(hex))) ==
 * unhex(hex) — the file's own contract. [QeBoundsVectorsTest] covers
 * the encode direction; this class exists because compaction's
 * bounds-merge now DECODES stored bounds to recompute typed min/max,
 * so decoder divergence would corrupt pruning bounds silently.
 */
class QeBoundsDecodeVectorsTest {
    private val vectorFile: Path =
        Path.of("..", "pyhoglake", "tests", "vectors", "bounds_vectors.json").normalize()

    @Test
    fun `kotlin decoder round-trips every two-way vector`() {
        assumeTrue(
            Files.exists(vectorFile),
            "SKIPPED: cross-language vector file not present at $vectorFile",
        )
        val root = ObjectMapper().readTree(Files.readString(vectorFile))
        val vectors = if (root.isArray) root else root["vectors"]
        assertThat(vectors.size()).isGreaterThan(0)

        val failures = mutableListOf<String>()
        var checked = 0
        for ((i, v) in vectors.withIndex()) {
            if (v["verify"]?.asText() == "encode_only") continue
            val type = ColType.fromWire(v["type"].asText())
            val hex = v["hex"].asText()
            val valueStr = v["value"].asText()
            val bytes = decodeHex(hex)
            checked++

            val decoded = IcebergSingleValue.decode(type, bytes)

            // decode(bytes) must equal the conventional value...
            if (!valueMatches(type, valueStr, decoded)) {
                failures += "vector[$i] $type value='$valueStr': decoded=$decoded"
                continue
            }
            // ...and re-encoding must reproduce the exact bytes.
            val reencoded = IcebergSingleValue.encode(type, decoded)
            if (!reencoded.contentEquals(bytes)) {
                failures += "vector[$i] $type value='$valueStr': " +
                    "re-encode ${hexOf(reencoded)} != $hex"
            }
        }
        assertThat(checked).describedAs("two-way vectors checked").isGreaterThan(40)
        assertThat(failures).describedAs("decode divergences (%d checked)", checked).isEmpty()
    }

    /** Compare a decoded value against the vector file's value conventions. */
    private fun valueMatches(
        type: ColType,
        value: String,
        decoded: Any,
    ): Boolean =
        when (type) {
            ColType.BOOLEAN -> decoded == value.toBooleanStrict()
            ColType.INT, ColType.DATE -> decoded == value.toInt()
            ColType.LONG, ColType.TIME, ColType.TIMESTAMP, ColType.TIMESTAMPTZ ->
                decoded == value.toLong()
            ColType.FLOAT ->
                when (value) {
                    // NaN payloads: the hex bits are authoritative.
                    "NaN" -> (decoded as Float).isNaN()
                    "Infinity" -> decoded == Float.POSITIVE_INFINITY
                    "-Infinity" -> decoded == Float.NEGATIVE_INFINITY
                    else -> decoded == value.toFloat()
                }
            ColType.DOUBLE ->
                when (value) {
                    "NaN" -> (decoded as Double).isNaN()
                    "Infinity" -> decoded == Double.POSITIVE_INFINITY
                    "-Infinity" -> decoded == Double.NEGATIVE_INFINITY
                    else -> decoded == value.toDouble()
                }
            ColType.STRING -> decoded == value
            ColType.UUID_T -> decoded == UUID.fromString(value)
            ColType.BINARY -> (decoded as ByteArray).contentEquals(Base64.getDecoder().decode(value))
            ColType.DECIMAL -> decoded == BigInteger(value)
        }

    private fun decodeHex(hex: String): ByteArray =
        ByteArray(hex.length / 2) { i -> hex.substring(i * 2, i * 2 + 2).toInt(16).toByte() }

    private fun hexOf(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it) }
}
