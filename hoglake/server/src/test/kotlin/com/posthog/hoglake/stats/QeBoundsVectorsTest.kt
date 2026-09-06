package com.posthog.hoglake.stats

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.UUID

/**
 * Cross-language differential vectors (fuzzing.md layer 2): the Python
 * codec (pyhoglake/bounds.py) emits canonical (type, type_params,
 * value, hex) vectors to pyhoglake/tests/vectors/bounds_vectors.json;
 * this test asserts the Kotlin encoder produces the same bytes for
 * every vector. Any divergence between the two codecs corrupts pruning
 * silently — here it is a test failure instead.
 *
 * Value conventions (from the file's own header, version 1):
 *  - boolean: "true" | "false"
 *  - int/date: decimal int32 string; long/time/timestamp/timestamptz:
 *    decimal int64 string
 *  - float/double: decimal string or "NaN"/"Infinity"/"-Infinity"; for
 *    NaN the HEX BYTES are authoritative (payload NaNs share the "NaN"
 *    token), so the JVM value is reconstructed from the hex bits and
 *    must re-encode to those exact bytes — this is precisely the
 *    raw-bits-preservation contract.
 *  - string: the string itself; uuid: canonical form; binary: base64
 *  - decimal: UNSCALED integer as decimal string
 *
 * If the vector file is absent (the Python side not yet landed), the
 * test SKIPS with a clear message rather than failing.
 */
class QeBoundsVectorsTest {
    private val vectorFile: Path =
        Path.of("..", "pyhoglake", "tests", "vectors", "bounds_vectors.json").normalize()

    @Test
    fun `kotlin encoder matches every python-generated vector`() {
        assumeTrue(
            Files.exists(vectorFile),
            "SKIPPED: cross-language vector file not present yet at $vectorFile " +
                "(the pyhoglake side generates it; re-run once it lands)",
        )
        val root = ObjectMapper().readTree(Files.readString(vectorFile))
        val vectors: JsonNode =
            when {
                root.isArray -> root
                root.has("vectors") && root["vectors"].isArray -> root["vectors"]
                else -> error("unrecognized vector file shape: expected array or {vectors: [...]}")
            }
        assertThat(vectors.size()).describedAs("vector count").isGreaterThan(0)

        val failures = mutableListOf<String>()
        var checked = 0
        for ((i, v) in vectors.withIndex()) {
            val typeStr = v["type"]?.asText() ?: error("vector[$i] has no type: $v")
            val hex = v["hex"]?.asText() ?: error("vector[$i] has no hex: $v")
            val valueStr = v["value"]?.asText() ?: error("vector[$i] has no value: $v")
            val type = ColType.fromWire(typeStr)
            val expected = decodeHex(hex)
            val value = jvmValue(type, valueStr, expected)
            val actual = IcebergSingleValue.encode(type, value)
            checked++
            if (!actual.contentEquals(expected)) {
                failures += "vector[$i] type=$typeStr value='$valueStr' " +
                    "(${v["note"]?.asText()}): python=${hexOf(expected)} kotlin=${hexOf(actual)}"
            }
        }
        assertThat(checked).isEqualTo(vectors.size())
        assertThat(failures)
            .describedAs("cross-language encoding divergences (checked %d vectors)", checked)
            .isEmpty()
    }

    // ---- value coercion per the file's conventions ------------------------

    private fun jvmValue(
        type: ColType,
        value: String,
        hexBytes: ByteArray,
    ): Any =
        when (type) {
            ColType.BOOLEAN -> value.toBooleanStrict()
            ColType.INT, ColType.DATE -> value.toInt()
            ColType.LONG, ColType.TIME, ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> value.toLong()
            ColType.FLOAT ->
                when (value) {
                    // NaN: the hex is authoritative for the payload; reconstruct
                    // the exact bit pattern and demand it round-trips.
                    "NaN" ->
                        Float.fromBits(
                            ByteBuffer.wrap(hexBytes).order(ByteOrder.LITTLE_ENDIAN).int,
                        )
                    "Infinity" -> Float.POSITIVE_INFINITY
                    "-Infinity" -> Float.NEGATIVE_INFINITY
                    else -> value.toFloat()
                }
            ColType.DOUBLE ->
                when (value) {
                    "NaN" ->
                        Double.fromBits(
                            ByteBuffer.wrap(hexBytes).order(ByteOrder.LITTLE_ENDIAN).long,
                        )
                    "Infinity" -> Double.POSITIVE_INFINITY
                    "-Infinity" -> Double.NEGATIVE_INFINITY
                    else -> value.toDouble()
                }
            ColType.STRING -> value
            ColType.UUID_T -> UUID.fromString(value)
            ColType.BINARY -> Base64.getDecoder().decode(value)
            ColType.DECIMAL -> BigInteger(value)
        }

    // ---- hex helpers -----------------------------------------------------

    private fun decodeHex(hex: String): ByteArray {
        require(hex.length % 2 == 0) { "odd hex length in '$hex'" }
        return ByteArray(hex.length / 2) { i ->
            hex.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
    }

    private fun hexOf(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it) }
}
