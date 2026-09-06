package com.posthog.hoglake.stats

import com.posthog.hoglake.model.ColType
import io.kotest.property.Arb
import io.kotest.property.arbitrary.Codepoint
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.byte
import io.kotest.property.arbitrary.byteArray
import io.kotest.property.arbitrary.egyptianHieroglyphs
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/**
 * QE property assault on IcebergSingleValue (fuzzing.md layer 1, JVM
 * side): encode determinism, fixed-width invariants per type over the
 * full value domains, and the codec's edge policies pinned as
 * assertions so a change in behavior is a test failure, not a silent
 * drift.
 *
 * PINNED POLICIES (the "actual policy" record the charter asks for):
 *  - float/double NaN: the RAW bit pattern is preserved (ByteBuffer
 *    putFloat/putDouble use floatToRawIntBits/doubleToRawLongBits), so
 *    non-canonical NaN payloads survive encoding byte-for-byte.
 *  - +0.0 and -0.0 encode DIFFERENTLY (sign bit preserved) for both
 *    float and double.
 *  - Infinities encode as their exact IEEE-754 bit patterns.
 *  - date from a LocalDate outside the int epoch-day range throws
 *    IllegalArgumentException naming the offending value (the internal
 *    Math.toIntExact ArithmeticException is translated to honor
 *    encode()'s KDoc'd contract).
 *  - timestamptz from an Instant whose micros-since-epoch overflow a
 *    long throws IllegalArgumentException naming the offending value
 *    (same translation over Math.multiplyExact/addExact).
 *  - strings: unpaired surrogates are replaced with '?' (0x3F) by the
 *    JVM UTF-8 encoder — bounds for such strings are lossy.
 *  - decimal: scale is NOT carried; BigDecimal("1.5") and
 *    BigDecimal("15") encode to the SAME bytes. Callers must normalize
 *    to the column scale first (documented in the KDoc; pinned here).
 *  - LONG accepts an Int (widening); INT does NOT accept a Long.
 */
class QeIcebergSingleValuePropertyTest {
    private fun leInt(bytes: ByteArray): Int = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).int

    private fun leLong(bytes: ByteArray): Long = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).long

    // ---- determinism + fixed widths -------------------------------------

    @Nested
    inner class WidthsAndDeterminism {
        @Test
        fun `int - 4 bytes, LE round-trip, deterministic, full domain`() =
            runBlocking<Unit> {
                checkAll(Arb.int()) { v ->
                    val enc = IcebergSingleValue.encode(ColType.INT, v)
                    assertThat(enc).hasSize(4)
                    assertThat(leInt(enc)).isEqualTo(v)
                    assertThat(IcebergSingleValue.encode(ColType.INT, v)).isEqualTo(enc)
                }
                for (v in listOf(Int.MIN_VALUE, Int.MAX_VALUE, 0, -1, 1)) {
                    assertThat(leInt(IcebergSingleValue.encodeInt(v))).isEqualTo(v)
                }
            }

        @Test
        fun `long - 8 bytes, LE round-trip, deterministic, full domain`() =
            runBlocking<Unit> {
                checkAll(Arb.long()) { v ->
                    val enc = IcebergSingleValue.encode(ColType.LONG, v)
                    assertThat(enc).hasSize(8)
                    assertThat(leLong(enc)).isEqualTo(v)
                    assertThat(IcebergSingleValue.encode(ColType.LONG, v)).isEqualTo(enc)
                }
                for (v in listOf(Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L)) {
                    assertThat(leLong(IcebergSingleValue.encodeLong(v))).isEqualTo(v)
                }
            }

        @Test
        fun `float - 4 bytes, raw IEEE-754 bits round-trip over all bit patterns`() =
            runBlocking<Unit> {
                // Every 32-bit pattern is a float: normals, subnormals,
                // ±0.0, ±inf, and every NaN payload.
                checkAll(Arb.int()) { bits ->
                    val v = Float.fromBits(bits)
                    val enc = IcebergSingleValue.encode(ColType.FLOAT, v)
                    assertThat(enc).hasSize(4)
                    assertThat(leInt(enc)).isEqualTo(java.lang.Float.floatToRawIntBits(v))
                }
            }

        @Test
        fun `double - 8 bytes, raw IEEE-754 bits round-trip over all bit patterns`() =
            runBlocking<Unit> {
                checkAll(Arb.long()) { bits ->
                    val v = Double.fromBits(bits)
                    val enc = IcebergSingleValue.encode(ColType.DOUBLE, v)
                    assertThat(enc).hasSize(8)
                    assertThat(leLong(enc)).isEqualTo(java.lang.Double.doubleToRawLongBits(v))
                }
            }

        @Test
        fun `time timestamp timestamptz raw micros - 8 bytes LE for any long`() =
            runBlocking<Unit> {
                checkAll(Arb.long()) { v ->
                    assertThat(leLong(IcebergSingleValue.encode(ColType.TIME, v))).isEqualTo(v)
                    assertThat(leLong(IcebergSingleValue.encode(ColType.TIMESTAMP, v))).isEqualTo(v)
                    assertThat(leLong(IcebergSingleValue.encode(ColType.TIMESTAMPTZ, v))).isEqualTo(v)
                }
            }

        @Test
        fun `date - 4 bytes LE for any int day count`() =
            runBlocking<Unit> {
                checkAll(Arb.int()) { v ->
                    assertThat(leInt(IcebergSingleValue.encode(ColType.DATE, v))).isEqualTo(v)
                }
            }

        @Test
        fun `uuid - always 16 bytes, big-endian, bijective`() =
            runBlocking<Unit> {
                checkAll(Arb.long(), Arb.long()) { hi, lo ->
                    val u = UUID(hi, lo)
                    val enc = IcebergSingleValue.encode(ColType.UUID_T, u)
                    assertThat(enc).hasSize(16)
                    val bb = ByteBuffer.wrap(enc) // big-endian by default
                    assertThat(UUID(bb.long, bb.long)).isEqualTo(u)
                }
            }

        @Test
        fun `boolean - exactly one byte, 0x00 or 0x01`() {
            assertThat(IcebergSingleValue.encode(ColType.BOOLEAN, false))
                .isEqualTo(byteArrayOf(0))
            assertThat(IcebergSingleValue.encode(ColType.BOOLEAN, true))
                .isEqualTo(byteArrayOf(1))
        }
    }

    // ---- float/double edge policy pins ----------------------------------

    @Nested
    inner class FloatDoublePolicies {
        @Test
        fun `NaN payloads are preserved raw - canonical and non-canonical`() {
            // Canonical quiet NaN.
            assertThat(IcebergSingleValue.encodeFloat(Float.NaN))
                .isEqualTo(byteArrayOf(0, 0, 0xC0.toByte(), 0x7F))
            assertThat(IcebergSingleValue.encodeDouble(Double.NaN))
                .isEqualTo(byteArrayOf(0, 0, 0, 0, 0, 0, 0xF8.toByte(), 0x7F))
            // Non-canonical payloads (signalling-shaped, negative NaN):
            // raw bits survive — the codec never canonicalizes.
            for (bits in listOf(0x7F800001, 0x7FC00001, 0xFFC00000.toInt(), 0x7FBFFFFF)) {
                val enc = IcebergSingleValue.encodeFloat(Float.fromBits(bits))
                assertThat(leInt(enc)).isEqualTo(bits)
            }
            for (bits in listOf(0x7FF0000000000001L, 0xFFF8000000000000UL.toLong())) {
                val enc = IcebergSingleValue.encodeDouble(Double.fromBits(bits))
                assertThat(leLong(enc)).isEqualTo(bits)
            }
        }

        @Test
        fun `plus and minus zero are distinct encodings`() {
            assertThat(IcebergSingleValue.encodeFloat(0.0f))
                .isNotEqualTo(IcebergSingleValue.encodeFloat(-0.0f))
            assertThat(IcebergSingleValue.encodeFloat(-0.0f))
                .isEqualTo(byteArrayOf(0, 0, 0, 0x80.toByte()))
            assertThat(IcebergSingleValue.encodeDouble(0.0))
                .isNotEqualTo(IcebergSingleValue.encodeDouble(-0.0))
        }

        @Test
        fun `infinities and subnormals encode their exact bit patterns`() {
            assertThat(leInt(IcebergSingleValue.encodeFloat(Float.POSITIVE_INFINITY)))
                .isEqualTo(0x7F800000)
            assertThat(leInt(IcebergSingleValue.encodeFloat(Float.NEGATIVE_INFINITY)))
                .isEqualTo(0xFF800000.toInt())
            assertThat(leInt(IcebergSingleValue.encodeFloat(Float.MIN_VALUE))) // subnormal
                .isEqualTo(1)
            assertThat(leLong(IcebergSingleValue.encodeDouble(Double.MIN_VALUE)))
                .isEqualTo(1L)
            assertThat(leLong(IcebergSingleValue.encodeDouble(-Double.MIN_VALUE)))
                .isEqualTo(java.lang.Double.doubleToRawLongBits(-Double.MIN_VALUE))
        }
    }

    // ---- strings ---------------------------------------------------------

    @Nested
    inner class Strings {
        private fun utf8Len(s: String): Int =
            s.codePoints().map { cp ->
                when {
                    cp < 0x80 -> 1
                    cp < 0x800 -> 2
                    cp < 0x10000 -> 3
                    else -> 4
                }
            }.sum()

        @Test
        fun `utf-8 length invariant holds incl astral plane`() =
            runBlocking<Unit> {
                checkAll(Arb.string(0..64, Codepoint.egyptianHieroglyphs())) { s ->
                    val enc = IcebergSingleValue.encode(ColType.STRING, s)
                    assertThat(enc).hasSize(utf8Len(s))
                    assertThat(String(enc, Charsets.UTF_8)).isEqualTo(s)
                }
                checkAll(Arb.string(0..64)) { s ->
                    val enc = IcebergSingleValue.encodeString(s)
                    assertThat(enc).hasSize(utf8Len(s))
                }
            }

        @Test
        fun `empty string and empty binary are zero-length encodings`() {
            assertThat(IcebergSingleValue.encode(ColType.STRING, "")).isEmpty()
            assertThat(IcebergSingleValue.encode(ColType.BINARY, ByteArray(0))).isEmpty()
        }

        @Test
        fun `PINNED - unpaired surrogates are replaced with 0x3F, lossily`() {
            // The JVM UTF-8 encoder substitutes '?' for malformed input.
            // Consequence: two DIFFERENT strings ("\uD800" and "?") share
            // one encoding — string bounds containing lone surrogates are
            // not faithful. Writers must not ship them.
            assertThat(IcebergSingleValue.encodeString("\uD800")).isEqualTo(byteArrayOf(0x3F))
            assertThat(IcebergSingleValue.encodeString("\uDFFF")).isEqualTo(byteArrayOf(0x3F))
            assertThat(IcebergSingleValue.encodeString("?"))
                .isEqualTo(IcebergSingleValue.encodeString("\uD800"))
        }

        @Test
        fun `binary passes bytes through for any content`() =
            runBlocking<Unit> {
                checkAll(Arb.byteArray(Arb.int(0..128), Arb.byte())) { b ->
                    val enc = IcebergSingleValue.encode(ColType.BINARY, b)
                    assertThat(enc).isEqualTo(b)
                    assertThat(enc).isNotSameAs(b) // defensive copy
                }
            }
    }

    // ---- decimals --------------------------------------------------------

    @Nested
    inner class Decimals {
        /** Full signed BigInteger domain from random two's-complement bytes. */
        private val arbBigInt: Arb<BigInteger> =
            arbitrary { rs ->
                val bytes = Arb.byteArray(Arb.int(1..20), Arb.byte()).bind()
                BigInteger(bytes)
            }

        @Test
        fun `minimal twos-complement - round-trips and is exactly minimal`() =
            runBlocking<Unit> {
                checkAll(arbBigInt) { v ->
                    val enc = IcebergSingleValue.encode(ColType.DECIMAL, v)
                    assertThat(BigInteger(enc)).isEqualTo(v)
                    // BigInteger.toByteArray is the minimal two's-complement
                    // form by contract: exactly bitLength/8 + 1 bytes.
                    assertThat(enc).hasSize(v.bitLength() / 8 + 1)
                }
            }

        @Test
        fun `precision-38 extremes - both signs, 16 bytes`() {
            val max38 = BigInteger.TEN.pow(38).subtract(BigInteger.ONE)
            val enc = IcebergSingleValue.encodeDecimalUnscaled(max38)
            assertThat(enc).hasSize(16)
            assertThat(BigInteger(enc)).isEqualTo(max38)
            val neg = IcebergSingleValue.encodeDecimalUnscaled(max38.negate())
            assertThat(neg).hasSize(16)
            assertThat(BigInteger(neg)).isEqualTo(max38.negate())
        }

        @Test
        fun `sign-extension boundaries - values needing an extra byte`() {
            // Positive values whose top bit would read as negative gain a
            // leading 0x00; negatives on the boundary shrink.
            val cases =
                mapOf(
                    BigInteger.valueOf(127) to byteArrayOf(0x7F),
                    BigInteger.valueOf(128) to byteArrayOf(0x00, 0x80.toByte()),
                    BigInteger.valueOf(-128) to byteArrayOf(0x80.toByte()),
                    BigInteger.valueOf(-129) to byteArrayOf(0xFF.toByte(), 0x7F),
                    BigInteger.valueOf(32767) to byteArrayOf(0x7F, 0xFF.toByte()),
                    BigInteger.valueOf(32768) to byteArrayOf(0x00, 0x80.toByte(), 0x00),
                )
            for ((v, expected) in cases) {
                assertThat(IcebergSingleValue.encodeDecimalUnscaled(v))
                    .describedAs("unscaled %s", v)
                    .isEqualTo(expected)
            }
        }

        @Test
        fun `PINNED - scale is not carried, differing scales can collide`() {
            // 1.5 (unscaled 15, scale 1) and 15 (unscaled 15, scale 0)
            // encode identically. The column type carries the scale; a
            // caller shipping a wrong-scale BigDecimal corrupts bounds
            // silently. Pinned so the policy is at least explicit.
            assertThat(IcebergSingleValue.encodeDecimal(BigDecimal("1.5")))
                .isEqualTo(IcebergSingleValue.encodeDecimal(BigDecimal("15")))
            // Same numeric value at different scales does NOT collide:
            assertThat(IcebergSingleValue.encodeDecimal(BigDecimal("14.20")))
                .isNotEqualTo(IcebergSingleValue.encodeDecimal(BigDecimal("14.2")))
        }
    }

    // ---- edge policies + dispatch strictness -----------------------------

    @Nested
    inner class EdgePolicies {
        @Test
        fun `LocalDate outside int epoch-days throws IllegalArgumentException naming the value`() {
            // encode()'s KDoc'd contract is IllegalArgumentException on
            // bad input; the internal Math.toIntExact ArithmeticException
            // is translated (regression pin for the QE-found leak).
            assertThatThrownBy { IcebergSingleValue.encode(ColType.DATE, LocalDate.MAX) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(LocalDate.MAX.toString())
            assertThatThrownBy { IcebergSingleValue.encode(ColType.DATE, LocalDate.MIN) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(LocalDate.MIN.toString())
        }

        @Test
        fun `Instant beyond long micros throws IllegalArgumentException naming the value`() {
            assertThatThrownBy { IcebergSingleValue.encode(ColType.TIMESTAMPTZ, Instant.MAX) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(Instant.MAX.toString())
            assertThatThrownBy { IcebergSingleValue.encode(ColType.TIMESTAMPTZ, Instant.MIN) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(Instant.MIN.toString())
            // The last representable micros value is fine.
            val edge = Instant.ofEpochSecond(Long.MAX_VALUE / 1_000_000, 0)
            assertThat(IcebergSingleValue.encode(ColType.TIMESTAMPTZ, edge)).hasSize(8)
        }

        @Test
        fun `LONG widens Int but INT never accepts Long`() =
            runBlocking<Unit> {
                checkAll(Arb.int()) { v ->
                    assertThat(IcebergSingleValue.encode(ColType.LONG, v))
                        .isEqualTo(IcebergSingleValue.encodeLong(v.toLong()))
                }
                assertThatThrownBy { IcebergSingleValue.encode(ColType.INT, 1L) }
                    .isInstanceOf(IllegalArgumentException::class.java)
                assertThatThrownBy { IcebergSingleValue.encode(ColType.INT, Long.MAX_VALUE) }
                    .isInstanceOf(IllegalArgumentException::class.java)
            }

        @Test
        fun `no cross-type value sneaks through dispatch`() {
            val wrong =
                mapOf<ColType, Any>(
                    ColType.BOOLEAN to 1,
                    ColType.INT to 1L,
                    ColType.FLOAT to 1.0,
                    ColType.DOUBLE to 1.0f,
                    ColType.DATE to 1L,
                    ColType.TIME to 1,
                    ColType.TIMESTAMP to Instant.EPOCH,
                    ColType.TIMESTAMPTZ to java.time.LocalDateTime.MIN,
                    ColType.STRING to byteArrayOf(1),
                    ColType.UUID_T to "f79c3e09-677c-4bbd-a479-3f349cb785e7",
                    ColType.BINARY to "bytes",
                    ColType.DECIMAL to 1.0,
                )
            for ((type, value) in wrong) {
                assertThatThrownBy { IcebergSingleValue.encode(type, value) }
                    .describedAs("%s should reject %s", type, value::class.simpleName)
                    .isInstanceOf(IllegalArgumentException::class.java)
            }
        }
    }
}
