package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
import java.math.BigInteger
import java.util.UUID

/**
 * Fuzz target (fuzzing.md layer 4, target b): property-style consistency
 * of [IcebergSingleValue.encode] / [IcebergSingleValue.compareValues]
 * over generated typed values.
 *
 * Properties:
 *  - encode is total on generated in-domain values and
 *    `decode(encode(x))` compares equal to x (value-level round-trip);
 *  - sign antisymmetry: sgn(cmp(a,b)) == -sgn(cmp(b,a));
 *  - reflexivity: cmp(a,a) == 0;
 *  - transitivity: a <= b && b <= c implies a <= c;
 *  - equals-consistency: cmp(a,b) == 0 implies sgn(cmp(a,c)) == sgn(cmp(b,c)).
 *
 * compareValues is what compaction's bounds-merge sorts with (raw byte
 * compare of the encodings would be wrong for signed little-endian ints)
 * — an inconsistent comparator silently corrupts merged bounds.
 */
class IcebergSingleValueCompareFuzzTest {
    @FuzzTest(maxDuration = "120s")
    fun comparatorConsistency(data: FuzzedDataProvider) {
        val type = data.pickValue(ColType.entries.toTypedArray())
        val a = generate(type, data) ?: return
        val b = generate(type, data) ?: return
        val c = generate(type, data) ?: return

        val roundTripped = IcebergSingleValue.decode(type, IcebergSingleValue.encode(type, a))
        check(IcebergSingleValue.compareValues(type, a, roundTripped) == 0) {
            "decode(encode(x)) does not compare equal to x for ${type.wire}: $a vs $roundTripped"
        }

        val ab = sgn(type, a, b)
        val ba = sgn(type, b, a)
        val bc = sgn(type, b, c)
        val ac = sgn(type, a, c)

        check(sgn(type, a, a) == 0) { "cmp(a,a) != 0 for ${type.wire}: $a" }
        check(ab == -ba) { "antisymmetry violated for ${type.wire}: cmp(a,b)=$ab cmp(b,a)=$ba" }
        if (ab <= 0 && bc <= 0) {
            check(ac <= 0) { "transitivity violated for ${type.wire}: a<=b<=c but cmp(a,c)=$ac" }
        }
        if (ab == 0) {
            check(ac == bc) {
                "equals-consistency violated for ${type.wire}: cmp(a,b)=0 but cmp(a,c)=$ac cmp(b,c)=$bc"
            }
        }
    }

    private fun sgn(
        type: ColType,
        x: Any,
        y: Any,
    ): Int = Integer.signum(IcebergSingleValue.compareValues(type, x, y))

    /** A generated in-domain value for [type]; null when the provider ran dry. */
    private fun generate(
        type: ColType,
        data: FuzzedDataProvider,
    ): Any? =
        when (type) {
            ColType.BOOLEAN -> data.consumeBoolean()
            ColType.INT, ColType.DATE -> data.consumeInt()
            ColType.LONG, ColType.TIME, ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> data.consumeLong()
            // Raw bit patterns cover NaN payloads, infinities, and both zeros.
            ColType.FLOAT -> Float.fromBits(data.consumeInt())
            ColType.DOUBLE -> Double.fromBits(data.consumeLong())
            ColType.STRING -> data.consumeString(64)
            ColType.UUID_T -> UUID(data.consumeLong(), data.consumeLong())
            ColType.BINARY -> data.consumeBytes(data.consumeInt(0, 64))
            ColType.DECIMAL ->
                data.consumeBytes(data.consumeInt(1, 32)).takeIf { it.isNotEmpty() }?.let { BigInteger(it) }
        }
}
