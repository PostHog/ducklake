package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

/**
 * Fuzz target (fuzzing.md layer 4, target a): [IcebergSingleValue.decode]
 * over arbitrary bytes for every [ColType].
 *
 * Contract under test:
 *  - decode never throws anything but [IllegalArgumentException] (the
 *    documented refusal for wrong-length / empty encodings);
 *  - when decode succeeds, `encode(type, decode(type, b))` reproduces `b`
 *    byte-for-byte for every CANONICAL encoding (the doc-comment
 *    invariant). Non-canonical inputs that decode has to accept —
 *    boolean bytes other than 0/1, non-minimal decimal two's-complement,
 *    invalid UTF-8 (decoded with replacement) — are exempt from the
 *    byte-exact check but must still re-encode without throwing;
 *  - a decoded value compares equal to itself under
 *    [IcebergSingleValue.compareValues].
 *
 * Input shape: byte 0 selects the ColType (mod 13), the rest is the
 * encoding under test. The committed corpus is derived from
 * pyhoglake/tests/vectors/bounds_vectors.json (see FuzzSeedGenerator).
 */
class IcebergSingleValueDecodeFuzzTest {
    @FuzzTest(maxDuration = "120s")
    fun decodeIsTotalAndRoundTrips(data: ByteArray) {
        if (data.isEmpty()) return
        val type = ColType.entries[(data[0].toInt() and 0xFF) % ColType.entries.size]
        val payload = data.copyOfRange(1, data.size)

        val decoded =
            try {
                IcebergSingleValue.decode(type, payload)
            } catch (_: IllegalArgumentException) {
                return // the documented refusal; anything else propagates as a finding
            }

        // Re-encoding a successfully decoded value must never throw.
        val reencoded = IcebergSingleValue.encode(type, decoded)

        if (isCanonical(type, payload)) {
            check(reencoded.contentEquals(payload)) {
                "encode(decode(x)) != x for ${type.wire}: " +
                    "in=${payload.toHex()} out=${reencoded.toHex()}"
            }
        }

        check(IcebergSingleValue.compareValues(type, decoded, decoded) == 0) {
            "decoded ${type.wire} value does not compare equal to itself"
        }
    }

    /** Encodings decode() accepts but is not required to reproduce byte-exactly. */
    private fun isCanonical(
        type: ColType,
        payload: ByteArray,
    ): Boolean =
        when (type) {
            // decode maps any nonzero byte to true; only 0x00/0x01 are canonical.
            ColType.BOOLEAN -> payload.size == 1 && payload[0] in 0..1
            // BigInteger re-encodes minimally; redundant sign-extension bytes drop.
            ColType.DECIMAL -> BigInteger(payload).toByteArray().size == payload.size
            // Invalid UTF-8 decodes with U+FFFD replacement and cannot round-trip.
            ColType.STRING -> isValidUtf8(payload)
            else -> true
        }

    private fun isValidUtf8(bytes: ByteArray): Boolean =
        try {
            Charsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
            true
        } catch (_: CharacterCodingException) {
            false
        }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
