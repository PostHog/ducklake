package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.compaction.PuffinDeletionVector
import java.io.IOException

/**
 * Fuzz target (fuzzing.md layer 4, target d — highest value: fresh code
 * decoding writer-supplied bytes): [PuffinDeletionVector.read] over
 * arbitrary puffin bytes.
 *
 * Contract under test:
 *  - a malformed file is refused LOUDLY and typed: IllegalArgumentException
 *    (the documented structural checks), IllegalStateException (missing
 *    footer fields via error()), or IOException (Jackson footer-payload
 *    parse, EOF inside the bitmap) — never an untyped crash, never a
 *    silent mis-decode;
 *  - decoding is deterministic: two reads of the same bytes agree on
 *    cardinality and membership;
 *  - a decoded vector behaves sanely: cardinality >= 0, contains() is
 *    total over non-negative positions.
 *
 * Allocation stays input-proportional by construction (bucket count is
 * bounds-checked, each bucket read consumes stream bytes), so no input
 * cap is needed; libFuzzer's default -malloc_limit backstops the claim.
 */
class PuffinDeletionVectorFuzzTest {
    @FuzzTest(maxDuration = "180s")
    fun readRefusesLoudlyOrDecodesDeterministically(data: ByteArray) {
        val first =
            try {
                PuffinDeletionVector.read(data)
            } catch (e: Exception) {
                check(e is IllegalArgumentException || e is IllegalStateException || e is IOException) {
                    "puffin DV decode escaped with untyped ${e.javaClass.name}: ${e.message}"
                }
                return
            }

        check(first.cardinality >= 0) { "negative DV cardinality ${first.cardinality}" }

        // Determinism: the same bytes must decode to the same vector.
        val second = PuffinDeletionVector.read(data)
        check(second.cardinality == first.cardinality) {
            "non-deterministic DV decode: ${first.cardinality} vs ${second.cardinality}"
        }
        for (position in PROBE_POSITIONS) {
            check(first.contains(position) == second.contains(position)) {
                "non-deterministic DV membership at position $position"
            }
        }
    }

    private companion object {
        val PROBE_POSITIONS =
            longArrayOf(0L, 1L, 42L, Int.MAX_VALUE.toLong(), 1L shl 32, (1L shl 32) + 7, Long.MAX_VALUE)
    }
}
