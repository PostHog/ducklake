package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Byte-level contract of the puffin `deletion-vector-v1` reader: the
 * writer half lives in [PuffinTestFiles] and mirrors the Iceberg v3
 * layout exactly, so the roundtrip pins both directions. Corruption
 * must fail loudly — a mis-decoded DV silently loses or resurrects
 * deletes.
 */
class PuffinDeletionVectorTest {
    @Test
    fun `roundtrips positions including bucket boundaries`() {
        val positions = listOf(0L, 1L, 3L, 4_000_000_000L, (1L shl 32) + 7, (5L shl 32))
        val dv = PuffinDeletionVector.read(PuffinTestFiles.deletionVector(positions))
        assertThat(dv.cardinality).isEqualTo(positions.size.toLong())
        for (p in positions) assertThat(dv.contains(p)).describedAs("position $p").isTrue()
        for (p in listOf(2L, 5L, 4_000_000_001L, (1L shl 32) + 8, (5L shl 32) + 1, 1L shl 33)) {
            assertThat(dv.contains(p)).describedAs("position $p").isFalse()
        }
    }

    @Test
    fun `empty vector decodes to zero cardinality`() {
        val dv = PuffinDeletionVector.read(PuffinTestFiles.deletionVector(emptyList()))
        assertThat(dv.cardinality).isZero()
        assertThat(dv.contains(0)).isFalse()
    }

    @Test
    fun `a corrupted vector byte fails the CRC check`() {
        val bytes = PuffinTestFiles.deletionVector(listOf(1L, 2L, 3L))
        // Flip a byte inside the blob's vector region (offset 4 = length
        // prefix, +4 magic; +12 lands in the serialized bitmap).
        bytes[16] = (bytes[16].toInt() xor 0x40).toByte()
        assertThatThrownBy { PuffinDeletionVector.read(bytes) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("CRC")
    }

    @Test
    fun `bad header magic is refused`() {
        val bytes = PuffinTestFiles.deletionVector(listOf(1L))
        bytes[0] = 'X'.code.toByte()
        assertThatThrownBy { PuffinDeletionVector.read(bytes) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("magic")
    }

    @Test
    fun `a compressed footer flag is refused, not mis-decoded`() {
        val bytes = PuffinTestFiles.deletionVector(listOf(1L))
        bytes[bytes.size - 8] = 1 // flags byte 0 bit 0 = payload compressed
        assertThatThrownBy { PuffinDeletionVector.read(bytes) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("compressed")
    }
}
