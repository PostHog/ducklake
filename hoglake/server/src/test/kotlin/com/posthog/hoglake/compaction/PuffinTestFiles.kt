package com.posthog.hoglake.compaction

import org.roaringbitmap.RoaringBitmap
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.zip.CRC32

/**
 * Spec-conformant Iceberg puffin `deletion-vector-v1` writer for tests
 * — the byte-level mirror of [PuffinDeletionVector]'s reader: "PFA1"
 * header, one DV blob (BE length prefix, D1 D3 39 64 magic, portable
 * 64-bit roaring bitmap, BE CRC-32), then the JSON footer sandwiched in
 * magic with its LE size and zero flags.
 */
object PuffinTestFiles {
    private val PUFFIN_MAGIC = byteArrayOf(0x50, 0x46, 0x41, 0x31)
    private val DV_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39, 0x64)

    fun deletionVector(positions: Collection<Long>): ByteArray {
        val vector = portableBitmap(positions)
        val blob = ByteArrayOutputStream()
        DataOutputStream(blob).use { out ->
            out.writeInt(4 + vector.size) // BE: magic + vector
            out.write(DV_MAGIC)
            out.write(vector)
            val crc = CRC32()
            crc.update(DV_MAGIC)
            crc.update(vector)
            out.writeInt(crc.value.toInt()) // BE
        }
        val blobBytes = blob.toByteArray()
        val payload =
            """{"blobs":[{"type":"deletion-vector-v1","fields":[],"snapshot-id":0,""" +
                """"sequence-number":0,"offset":4,"length":${blobBytes.size},""" +
                """"properties":{"cardinality":"${positions.size}"}}],"properties":{}}"""
        val payloadBytes = payload.toByteArray(StandardCharsets.UTF_8)

        val file = ByteArrayOutputStream()
        file.write(PUFFIN_MAGIC)
        file.write(blobBytes)
        file.write(PUFFIN_MAGIC)
        file.write(payloadBytes)
        file.write(
            ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(payloadBytes.size).array(),
        )
        file.write(byteArrayOf(0, 0, 0, 0)) // flags: uncompressed
        file.write(PUFFIN_MAGIC)
        return file.toByteArray()
    }

    /** Portable 64-bit roaring: LE bucket count, per bucket LE high key + 32-bit bitmap. */
    private fun portableBitmap(positions: Collection<Long>): ByteArray {
        require(positions.all { it >= 0 }) { "negative delete position" }
        val buckets = positions.groupBy { (it ushr 32).toInt() }.toSortedMap()
        val out = ByteArrayOutputStream()
        DataOutputStream(out).use { stream ->
            stream.writeLong(java.lang.Long.reverseBytes(buckets.size.toLong()))
            for ((key, bucketPositions) in buckets) {
                stream.writeInt(Integer.reverseBytes(key))
                val bitmap = RoaringBitmap()
                bucketPositions.forEach { bitmap.add(it.toInt()) }
                bitmap.serialize(stream)
            }
        }
        return out.toByteArray()
    }
}
