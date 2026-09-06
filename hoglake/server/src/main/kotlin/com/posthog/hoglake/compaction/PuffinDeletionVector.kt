package com.posthog.hoglake.compaction

import com.fasterxml.jackson.databind.ObjectMapper
import org.roaringbitmap.RoaringBitmap
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * The deleted positions of one data file, decoded from its live DV.
 * Positions are 0-based physical row ordinals within the data file
 * (Iceberg deletion-vector semantics) — NOT hoglake row ids: for a
 * positional file ordinal o maps to row id row_id_start + o; for an
 * explicit-row-id file the ordinal indexes the physical row whose id
 * rides the `_hog_row_id` column.
 */
class DeletionVector internal constructor(
    /** 32-bit roaring bitmap per high-32-bit position bucket. */
    private val buckets: Map<Int, RoaringBitmap>,
) {
    val cardinality: Long =
        buckets.values.sumOf { it.longCardinality }

    fun contains(position: Long): Boolean {
        require(position >= 0) { "negative position $position" }
        val bucket = buckets[(position ushr 32).toInt()] ?: return false
        return bucket.contains(position.toInt())
    }
}

/**
 * Reader for hoglake's one delete encoding: an Iceberg v3 puffin file
 * carrying a single `deletion-vector-v1` blob (iceberg-federation.md §4
 * — internal readers and the facade share this encoding by design; the
 * hog_delete_file.file_format vocabulary is exactly 'puffin-dv').
 *
 * Puffin container (Puffin spec v1): "PFA1" magic, blob bytes, then the
 * footer — magic, UTF-8 JSON FooterPayload, its 4-byte LE size, 4 flag
 * bytes, magic. The only defined flag (payload lz4-compression) is
 * refused: hoglake writers ship uncompressed footers, and a corrupt or
 * exotic DV must fail the group loudly, never decode wrongly.
 *
 * `deletion-vector-v1` blob content (Iceberg table spec v3):
 *   - combined length of magic + vector, 4 bytes big-endian
 *   - magic bytes D1 D3 39 64
 *   - the vector: a portable 64-bit roaring bitmap (8-byte LE bucket
 *     count, then per bucket a 4-byte LE high key + a standard portable
 *     32-bit roaring bitmap — the Java RoaringBitmap stream format)
 *   - CRC-32 of magic + vector, 4 bytes big-endian
 *
 * Every structural check throws [IllegalArgumentException] with the
 * offending detail; the compaction sweep treats that as a failed group
 * (isolated, logged), never a partial apply.
 */
object PuffinDeletionVector {
    private const val BLOB_TYPE = "deletion-vector-v1"
    private val PUFFIN_MAGIC = byteArrayOf(0x50, 0x46, 0x41, 0x31) // "PFA1"
    private val DV_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39, 0x64)
    private val json = ObjectMapper()

    fun read(bytes: ByteArray): DeletionVector {
        require(bytes.size >= PUFFIN_MAGIC.size * 3 + 8) { "puffin file too small (${bytes.size} bytes)" }
        requireMagic(bytes, 0, "header")
        requireMagic(bytes, bytes.size - 4, "footer trailer")
        val flags = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int
        require(flags and 1 == 0) { "compressed puffin footer payloads are not supported" }
        require(flags and 1.inv() == 0) { "unknown puffin footer flags 0x${Integer.toHexString(flags)}" }
        val payloadSize = ByteBuffer.wrap(bytes, bytes.size - 12, 4).order(ByteOrder.LITTLE_ENDIAN).int
        val payloadStart = bytes.size - 12 - payloadSize
        require(payloadSize >= 0 && payloadStart >= PUFFIN_MAGIC.size * 2) {
            "puffin footer payload size $payloadSize is out of bounds"
        }
        requireMagic(bytes, payloadStart - 4, "footer header")

        val payload = json.readTree(bytes, payloadStart, payloadSize)
        val blobs =
            (payload.get("blobs")?.takeIf { it.isArray } ?: error("puffin footer payload has no blobs array"))
                .filter { it.get("type")?.asText() == BLOB_TYPE }
        require(blobs.size == 1) {
            "expected exactly one $BLOB_TYPE blob, found ${blobs.size}"
        }
        val blob = blobs.single()
        require(blob.get("compression-codec").let { it == null || it.isNull }) {
            "compressed $BLOB_TYPE blobs are not supported"
        }
        val offset = blob.get("offset")?.asLong() ?: error("$BLOB_TYPE blob has no offset")
        val length = blob.get("length")?.asLong() ?: error("$BLOB_TYPE blob has no length")
        require(offset >= PUFFIN_MAGIC.size && length >= 12 && offset + length <= payloadStart - 4) {
            "$BLOB_TYPE blob range [$offset, +$length) is out of bounds"
        }
        return decodeBlob(bytes, Math.toIntExact(offset), Math.toIntExact(length))
    }

    private fun decodeBlob(
        bytes: ByteArray,
        offset: Int,
        length: Int,
    ): DeletionVector {
        val declared = ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.BIG_ENDIAN).int
        // declared covers magic + vector; the blob adds its own 4-byte
        // length prefix and 4-byte CRC suffix.
        require(declared == length - 8) {
            "$BLOB_TYPE length prefix $declared does not match blob length $length"
        }
        for (i in DV_MAGIC.indices) {
            require(bytes[offset + 4 + i] == DV_MAGIC[i]) { "bad $BLOB_TYPE magic" }
        }
        val vectorLen = declared - 4
        val crc = CRC32()
        crc.update(bytes, offset + 4, 4 + vectorLen)
        val storedCrc =
            ByteBuffer.wrap(bytes, offset + 8 + vectorLen, 4).order(ByteOrder.BIG_ENDIAN).int
        require(crc.value.toInt() == storedCrc) {
            "$BLOB_TYPE CRC mismatch: stored $storedCrc, computed ${crc.value.toInt()}"
        }
        return deserializePortable(bytes, offset + 8, vectorLen)
    }

    /** Portable 64-bit roaring bitmap: LE bucket count, then per-bucket LE key + 32-bit bitmap. */
    private fun deserializePortable(
        bytes: ByteArray,
        offset: Int,
        length: Int,
    ): DeletionVector {
        val stream = DataInputStream(ByteArrayInputStream(bytes, offset, length))
        val bucketCount = java.lang.Long.reverseBytes(stream.readLong())
        require(bucketCount in 0..Int.MAX_VALUE.toLong()) { "implausible DV bucket count $bucketCount" }
        val buckets = LinkedHashMap<Int, RoaringBitmap>()
        repeat(Math.toIntExact(bucketCount)) {
            val key = Integer.reverseBytes(stream.readInt())
            val bitmap = RoaringBitmap()
            // The Java stream format IS the spec's portable 32-bit format.
            bitmap.deserialize(stream)
            require(buckets.put(key, bitmap) == null) { "duplicate DV bucket key $key" }
        }
        require(stream.read() == -1) { "trailing bytes after the DV bitmap" }
        return DeletionVector(buckets)
    }

    private fun requireMagic(
        bytes: ByteArray,
        at: Int,
        where: String,
    ) {
        for (i in PUFFIN_MAGIC.indices) {
            require(bytes[at + i] == PUFFIN_MAGIC[i]) { "bad puffin magic at $where (offset $at)" }
        }
    }
}
