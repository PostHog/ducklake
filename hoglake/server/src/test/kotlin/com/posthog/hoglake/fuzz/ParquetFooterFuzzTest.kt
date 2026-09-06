package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Fuzz target (fuzzing.md layer 4, target c): the parquet footer parse
 * the hydrator runs on writer-supplied bytes, plus [FooterStats] over
 * whatever parses.
 *
 * The harness mirrors Hydrator.parseFooter exactly:
 * `ParquetFileReader.open(input).use { it.footer }` over an in-memory
 * [InputFile]. Input shape: bytes starting with "PAR1" are treated as a
 * whole parquet file; anything else is wrapped as the thrift footer of a
 * synthetic file (PAR1 + data + LE len + PAR1) so the fuzzer spends its
 * time inside the thrift decode, not hunting for magic bytes.
 *
 * Contract under test:
 *  - the footer parse fails typed/structurally — IOException (including
 *    parquet's ParquetDecodingException-wrapped thrift failures) or a
 *    refusal RuntimeException raised from parquet's own frames
 *    (parquet-java deliberately uses bare RuntimeException for "is not a
 *    Parquet file" / "corrupted file") — never NPE/ClassCastException,
 *    never a crash outside parquet frames, never a hang, never unbounded
 *    allocation (input capped at 1 MiB; thrift's own limits govern the
 *    rest);
 *  - when the footer DOES parse, FooterStats.missingFieldIds /
 *    usesFieldIds / aggregate are total: they never throw, whatever the
 *    schema shape (that is the hydrator's "bounds NULL, never guessed"
 *    contract — the TransformGlobalStatsRow crash class from the defect
 *    ledger).
 */
class ParquetFooterFuzzTest {
    @FuzzTest(maxDuration = "180s")
    fun footerParseFailsTypedAndStatsAreTotal(data: ByteArray) {
        if (data.size > MAX_INPUT_BYTES) return
        val file = if (startsWithMagic(data)) data else wrapAsFooter(data)

        val footer: ParquetMetadata =
            try {
                ParquetFileReader.open(BytesInputFile(file)).use { it.footer }
            } catch (e: Exception) {
                checkAllowedParseFailure(e)
                return
            }

        // Parsed: the pure stats layer must be total over any schema shape.
        val schema = footer.fileMetaData.schema
        FooterStats.missingFieldIds(schema)
        FooterStats.usesFieldIds(schema)
        FooterStats.aggregate(footer, CATALOG_COLUMNS, "fuzz://footer")
    }

    private fun checkAllowedParseFailure(e: Exception) {
        // KNOWN UPSTREAM WART (fuzzer-found 2026-09-06, pinned corpus entry
        // thrift_readbinary_npe): shaded thrift's TCompactProtocol.readBinary
        // NPEs (ByteBuffer.wrap(null)) while SKIPPING an unknown binary field
        // with a hostile length in a corrupt footer. parquet-java internal;
        // the hydrator contains it (Hydrator.kt catch(Exception) -> 'failed'),
        // but it is a bare NPE where a decoding refusal belongs. Carved out
        // by exact signature so any OTHER NPE stays a finding.
        if (e is NullPointerException &&
            e.stackTrace.any { it.className.startsWith("shaded.parquet.org.apache.thrift.protocol") }
        ) {
            return
        }
        // NPE/CCE are never a deliberate refusal, wherever they come from.
        if (e is NullPointerException || e is ClassCastException) {
            throw IllegalStateException("footer parse escaped with ${e.javaClass.name}: ${e.message}", e)
        }
        val parquetFrame =
            e.stackTrace.firstOrNull()?.className.orEmpty().let {
                it.startsWith("org.apache.parquet") || it.startsWith("shaded.parquet")
            }
        val allowed =
            e is IOException ||
                // parquet-java refuses structurally-broken files with typed
                // subclasses AND bare RuntimeException ("is not a Parquet
                // file", "corrupted file: the footer index..."); a refusal
                // raised from a parquet frame is a refusal.
                (e is RuntimeException && (parquetFrame || e.javaClass.name.startsWith("org.apache.parquet")))
        if (!allowed) {
            throw IllegalStateException("footer parse escaped with untyped ${e.javaClass.name}: ${e.message}", e)
        }
    }

    private fun startsWithMagic(data: ByteArray): Boolean =
        data.size >= 4 && data[0] == 'P'.code.toByte() && data[1] == 'A'.code.toByte() &&
            data[2] == 'R'.code.toByte() && data[3] == '1'.code.toByte()

    /** PAR1 + data-as-thrift-footer + LE footer length + PAR1. */
    private fun wrapAsFooter(data: ByteArray): ByteArray {
        val out = ByteBuffer.allocate(4 + data.size + 8).order(ByteOrder.LITTLE_ENDIAN)
        out.put(MAGIC)
        out.put(data)
        out.putInt(data.size)
        out.put(MAGIC)
        return out.array()
    }

    /** Minimal in-memory [InputFile], the fuzz twin of Hydrator's RegionInputFile. */
    private class BytesInputFile(private val bytes: ByteArray) : InputFile {
        override fun getLength(): Long = bytes.size.toLong()

        override fun newStream(): SeekableInputStream =
            object : SeekableInputStream() {
                private var pos = 0L

                override fun getPos(): Long = pos

                override fun seek(newPos: Long) {
                    pos = newPos
                }

                override fun read(): Int {
                    if (pos >= bytes.size) return -1
                    val v = bytes[posInt(1)].toInt() and 0xFF
                    pos += 1
                    return v
                }

                override fun read(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ): Int {
                    if (len == 0) return 0
                    if (pos >= bytes.size) return -1
                    val n = minOf(len.toLong(), bytes.size - pos).toInt()
                    System.arraycopy(bytes, posInt(n), b, off, n)
                    pos += n
                    return n
                }

                override fun readFully(b: ByteArray) = readFully(b, 0, b.size)

                override fun readFully(
                    b: ByteArray,
                    start: Int,
                    len: Int,
                ) {
                    if (pos + len > bytes.size) throw EOFException("read past end of fuzz input")
                    System.arraycopy(bytes, posInt(len), b, start, len)
                    pos += len
                }

                override fun read(buf: ByteBuffer): Int {
                    val len = buf.remaining()
                    if (len == 0) return 0
                    if (pos >= bytes.size) return -1
                    val n = minOf(len.toLong(), bytes.size - pos).toInt()
                    buf.put(bytes, posInt(n), n)
                    pos += n
                    return n
                }

                override fun readFully(buf: ByteBuffer) {
                    val len = buf.remaining()
                    if (pos + len > bytes.size) throw EOFException("read past end of fuzz input")
                    buf.put(bytes, posInt(len), len)
                    pos += len
                }

                private fun posInt(len: Int): Int {
                    if (pos < 0 || pos + len > bytes.size) {
                        throw EOFException("range [$pos, +$len) outside fuzz input of ${bytes.size} bytes")
                    }
                    return Math.toIntExact(pos)
                }
            }
    }

    private companion object {
        val MAGIC = byteArrayOf(0x50, 0x41, 0x52, 0x31) // "PAR1"
        const val MAX_INPUT_BYTES = 1 shl 20

        /** One catalog column per ColType so aggregate exercises every decode arm. */
        val CATALOG_COLUMNS: List<CatalogColumn> =
            ColType.entries.mapIndexed { i, type ->
                CatalogColumn(
                    fieldId = (i + 1).toLong(),
                    name = "c_${type.wire}",
                    type = type,
                    decimalScale = if (type == ColType.DECIMAL) 2 else null,
                )
            }
    }
}
