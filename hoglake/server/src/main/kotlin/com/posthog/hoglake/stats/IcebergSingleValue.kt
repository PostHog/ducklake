package com.posthog.hoglake.stats

import com.posthog.hoglake.model.ColType
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * Iceberg "Binary single-value serialization"
 * (https://iceberg.apache.org/spec/#binary-single-value-serialization) —
 * the encoding used for manifest `lower_bounds` / `upper_bounds` and for
 * `hog_file_column_stats.lower_bound` / `upper_bound` (V1__init.sql),
 * so manifest generation for the Iceberg facade is a mechanical copy.
 *
 * Encodings:
 *  - boolean: 1 byte, 0x00 = false / 0x01 = true
 *  - int: 4-byte little-endian
 *  - long: 8-byte little-endian
 *  - float: 4-byte IEEE-754, little-endian
 *  - double: 8-byte IEEE-754, little-endian
 *  - date: days since 1970-01-01 as int, 4-byte little-endian
 *  - time: microseconds since midnight as long, 8-byte little-endian
 *  - timestamp/timestamptz: microseconds since epoch as long, 8-byte LE
 *  - string: UTF-8 bytes, no length prefix
 *  - uuid: 16 bytes, big-endian (most significant byte first)
 *  - binary: the bytes themselves
 *  - decimal: unscaled value as minimal two's-complement big-endian
 */
object IcebergSingleValue {
    fun encodeBoolean(value: Boolean): ByteArray = byteArrayOf(if (value) 0x01 else 0x00)

    fun encodeInt(value: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array()

    fun encodeLong(value: Long): ByteArray =
        ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array()

    fun encodeFloat(value: Float): ByteArray =
        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array()

    fun encodeDouble(value: Double): ByteArray =
        ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array()

    /** Date as days since the unix epoch (may be negative). */
    fun encodeDate(daysSinceEpoch: Int): ByteArray = encodeInt(daysSinceEpoch)

    fun encodeDate(value: LocalDate): ByteArray =
        try {
            encodeDate(Math.toIntExact(value.toEpochDay()))
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "date $value is outside the encodable range (int32 epoch days)",
            )
        }

    /** Time of day as microseconds since midnight. */
    fun encodeTimeMicros(microsSinceMidnight: Long): ByteArray = encodeLong(microsSinceMidnight)

    fun encodeTime(value: LocalTime): ByteArray = encodeTimeMicros(value.toNanoOfDay() / 1_000)

    /** Timestamp (with or without zone) as microseconds since the unix epoch. */
    fun encodeTimestampMicros(microsSinceEpoch: Long): ByteArray = encodeLong(microsSinceEpoch)

    fun encodeTimestamp(value: LocalDateTime): ByteArray = encodeTimestamptz(value.toInstant(ZoneOffset.UTC))

    fun encodeTimestamptz(value: Instant): ByteArray =
        try {
            encodeTimestampMicros(
                Math.addExact(
                    Math.multiplyExact(value.epochSecond, 1_000_000L),
                    (value.nano / 1_000).toLong(),
                ),
            )
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "timestamp $value is outside the encodable range (int64 micros since epoch)",
            )
        }

    fun encodeString(value: String): ByteArray = value.toByteArray(Charsets.UTF_8)

    /** UUID as 16 bytes, most significant byte first. */
    fun encodeUuid(value: UUID): ByteArray =
        ByteBuffer.allocate(16)
            .putLong(value.mostSignificantBits)
            .putLong(value.leastSignificantBits)
            .array()

    fun encodeBinary(value: ByteArray): ByteArray = value.copyOf()

    /**
     * Decimal as the minimal two's-complement big-endian bytes of the
     * unscaled value. The scale is carried by the column type, not the
     * serialized value; callers must ensure [unscaled] is already at the
     * column's scale.
     */
    fun encodeDecimalUnscaled(unscaled: BigInteger): ByteArray = unscaled.toByteArray()

    fun encodeDecimal(value: BigDecimal): ByteArray = encodeDecimalUnscaled(value.unscaledValue())

    /**
     * Encode [value] for catalog column type [type]. Accepts the natural
     * JVM primitive for each type plus the obvious java.time /
     * java.math companions. Throws [IllegalArgumentException] on a
     * type/value mismatch — bounds must never be guessed.
     */
    fun encode(
        type: ColType,
        value: Any,
    ): ByteArray =
        when (type) {
            ColType.BOOLEAN -> encodeBoolean(expect(type, value))
            ColType.INT -> encodeInt(expect(type, value))
            ColType.LONG ->
                when (value) {
                    is Long -> encodeLong(value)
                    is Int -> encodeLong(value.toLong())
                    else -> mismatch(type, value)
                }
            ColType.FLOAT -> encodeFloat(expect(type, value))
            ColType.DOUBLE -> encodeDouble(expect(type, value))
            ColType.DATE ->
                when (value) {
                    is Int -> encodeDate(value)
                    is LocalDate -> encodeDate(value)
                    else -> mismatch(type, value)
                }
            ColType.TIME ->
                when (value) {
                    is Long -> encodeTimeMicros(value)
                    is LocalTime -> encodeTime(value)
                    else -> mismatch(type, value)
                }
            ColType.TIMESTAMP ->
                when (value) {
                    is Long -> encodeTimestampMicros(value)
                    is LocalDateTime -> encodeTimestamp(value)
                    else -> mismatch(type, value)
                }
            ColType.TIMESTAMPTZ ->
                when (value) {
                    is Long -> encodeTimestampMicros(value)
                    is Instant -> encodeTimestamptz(value)
                    else -> mismatch(type, value)
                }
            ColType.STRING -> encodeString(expect(type, value))
            ColType.UUID_T -> encodeUuid(expect(type, value))
            ColType.BINARY -> encodeBinary(expect(type, value))
            ColType.DECIMAL ->
                when (value) {
                    is BigDecimal -> encodeDecimal(value)
                    is BigInteger -> encodeDecimalUnscaled(value)
                    else -> mismatch(type, value)
                }
        }

    /**
     * Inverse of [encode] — mirrors pyhoglake's `decode_bound` (the two
     * codecs are kept bit-identical by the shared vector file
     * pyhoglake/tests/vectors/bounds_vectors.json). Returns the natural
     * JVM value for each type: Boolean, Int (int/date-days), Long
     * (long/time/timestamp micros), Float, Double, String, UUID,
     * ByteArray (binary), BigInteger (decimal UNSCALED — the scale is
     * carried by the column type, exactly like [encodeDecimalUnscaled]).
     * The invariant `encode(type, decode(type, b)) contentEquals b`
     * holds for every well-formed encoding.
     */
    fun decode(
        type: ColType,
        data: ByteArray,
    ): Any =
        when (type) {
            ColType.BOOLEAN -> {
                expectLength(type, data, 1)
                data[0] != 0.toByte()
            }
            ColType.INT, ColType.DATE -> {
                expectLength(type, data, 4)
                ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).int
            }
            ColType.LONG, ColType.TIME, ColType.TIMESTAMP, ColType.TIMESTAMPTZ -> {
                expectLength(type, data, 8)
                ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).long
            }
            ColType.FLOAT -> {
                expectLength(type, data, 4)
                Float.fromBits(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).int)
            }
            ColType.DOUBLE -> {
                expectLength(type, data, 8)
                Double.fromBits(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).long)
            }
            ColType.STRING -> String(data, Charsets.UTF_8)
            ColType.UUID_T -> {
                expectLength(type, data, 16)
                val buf = ByteBuffer.wrap(data)
                UUID(buf.long, buf.long)
            }
            ColType.BINARY -> data.copyOf()
            ColType.DECIMAL -> {
                require(data.isNotEmpty()) { "empty decimal encoding" }
                BigInteger(data)
            }
        }

    /**
     * Typed comparison of two [decode]d values for [type]. STRING
     * compares as UTF-8 bytes unsigned (== code-point order — Java's
     * String.compareTo is UTF-16 unit order, which disagrees above the
     * BMP); UUID/BINARY compare bytes unsigned; everything else through
     * its natural Comparable. This is what makes compaction's
     * bounds-merge correct where a raw byte compare of the ENCODINGS
     * would not be (signed little-endian ints do not sort bytewise).
     */
    @Suppress("UNCHECKED_CAST")
    fun compareValues(
        type: ColType,
        a: Any,
        b: Any,
    ): Int =
        when (type) {
            ColType.STRING ->
                java.util.Arrays.compareUnsigned(
                    (a as String).toByteArray(Charsets.UTF_8),
                    (b as String).toByteArray(Charsets.UTF_8),
                )
            ColType.BINARY ->
                java.util.Arrays.compareUnsigned(a as ByteArray, b as ByteArray)
            ColType.UUID_T ->
                java.util.Arrays.compareUnsigned(encodeUuid(a as UUID), encodeUuid(b as UUID))
            else -> (a as Comparable<Any>).compareTo(b)
        }

    private fun expectLength(
        type: ColType,
        data: ByteArray,
        expected: Int,
    ) {
        require(data.size == expected) {
            "cannot decode ${type.wire}: expected $expected bytes, got ${data.size}"
        }
    }

    private inline fun <reified T> expect(
        type: ColType,
        value: Any,
    ): T = value as? T ?: mismatch(type, value)

    private fun mismatch(
        type: ColType,
        value: Any,
    ): Nothing =
        throw IllegalArgumentException(
            "cannot encode ${value::class.qualifiedName} as Iceberg single-value for column type ${type.wire}",
        )
}
