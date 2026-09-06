package com.posthog.hoglake.trino.testing

import dev.hardwood.OutputFile
import dev.hardwood.metadata.LogicalType
import dev.hardwood.metadata.PhysicalType
import dev.hardwood.metadata.RepetitionType
import dev.hardwood.schema.FileSchema
import dev.hardwood.writer.ParquetFileWriter
import dev.hardwood.writer.WriterConfig
import java.nio.file.Files
import java.time.Instant

/**
 * The known parquet fixture, written with Hardwood exactly like the root
 * project's hydrator tests (Hardwood 1.1.0.Beta1 writes no
 * PARQUET:field_id, so connector reads exercise the name-fallback
 * binding). 25 rows in row groups of <= 10:
 *
 *   id    (long, required) : 0..24
 *   score (double)         : null when i % 5 == 0, else i * 1.5
 *   name  (string)         : null when i == 13, else "row-%02d"
 *   ts    (timestamptz)    : epoch second 1_700_000_000 + i, micros
 */
object TestParquet {
    const val ROWS = 25
    const val EPOCH0 = 1_700_000_000L

    /**
     * A second id-less fixture for the type-promotion integration test:
     *
     *   id (long, required) : 0..9
     *   n  (int32, optional): i * 10
     */
    const val INT_ROWS = 10

    @JvmStatic
    fun writeIntColumnParquet(): ByteArray {
        val schema = FileSchema.builder("ints")
            .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("n", PhysicalType.INT32, RepetitionType.OPTIONAL)
            .build()
        val tmp = Files.createTempFile("hoglake-trino-int", ".parquet")
        try {
            ParquetFileWriter.create(
                OutputFile.of(tmp), schema,
                WriterConfig.builder().rowGroupTargetRows(10).build(),
            ).use { writer ->
                val rows = writer.rowWriter()
                for (i in 0 until INT_ROWS) {
                    rows.writeRow { r ->
                        r.setLong("id", i.toLong())
                        r.setInt("n", i * 10)
                    }
                }
            }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    @JvmStatic
    fun writeSampleParquet(): ByteArray {
        val schema = FileSchema.builder("events")
            .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("score", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
            .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.StringType())
            .addColumn(
                "ts", PhysicalType.INT64, RepetitionType.OPTIONAL,
                LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS),
            )
            .build()
        val tmp = Files.createTempFile("hoglake-trino", ".parquet")
        try {
            ParquetFileWriter.create(
                OutputFile.of(tmp), schema,
                WriterConfig.builder().rowGroupTargetRows(10).build(),
            ).use { writer ->
                val rows = writer.rowWriter()
                for (i in 0 until ROWS) {
                    rows.writeRow { r ->
                        r.setLong("id", i.toLong())
                        if (i % 5 == 0) r.setNull("score") else r.setDouble("score", i * 1.5)
                        if (i == 13) r.setNull("name") else r.setString("name", "row-%02d".format(i))
                        r.setTimestamp("ts", Instant.ofEpochSecond(EPOCH0 + i))
                    }
                }
            }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }
}
