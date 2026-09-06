package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Regression for the pyhoglake-found bug (2026-09-05): getTable at a
 * historical snapshot returned head-scoped record_count/file_size_bytes
 * (from hog_table_stats) while file_count was snapshot-scoped. All
 * TableInfo aggregates must come from the files visible at the
 * requested snapshot.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TimeTravelAggregatesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `table aggregates are snapshot scoped`() {
        catalogs.createCatalog("tta", "s3://tta/")
        catalogs.createNamespace("tta", "ns")
        catalogs.createTable("tta", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        fun append(
            path: String,
            rows: Long,
            bytes: Long,
        ) = commits.commit(
            "tta",
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration(path, rows, bytes))),
                    ),
            ),
        )

        val first = append("s3://tta/a.parquet", 10, 1000)
        val second = append("s3://tta/b.parquet", 5, 500)

        val atFirst = catalogs.getTable("tta", "ns", "t", snapshot = first.snapshotId)
        assertThat(atFirst.fileCount).isEqualTo(1)
        assertThat(atFirst.recordCount).isEqualTo(10)
        assertThat(atFirst.fileSizeBytes).isEqualTo(1000)

        val atHead = catalogs.getTable("tta", "ns", "t")
        assertThat(atHead.fileCount).isEqualTo(2)
        assertThat(atHead.recordCount).isEqualTo(15)
        assertThat(atHead.fileSizeBytes).isEqualTo(1500)

        // listTables aggregates agree with head.
        val listed = catalogs.listTables("tta", "ns").single()
        assertThat(listed.recordCount).isEqualTo(15)
        assertThat(listed.fileSizeBytes).isEqualTo(1500)
        assertThat(second.snapshotId).isGreaterThan(first.snapshotId)
    }
}
