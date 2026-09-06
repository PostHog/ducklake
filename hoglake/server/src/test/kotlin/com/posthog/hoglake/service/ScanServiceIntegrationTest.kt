package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * ScanService against a real catalog history: data-file/DV pairing at
 * head and at historical snapshots across a DV supersession chain, plus
 * partitioning metadata on the planned files. DDL state is seeded via
 * direct SQL; commits go through CommitService.
 */
@Tag("integration")
class ScanServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi: Jdbi get() = db.jdbi
    private val commits = CommitService(db.jdbi)
    private val scan = ScanService(db.jdbi)

    @AfterEach
    fun tearDown() = db.close()

    private data class Fixture(val catalogId: Long, val tables: Map<String, Long>)

    /** Catalog "cat" / namespace "ns" with tables; "events" gets a 2-field spec. */
    private fun seed(): Fixture =
        jdbi.withHandle<Fixture, Exception> { h ->
            val catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('cat', 's3://b/cat') RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.createUpdate(
                "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 1, 'ns')",
            ).bind(0, catalogId).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET next_namespace_id = 2, next_table_id = 3 WHERE catalog_id = ?",
            ).bind(0, catalogId).execute()
            val tables =
                listOf("events" to 1L, "plain" to 2L).associate { (name, tableId) ->
                    h.createUpdate(
                        "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, next_field_id) " +
                            "VALUES (?, ?, 0, 3)",
                    ).bind(0, catalogId).bind(1, tableId).execute()
                    h.createUpdate(
                        """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                VALUES (?, ?, 0, 1, ?)
                """,
                    ).bind(0, catalogId).bind(1, tableId).bind(2, name).execute()
                    for (fieldId in 1L..2L) {
                        h.createUpdate(
                            """
                    INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot,
                                            name, col_type, ordinal)
                    VALUES (?, ?, ?, 0, ?, 'string', ?)
                    """,
                        ).bind(0, catalogId).bind(1, tableId).bind(2, fieldId)
                            .bind(3, "col$fieldId").bind(4, (fieldId - 1).toInt()).execute()
                    }
                    h.createUpdate(
                        "INSERT INTO hog_table_stats (catalog_id, table_id) VALUES (?, ?)",
                    ).bind(0, catalogId).bind(1, tableId).execute()
                    name to tableId
                }
            h.createUpdate(
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) VALUES (?, 1, 1, 0)",
            ).bind(0, catalogId).execute()
            h.createUpdate(
                """
            INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, transform)
            VALUES (?, 1, 1, 0, 1, 'day'), (?, 1, 1, 1, 2, 'identity')
            """,
            ).bind(0, catalogId).bind(1, catalogId).execute()
            Fixture(catalogId, tables)
        }

    private fun file(
        path: String,
        records: Long,
        pv: List<String?>? = null,
    ) = FileRegistration(path, records, records * 100, null, null, pv)

    /**
     * History:
     *  snap 1: events f1 (id 1, rows 0..9, pv [2026-01-01, x]),
     *          events f2 (id 2, rows 10..14, pv [2026-01-02, null]),
     *          plain  p1 (id 3, rows 0..6)
     *  snap 2: DV A (id 4) on f1, count 3
     *  snap 3: DV B (id 5) on f1, count 6 — supersedes A
     */
    private fun seedHistory(): Fixture {
        val fx = seed()
        commits.commit(
            "cat",
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "events",
                            listOf(
                                file("s3://b/f1.parquet", 10, listOf("2026-01-01", "x")),
                                file("s3://b/f2.parquet", 5, listOf("2026-01-02", null)),
                            ),
                        ),
                        TableAppend("ns", "plain", listOf(file("s3://b/p1.parquet", 7))),
                    ),
            ),
        )
        commits.commit(
            "cat",
            CommitRequest(
                readSnapshot = 1,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "events",
                            listOf(DeleteFileRegistration(1, "s3://b/dv-a.puffin", 3, 32)),
                        ),
                    ),
            ),
        )
        commits.commit(
            "cat",
            CommitRequest(
                readSnapshot = 2,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "events",
                            listOf(DeleteFileRegistration(1, "s3://b/dv-b.puffin", 6, 48)),
                        ),
                    ),
            ),
        )
        return fx
    }

    @Test
    fun `head plan pairs the live DV and carries partition metadata`() {
        seedHistory()

        val plan = scan.planScan("cat", "ns", "events")
        assertThat(plan).hasSize(2)
        assertThat(plan.map { it.dataFile.rowIdStart }).containsExactly(0L, 10L)

        plan[0].let { sf ->
            assertThat(sf.dataFile.dataFileId).isEqualTo(1)
            assertThat(sf.dataFile.specId).isEqualTo(1)
            assertThat(sf.dataFile.partitionValues).containsExactly("2026-01-01", "x")
            val dv = sf.deleteFile!!
            assertThat(dv.deleteFileId).isEqualTo(5)
            assertThat(dv.dataFileId).isEqualTo(1)
            assertThat(dv.path).isEqualTo("s3://b/dv-b.puffin")
            assertThat(dv.fileFormat).isEqualTo("puffin-dv")
            assertThat(dv.deleteCount).isEqualTo(6)
            assertThat(dv.fileSizeBytes).isEqualTo(48)
            assertThat(dv.beginSnapshot).isEqualTo(3)
        }
        plan[1].let { sf ->
            assertThat(sf.dataFile.dataFileId).isEqualTo(2)
            assertThat(sf.dataFile.partitionValues).containsExactly("2026-01-02", null)
            assertThat(sf.deleteFile).isNull()
        }
    }

    @Test
    fun `time travel returns the DV that was live at the snapshot`() {
        seedHistory()

        // At snapshot 2 the superseded DV A is the visible one.
        val at2 = scan.planScan("cat", "ns", "events", snapshot = 2)
        assertThat(at2[0].deleteFile!!.deleteFileId).isEqualTo(4)
        assertThat(at2[0].deleteFile!!.deleteCount).isEqualTo(3)
        assertThat(at2[0].deleteFile!!.path).isEqualTo("s3://b/dv-a.puffin")
        assertThat(at2[1].deleteFile).isNull()

        // At snapshot 1 no DV existed yet.
        val at1 = scan.planScan("cat", "ns", "events", snapshot = 1)
        assertThat(at1).hasSize(2)
        assertThat(at1.mapNotNull { it.deleteFile }).isEmpty()

        // At snapshot 0 the table had no files.
        assertThat(scan.planScan("cat", "ns", "events", snapshot = 0)).isEmpty()
    }

    @Test
    fun `unpartitioned files plan with null partitioning and no DV`() {
        seedHistory()
        val plan = scan.planScan("cat", "ns", "plain")
        assertThat(plan).hasSize(1)
        assertThat(plan[0].dataFile.specId).isNull()
        assertThat(plan[0].dataFile.partitionValues).isNull()
        assertThat(plan[0].deleteFile).isNull()
    }

    @Test
    fun `resolution failures`() {
        seedHistory()
        assertThatThrownBy { scan.planScan("nope", "ns", "events") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { scan.planScan("cat", "nope", "events") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { scan.planScan("cat", "ns", "ghosts") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { scan.planScan("cat", "ns", "events", snapshot = 99) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }
}
