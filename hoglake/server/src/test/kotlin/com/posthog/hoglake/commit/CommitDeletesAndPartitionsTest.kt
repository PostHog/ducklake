package com.posthog.hoglake.commit

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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * CommitService v2: partition-value registration on appends and
 * deletion-vector (DV) registration. Fixtures are direct SQL (including
 * partition specs) — no dependency on the DDL/alter services.
 */
@Tag("integration")
class CommitDeletesAndPartitionsTest {

    private val db = PgTestSupport.freshDatabase()
    private val jdbi: Jdbi get() = db.jdbi
    private val service = CommitService(db.jdbi)

    @AfterEach
    fun tearDown() = db.close()

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private data class Fixture(
        val catalogId: Long,
        val namespaceId: Long,
        /** table name -> tableId */
        val tables: Map<String, Long>,
    )

    private fun seed(
        catalogName: String = "cat",
        namespace: String = "ns",
        tableNames: List<String> = listOf("events"),
        columnsPerTable: Int = 2,
    ): Fixture = jdbi.withHandle<Fixture, Exception> { h ->
        val catalogId = h.createQuery(
            "INSERT INTO hog_catalog (name, data_path) VALUES (?, ?) RETURNING catalog_id",
        ).bind(0, catalogName).bind(1, "s3://bucket/$catalogName").mapTo(Long::class.java).one()

        val namespaceId = h.createQuery(
            """
            UPDATE hog_catalog SET next_namespace_id = next_namespace_id + 1
             WHERE catalog_id = ? RETURNING next_namespace_id - 1
            """,
        ).bind(0, catalogId).mapTo(Long::class.java).one()
        h.createUpdate(
            "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, ?, ?)",
        ).bind(0, catalogId).bind(1, namespaceId).bind(2, namespace).execute()

        val tables = tableNames.associateWith { name ->
            val tableId = h.createQuery(
                """
                UPDATE hog_catalog SET next_table_id = next_table_id + 1
                 WHERE catalog_id = ? RETURNING next_table_id - 1
                """,
            ).bind(0, catalogId).mapTo(Long::class.java).one()
            h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot, next_field_id)
                VALUES (?, ?, 0, ?)
                """,
            ).bind(0, catalogId).bind(1, tableId).bind(2, columnsPerTable + 1L).execute()
            h.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                VALUES (?, ?, 0, ?, ?)
                """,
            ).bind(0, catalogId).bind(1, tableId).bind(2, namespaceId).bind(3, name).execute()
            for (ordinal in 0 until columnsPerTable) {
                h.createUpdate(
                    """
                    INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot,
                                            name, col_type, ordinal)
                    VALUES (?, ?, ?, 0, ?, ?, ?)
                    """,
                ).bind(0, catalogId).bind(1, tableId).bind(2, ordinal + 1L)
                    .bind(3, "col${ordinal + 1}").bind(4, if (ordinal == 0) "long" else "string")
                    .bind(5, ordinal).execute()
            }
            h.createUpdate(
                "INSERT INTO hog_table_stats (catalog_id, table_id) VALUES (?, ?)",
            ).bind(0, catalogId).bind(1, tableId).execute()
            tableId
        }
        Fixture(catalogId, namespaceId, tables)
    }

    /** Live spec: fields are (source_field_id, transform) in key_index order. */
    private fun seedSpec(
        catalogId: Long,
        tableId: Long,
        fields: List<Pair<Long, String>>,
        specId: Long = 1,
    ) = jdbi.useHandle<Exception> { h ->
        h.createUpdate(
            """
            INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot)
            VALUES (?, ?, ?, 0)
            """,
        ).bind(0, catalogId).bind(1, tableId).bind(2, specId).execute()
        for ((keyIndex, field) in fields.withIndex()) {
            h.createUpdate(
                """
                INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index,
                                                 source_field_id, transform)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
            ).bind(0, catalogId).bind(1, tableId).bind(2, specId).bind(3, keyIndex)
                .bind(4, field.first).bind(5, field.second).execute()
        }
    }

    /** Mint a snapshot with one change row via direct SQL (DDL simulation). */
    private fun seedChange(catalogId: Long, kind: String, objectId: Long): Long =
        jdbi.withHandle<Long, Exception> { h ->
            val snapshotId = h.createQuery(
                """
                UPDATE hog_catalog SET last_snapshot_id = last_snapshot_id + 1
                 WHERE catalog_id = ? RETURNING last_snapshot_id
                """,
            ).bind(0, catalogId).mapTo(Long::class.java).one()
            h.createUpdate(
                """
                INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version)
                SELECT catalog_id, ?, schema_version FROM hog_catalog WHERE catalog_id = ?
                """,
            ).bind(0, snapshotId).bind(1, catalogId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
                VALUES (?, ?, ?, ?)
                """,
            ).bind(0, catalogId).bind(1, snapshotId).bind(2, kind).bind(3, objectId).execute()
            snapshotId
        }

    private fun file(
        path: String,
        records: Long,
        partitionValues: List<String?>? = null,
    ) = FileRegistration(path, records, records * 100, 1234, null, partitionValues)

    private fun del(dataFileId: Long, count: Long, path: String = "s3://b/dv-$dataFileId-$count.puffin") =
        DeleteFileRegistration(dataFileId, path, count, 64)

    private fun append(table: String, vararg files: FileRegistration) =
        TableAppend("ns", table, files.toList())

    private fun deletes(table: String, vararg files: DeleteFileRegistration) =
        TableDeletes("ns", table, files.toList())

    private data class DvRow(
        val deleteFileId: Long,
        val tableId: Long,
        val dataFileId: Long,
        val beginSnapshot: Long,
        val endSnapshot: Long?,
        val path: String,
        val fileFormat: String,
        val deleteCount: Long,
        val fileSizeBytes: Long,
    )

    private fun dvRows(catalogId: Long): List<DvRow> =
        jdbi.withHandle<List<DvRow>, Exception> { h ->
            h.createQuery(
                """
                SELECT delete_file_id, table_id, data_file_id, begin_snapshot, end_snapshot,
                       path, file_format, delete_count, file_size_bytes
                  FROM hog_delete_file WHERE catalog_id = ? ORDER BY delete_file_id
                """,
            ).bind(0, catalogId).map { rs, _ ->
                DvRow(
                    rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4),
                    rs.getLong(5).let { if (rs.wasNull()) null else it },
                    rs.getString(6), rs.getString(7), rs.getLong(8), rs.getLong(9),
                )
            }.list()
        }

    private fun dataFileCount(catalogId: Long): Long =
        jdbi.withHandle<Long, Exception> { h ->
            h.createQuery("SELECT count(*) FROM hog_data_file WHERE catalog_id = ?")
                .bind(0, catalogId).mapTo(Long::class.java).one()
        }

    private fun tableStats(catalogId: Long, tableId: Long): Triple<Long, Long, Long> =
        jdbi.withHandle<Triple<Long, Long, Long>, Exception> { h ->
            h.createQuery(
                """
                SELECT record_count, file_size_bytes, next_row_id FROM hog_table_stats
                 WHERE catalog_id = ? AND table_id = ?
                """,
            ).bind(0, catalogId).bind(1, tableId)
                .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }.one()
        }

    private fun changeRows(catalogId: Long, snapshotId: Long): List<Pair<String, Long>> =
        jdbi.withHandle<List<Pair<String, Long>>, Exception> { h ->
            h.createQuery(
                """
                SELECT kind, object_id FROM hog_snapshot_change
                 WHERE catalog_id = ? AND snapshot_id = ? ORDER BY kind, object_id
                """,
            ).bind(0, catalogId).bind(1, snapshotId)
                .map { rs, _ -> rs.getString(1) to rs.getLong(2) }.list()
        }

    private fun allocators(catalogId: Long): Pair<Long, Long> =
        jdbi.withHandle<Pair<Long, Long>, Exception> { h ->
            h.createQuery(
                "SELECT last_snapshot_id, next_file_id FROM hog_catalog WHERE catalog_id = ?",
            ).bind(0, catalogId).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
        }

    private fun specAndValues(catalogId: Long, dataFileId: Long): Pair<Long?, List<String?>> =
        jdbi.withHandle<Pair<Long?, List<String?>>, Exception> { h ->
            val specId = h.createQuery(
                "SELECT spec_id FROM hog_data_file WHERE catalog_id = ? AND data_file_id = ?",
            ).bind(0, catalogId).bind(1, dataFileId)
                .map { rs, _ -> rs.getLong(1).let { if (rs.wasNull()) null else it } }.one()
            val values = h.createQuery(
                """
                SELECT value FROM hog_file_partition_value
                 WHERE catalog_id = ? AND data_file_id = ? ORDER BY key_index
                """,
            ).bind(0, catalogId).bind(1, dataFileId)
                .map { rs, _ -> rs.getString(1) }.list()
            specId to values
        }

    // ------------------------------------------------------------------
    // Partitioned appends
    // ------------------------------------------------------------------

    @Test
    fun `partitioned append lands spec_id and partition values`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        seedSpec(fx.catalogId, tableId, listOf(1L to "day", 2L to "identity"))

        service.commit(
            "cat",
            CommitRequest(
                appends = listOf(
                    append(
                        "events",
                        file("s3://b/f1.parquet", 10, listOf("2026-01-01", "x")),
                        file("s3://b/f2.parquet", 5, listOf("2026-01-02", null)),
                    ),
                ),
            ),
        )

        assertThat(specAndValues(fx.catalogId, 1))
            .isEqualTo(1L to listOf("2026-01-01", "x"))
        assertThat(specAndValues(fx.catalogId, 2))
            .isEqualTo(1L to listOf<String?>("2026-01-02", null))
    }

    @Test
    fun `partition arity mismatch is Validation`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        seedSpec(fx.catalogId, tableId, listOf(1L to "day", 2L to "identity"))

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    appends = listOf(append("events", file("s3://b/f.parquet", 1, listOf("only-one")))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("arity")
        assertThat(dataFileCount(fx.catalogId)).isEqualTo(0)
    }

    @Test
    fun `missing partition values on a partitioned table is Validation`() {
        val fx = seed()
        seedSpec(fx.catalogId, fx.tables.getValue("events"), listOf(1L to "identity"))

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(appends = listOf(append("events", file("s3://b/f.parquet", 1)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("no partition_values")
    }

    @Test
    fun `partition values on an unpartitioned table is Validation`() {
        seed()
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    appends = listOf(append("events", file("s3://b/f.parquet", 1, listOf("v")))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("not partitioned")
    }

    // ------------------------------------------------------------------
    // Deletes: happy path + supersession
    // ------------------------------------------------------------------

    @Test
    fun `delete registers a DV without touching table stats`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snapshot 1, data file 1
        val statsBefore = tableStats(fx.catalogId, tableId)

        val result = service.commit(
            "cat",
            CommitRequest(
                readSnapshot = 1,
                deletes = listOf(deletes("events", del(1, 3, "s3://b/dv1.puffin"))),
            ),
        )

        assertThat(result.snapshotId).isEqualTo(2)
        assertThat(dvRows(fx.catalogId)).containsExactly(
            DvRow(2, tableId, 1, 2, null, "s3://b/dv1.puffin", "puffin-dv", 3, 64),
        )
        assertThat(changeRows(fx.catalogId, 2))
            .containsExactly("table_deleted_from" to tableId)
        // Gross append counters untouched: net liveness is a read-time concern.
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(statsBefore)
    }

    @Test
    fun `DV supersession chain is monotonic and snapshot-correct`() {
        val fx = seed()
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snap 1, file 1
        service.commit(
            "cat",
            CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 3)))),
        ) // snap 2, DV id 2
        service.commit(
            "cat",
            CommitRequest(readSnapshot = 2, deletes = listOf(deletes("events", del(1, 5)))),
        ) // snap 3, DV id 3 supersedes id 2

        val rows = dvRows(fx.catalogId)
        assertThat(rows).hasSize(2)
        assertThat(rows[0].deleteFileId).isEqualTo(2)
        assertThat(rows[0].endSnapshot).isEqualTo(3)
        assertThat(rows[1].deleteFileId).isEqualTo(3)
        assertThat(rows[1].endSnapshot).isNull()
        assertThat(rows[1].deleteCount).isEqualTo(5)

        // Shrink -> Validation.
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 3, deletes = listOf(deletes("events", del(1, 4)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("only grow")

        // Equal count is allowed (covers the old vector).
        service.commit(
            "cat",
            CommitRequest(readSnapshot = 3, deletes = listOf(deletes("events", del(1, 5)))),
        ) // snap 4
        val after = dvRows(fx.catalogId)
        assertThat(after.filter { it.endSnapshot == null }).hasSize(1)
        assertThat(after.last().beginSnapshot).isEqualTo(4)
    }

    @Test
    fun `DV registered after readSnapshot is CommitConflict`() {
        val fx = seed()
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snap 1
        service.commit(
            "cat",
            CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 3)))),
        ) // snap 2: the concurrent DV

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 7)))),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("superseded")
        assertThat(dvRows(fx.catalogId).filter { it.endSnapshot == null }).hasSize(1)
    }

    @Test
    fun `deletes without readSnapshot are Validation`() {
        seed()
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        )
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(deletes = listOf(deletes("events", del(1, 1)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("read_snapshot")
    }

    @Test
    fun `empty appends and deletes is Validation`() {
        seed()
        assertThatThrownBy { service.commit("cat", CommitRequest()) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ------------------------------------------------------------------
    // Delete target validation
    // ------------------------------------------------------------------

    @Test
    fun `delete target validation failures`() {
        val fx = seed(tableNames = listOf("events", "persons"))
        val eventsId = fx.tables.getValue("events")
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snap 1, file 1 in events

        // Unknown data file.
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(99, 1)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("unknown data_file_id 99")

        // File belongs to another table.
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("persons", del(1, 1)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("another table")

        // delete_count out of range.
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 11)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("exceeds record_count")
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 0)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Duplicate targets in one request.
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 1,
                    deletes = listOf(deletes("events", del(1, 1), del(1, 2))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("duplicate delete target")

        // Dead (end-snapshotted) data file.
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE hog_data_file SET end_snapshot = begin_snapshot + 1
                 WHERE catalog_id = ? AND data_file_id = 1
                """,
            ).bind(0, fx.catalogId).execute()
        }
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 1)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("no longer live")

        assertThat(dvRows(fx.catalogId)).isEmpty()
        assertThat(tableStats(fx.catalogId, eventsId)).isEqualTo(Triple(10L, 1000L, 10L))
    }

    @Test
    fun `table_altered since readSnapshot conflicts a delete commit`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snap 1
        seedChange(fx.catalogId, "table_altered", tableId) // snap 2

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(readSnapshot = 1, deletes = listOf(deletes("events", del(1, 1)))),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("ns.events")
        assertThat(dvRows(fx.catalogId)).isEmpty()
    }

    // ------------------------------------------------------------------
    // Mixed commits
    // ------------------------------------------------------------------

    @Test
    fun `mixed append and delete on one table succeeds with both change kinds`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snap 1, file 1

        val result = service.commit(
            "cat",
            CommitRequest(
                readSnapshot = 1,
                appends = listOf(append("events", file("s3://b/f2.parquet", 4))),
                deletes = listOf(deletes("events", del(1, 2, "s3://b/dv.puffin"))),
            ),
        ) // snap 2: data file 2, DV file 3 (shared allocator)

        assertThat(result.snapshotId).isEqualTo(2)
        assertThat(changeRows(fx.catalogId, 2)).containsExactly(
            "table_deleted_from" to tableId,
            "table_inserted_into" to tableId,
        )
        assertThat(dvRows(fx.catalogId)).containsExactly(
            DvRow(3, tableId, 1, 2, null, "s3://b/dv.puffin", "puffin-dv", 2, 64),
        )
        // Stats reflect the append only.
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(Triple(14L, 1400L, 14L))
    }

    @Test
    fun `delete targeting a data file created in the same commit is Validation`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        // The appended file would get data_file_id 1; the delete targets it.
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 0,
                    appends = listOf(append("events", file("s3://b/f1.parquet", 10))),
                    deletes = listOf(deletes("events", del(1, 1))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("same commit")

        // Full rollback: the appends died with the delete.
        assertThat(dataFileCount(fx.catalogId)).isEqualTo(0)
        assertThat(dvRows(fx.catalogId)).isEmpty()
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(Triple(0L, 0L, 0L))
        assertThat(allocators(fx.catalogId)).isEqualTo(0L to 1L)
    }

    @Test
    fun `bad delete rolls back the appends in the same commit`() {
        val fx = seed()
        val tableId = fx.tables.getValue("events")
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 10)))),
        ) // snap 1
        val allocatorsBefore = allocators(fx.catalogId)

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 1,
                    appends = listOf(append("events", file("s3://b/f2.parquet", 4))),
                    deletes = listOf(deletes("events", del(99, 1))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("unknown data_file_id 99")

        assertThat(dataFileCount(fx.catalogId)).isEqualTo(1)
        assertThat(dvRows(fx.catalogId)).isEmpty()
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(Triple(10L, 1000L, 10L))
        assertThat(allocators(fx.catalogId)).isEqualTo(allocatorsBefore)
    }

    // ------------------------------------------------------------------
    // Concurrency
    // ------------------------------------------------------------------

    @Test
    fun `concurrent deletes of one data file - exactly one wins`() {
        val fx = seed()
        service.commit(
            "cat",
            CommitRequest(appends = listOf(append("events", file("s3://b/f1.parquet", 100)))),
        ) // snap 1, file 1

        val pool = Executors.newFixedThreadPool(2)
        val start = CountDownLatch(1)
        val outcomes = java.util.concurrent.ConcurrentLinkedQueue<Any>()
        try {
            val futures = (0 until 2).map { i ->
                pool.submit {
                    start.await()
                    try {
                        outcomes.add(
                            service.commit(
                                "cat",
                                CommitRequest(
                                    readSnapshot = 1,
                                    deletes = listOf(
                                        deletes("events", del(1, 10, "s3://b/dv-$i.puffin")),
                                    ),
                                ),
                            ),
                        )
                    } catch (e: Throwable) {
                        outcomes.add(e)
                    }
                }
            }
            start.countDown()
            futures.forEach { it.get(60, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        val successes = outcomes.filterIsInstance<com.posthog.hoglake.model.CommitResult>()
        val conflicts = outcomes.filterIsInstance<HoglakeException.CommitConflict>()
        assertThat(successes).hasSize(1)
        assertThat(conflicts).hasSize(1)

        // No lost update, and the one-live-DV invariant holds.
        val rows = dvRows(fx.catalogId)
        assertThat(rows).hasSize(1)
        assertThat(rows[0].endSnapshot).isNull()
        assertThat(rows[0].beginSnapshot).isEqualTo(successes[0].snapshotId)
    }
}
