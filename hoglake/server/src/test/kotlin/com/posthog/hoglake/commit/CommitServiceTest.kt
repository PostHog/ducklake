package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
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

@Tag("integration")
class CommitServiceTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi: Jdbi get() = db.jdbi
    private val service = CommitService(db.jdbi)

    @AfterEach
    fun tearDown() = db.close()

    // ------------------------------------------------------------------
    // Fixtures: direct SQL, no dependency on the DDL service.
    // ------------------------------------------------------------------

    private data class Fixture(
        val catalogId: Long,
        val namespaceId: Long,
        /** table name -> (tableId, live field ids) */
        val tables: Map<String, Pair<Long, List<Long>>>,
    )

    private fun seed(
        catalogName: String = "cat",
        namespace: String = "ns",
        tableNames: List<String> = listOf("events"),
        columnsPerTable: Int = 2,
    ): Fixture =
        jdbi.withHandle<Fixture, Exception> { h ->
            val catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES (?, ?) RETURNING catalog_id",
                ).bind(0, catalogName).bind(1, "s3://bucket/$catalogName").mapTo(Long::class.java).one()

            val namespaceId =
                h.createQuery(
                    """
            UPDATE hog_catalog SET next_namespace_id = next_namespace_id + 1
             WHERE catalog_id = ? RETURNING next_namespace_id - 1
            """,
                ).bind(0, catalogId).mapTo(Long::class.java).one()
            h.createUpdate(
                "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, ?, ?)",
            ).bind(0, catalogId).bind(1, namespaceId).bind(2, namespace).execute()

            val tables =
                tableNames.associateWith { name ->
                    val tableId =
                        h.createQuery(
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
                    val fieldIds = (1L..columnsPerTable).toList()
                    for ((ordinal, fieldId) in fieldIds.withIndex()) {
                        h.createUpdate(
                            """
                    INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot,
                                            name, col_type, ordinal)
                    VALUES (?, ?, ?, 0, ?, ?, ?)
                    """,
                        ).bind(0, catalogId).bind(1, tableId).bind(2, fieldId)
                            .bind(3, "col$fieldId").bind(4, if (ordinal == 0) "long" else "string")
                            .bind(5, ordinal).execute()
                    }
                    h.createUpdate(
                        "INSERT INTO hog_table_stats (catalog_id, table_id) VALUES (?, ?)",
                    ).bind(0, catalogId).bind(1, tableId).execute()
                    tableId to fieldIds
                }
            Fixture(catalogId, namespaceId, tables)
        }

    /** Mint a snapshot with one change row via direct SQL (DDL simulation). */
    private fun seedChange(
        catalogId: Long,
        kind: String,
        objectId: Long,
    ): Long =
        jdbi.withHandle<Long, Exception> { h ->
            val snapshotId =
                h.createQuery(
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
        bytes: Long = records * 100,
        footerSize: Long? = 1234,
        stats: List<ColumnStats>? = null,
    ) = FileRegistration(path, records, bytes, footerSize, stats)

    private fun stats(
        fieldId: Long,
        values: Long,
        nulls: Long = 0,
    ) = ColumnStats(
        fieldId = fieldId,
        valueCount = values,
        nullCount = nulls,
        nanCount = null,
        sizeBytes = values * 8,
        lowerBound = byteArrayOf(0),
        upperBound = byteArrayOf(127),
    )

    private data class DbFile(
        val dataFileId: Long,
        val tableId: Long,
        val path: String,
        val recordCount: Long,
        val rowIdStart: Long,
        val statsState: String,
        val beginSnapshot: Long,
        val footerSize: Long?,
    )

    private fun dataFiles(catalogId: Long): List<DbFile> =
        jdbi.withHandle<List<DbFile>, Exception> { h ->
            h.createQuery(
                """
                SELECT data_file_id, table_id, path, record_count, row_id_start,
                       stats_state, begin_snapshot, footer_size
                  FROM hog_data_file WHERE catalog_id = ? ORDER BY data_file_id
                """,
            ).bind(0, catalogId).map { rs, _ ->
                DbFile(
                    rs.getLong(1),
                    rs.getLong(2),
                    rs.getString(3),
                    rs.getLong(4),
                    rs.getLong(5),
                    rs.getString(6),
                    rs.getLong(7),
                    rs.getLong(8).let { if (rs.wasNull()) null else it },
                )
            }.list()
        }

    private fun tableStats(
        catalogId: Long,
        tableId: Long,
    ): Triple<Long, Long, Long> =
        jdbi.withHandle<Triple<Long, Long, Long>, Exception> { h ->
            h.createQuery(
                """
                SELECT record_count, file_size_bytes, next_row_id FROM hog_table_stats
                 WHERE catalog_id = ? AND table_id = ?
                """,
            ).bind(0, catalogId).bind(1, tableId)
                .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }.one()
        }

    private fun snapshotIds(catalogId: Long): List<Long> =
        jdbi.withHandle<List<Long>, Exception> { h ->
            h.createQuery(
                "SELECT snapshot_id FROM hog_snapshot WHERE catalog_id = ? ORDER BY snapshot_id",
            ).bind(0, catalogId).mapTo(Long::class.java).list()
        }

    private fun changeRows(
        catalogId: Long,
        snapshotId: Long,
    ): List<Pair<String, Long>> =
        jdbi.withHandle<List<Pair<String, Long>>, Exception> { h ->
            h.createQuery(
                """
                SELECT kind, object_id FROM hog_snapshot_change
                 WHERE catalog_id = ? AND snapshot_id = ? ORDER BY object_id
                """,
            ).bind(0, catalogId).bind(1, snapshotId)
                .map { rs, _ -> rs.getString(1) to rs.getLong(2) }.list()
        }

    private fun statsRowCount(catalogId: Long): Long =
        jdbi.withHandle<Long, Exception> { h ->
            h.createQuery("SELECT count(*) FROM hog_file_column_stats WHERE catalog_id = ?")
                .bind(0, catalogId).mapTo(Long::class.java).one()
        }

    // ------------------------------------------------------------------
    // Happy paths
    // ------------------------------------------------------------------

    @Test
    fun `single table commit with full stats`() {
        val fx = seed()
        val (tableId, fieldIds) = fx.tables.getValue("events")

        val result =
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 0,
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "events",
                                listOf(
                                    file(
                                        "s3://bucket/cat/f1.parquet",
                                        10,
                                        1000,
                                        stats = fieldIds.map { stats(it, 10) },
                                    ),
                                    file(
                                        "s3://bucket/cat/f2.parquet",
                                        5,
                                        500,
                                        stats = fieldIds.map { stats(it, 5, 1) },
                                    ),
                                ),
                            ),
                        ),
                    author = "jakob",
                    message = "first commit",
                ),
            )

        assertThat(result.snapshotId).isEqualTo(1)
        assertThat(result.schemaVersion).isEqualTo(0)

        // Snapshot row + typed change row.
        val snap =
            jdbi.withHandle<Triple<Long, String?, String?>, Exception> { h ->
                h.createQuery(
                    """
                SELECT schema_version, author, commit_message FROM hog_snapshot
                 WHERE catalog_id = ? AND snapshot_id = 1
                """,
                ).bind(0, fx.catalogId)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getString(2), rs.getString(3)) }.one()
            }
        assertThat(snap).isEqualTo(Triple(0L, "jakob", "first commit"))
        assertThat(changeRows(fx.catalogId, 1))
            .containsExactly("table_inserted_into" to tableId)

        // Files: dense ids from 1, contiguous row-id slices in request order.
        val files = dataFiles(fx.catalogId)
        assertThat(files).hasSize(2)
        assertThat(files.map { it.dataFileId }).containsExactly(1L, 2L)
        assertThat(files[0]).isEqualTo(
            DbFile(1, tableId, "s3://bucket/cat/f1.parquet", 10, 0, "provided", 1, 1234),
        )
        assertThat(files[1]).isEqualTo(
            DbFile(2, tableId, "s3://bucket/cat/f2.parquet", 5, 10, "provided", 1, 1234),
        )

        // Stats rows for every (file, field).
        assertThat(statsRowCount(fx.catalogId)).isEqualTo(4)
        val nullCounts =
            jdbi.withHandle<Map<Long, Long>, Exception> { h ->
                h.createQuery(
                    """
                SELECT field_id, null_count FROM hog_file_column_stats
                 WHERE catalog_id = ? AND data_file_id = 2
                """,
                ).bind(0, fx.catalogId).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.list().toMap()
            }
        assertThat(nullCounts).isEqualTo(mapOf(1L to 1L, 2L to 1L))

        // Rollup + row-id allocator advanced.
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(Triple(15L, 1500L, 15L))
    }

    @Test
    fun `multi table commit and duplicate appends merge`() {
        val fx = seed(tableNames = listOf("events", "persons"))
        val (eventsId, _) = fx.tables.getValue("events")
        val (personsId, _) = fx.tables.getValue("persons")

        val result =
            service.commit(
                "cat",
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "events", listOf(file("s3://b/e1.parquet", 3))),
                            TableAppend("ns", "persons", listOf(file("s3://b/p1.parquet", 7))),
                            // Duplicate (ns, table): merged into the first events append.
                            TableAppend("ns", "events", listOf(file("s3://b/e2.parquet", 4))),
                        ),
                ),
            )

        assertThat(result.snapshotId).isEqualTo(1)
        // One snapshot, one change row per touched table.
        assertThat(snapshotIds(fx.catalogId)).containsExactly(1L)
        assertThat(changeRows(fx.catalogId, 1)).containsExactlyInAnyOrder(
            "table_inserted_into" to eventsId,
            "table_inserted_into" to personsId,
        )

        val byPath = dataFiles(fx.catalogId).associateBy { it.path }
        assertThat(byPath).hasSize(3)
        // Merged events files share one contiguous per-table range in request order.
        assertThat(byPath.getValue("s3://b/e1.parquet").rowIdStart).isEqualTo(0)
        assertThat(byPath.getValue("s3://b/e2.parquet").rowIdStart).isEqualTo(3)
        // persons has its own allocator.
        assertThat(byPath.getValue("s3://b/p1.parquet").rowIdStart).isEqualTo(0)

        assertThat(tableStats(fx.catalogId, eventsId).third).isEqualTo(7)
        assertThat(tableStats(fx.catalogId, personsId).third).isEqualTo(7)
    }

    @Test
    fun `deferred stats registers pending with no stats rows`() {
        val fx = seed()
        service.commit(
            "cat",
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "events", listOf(file("s3://b/f.parquet", 9, stats = null))),
                    ),
            ),
        )
        val files = dataFiles(fx.catalogId)
        assertThat(files).hasSize(1)
        assertThat(files[0].statsState).isEqualTo("pending")
        assertThat(statsRowCount(fx.catalogId)).isEqualTo(0)
    }

    @Test
    fun `consecutive commits produce contiguous non-overlapping row id ranges`() {
        val fx = seed()
        val (tableId, _) = fx.tables.getValue("events")

        service.commit(
            "cat",
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "events",
                            listOf(file("s3://b/a.parquet", 100), file("s3://b/b.parquet", 50)),
                        ),
                    ),
            ),
        )
        service.commit(
            "cat",
            CommitRequest(
                appends = listOf(TableAppend("ns", "events", listOf(file("s3://b/c.parquet", 25)))),
            ),
        )

        val files = dataFiles(fx.catalogId)
        assertThat(files.map { it.path to it.rowIdStart }).containsExactly(
            "s3://b/a.parquet" to 0L,
            "s3://b/b.parquet" to 100L,
            "s3://b/c.parquet" to 150L,
        )
        assertThat(tableStats(fx.catalogId, tableId).third).isEqualTo(175)
    }

    // ------------------------------------------------------------------
    // Conflicts and the append fast path
    // ------------------------------------------------------------------

    @Test
    fun `table_altered since readSnapshot conflicts and writes nothing`() {
        val fx = seed(tableNames = listOf("events"))
        val (tableId, _) = fx.tables.getValue("events")
        seedChange(fx.catalogId, "table_altered", tableId) // snapshot 1

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 0,
                    appends =
                        listOf(
                            TableAppend("ns", "events", listOf(file("s3://b/f.parquet", 10))),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("ns.events")

        // Atomicity: nothing written, no allocator advanced.
        assertThat(snapshotIds(fx.catalogId)).containsExactly(1L) // only the seeded DDL snapshot
        assertThat(dataFiles(fx.catalogId)).isEmpty()
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(Triple(0L, 0L, 0L))
        val (lastSnapshot, nextFileId) = allocators(fx.catalogId)
        assertThat(lastSnapshot).isEqualTo(1)
        assertThat(nextFileId).isEqualTo(1)
    }

    @Test
    fun `table_dropped since readSnapshot conflicts`() {
        val fx = seed(tableNames = listOf("events", "persons"))
        val (eventsId, _) = fx.tables.getValue("events")
        // Change row targeting the events table id (seeded directly; the
        // conflict check keys on object_id, not on current liveness).
        seedChange(fx.catalogId, "table_dropped", eventsId)

        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 0,
                    appends =
                        listOf(
                            TableAppend("ns", "events", listOf(file("s3://b/f.parquet", 1))),
                            TableAppend("ns", "persons", listOf(file("s3://b/g.parquet", 1))),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("ns.events")
        assertThat(dataFiles(fx.catalogId)).isEmpty()
    }

    @Test
    fun `table_inserted_into changes since readSnapshot do not conflict`() {
        val fx = seed()
        service.commit(
            "cat",
            CommitRequest(
                readSnapshot = null,
                appends = listOf(TableAppend("ns", "events", listOf(file("s3://b/a.parquet", 5)))),
            ),
        ) // snapshot 1: table_inserted_into on events

        // readSnapshot 0 predates snapshot 1 — but appends never conflict with appends.
        val result =
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 0,
                    appends = listOf(TableAppend("ns", "events", listOf(file("s3://b/b.parquet", 5)))),
                ),
            )
        assertThat(result.snapshotId).isEqualTo(2)
    }

    @Test
    fun `blind append succeeds despite concurrent DDL history`() {
        val fx = seed()
        val (tableId, _) = fx.tables.getValue("events")
        seedChange(fx.catalogId, "table_altered", tableId)

        val result =
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = null,
                    appends = listOf(TableAppend("ns", "events", listOf(file("s3://b/f.parquet", 2)))),
                ),
            )
        assertThat(result.snapshotId).isEqualTo(2)
        assertThat(dataFiles(fx.catalogId)).hasSize(1)
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    @Test
    fun `unknown catalog is NotFound`() {
        seed()
        assertThatThrownBy {
            service.commit(
                "nope",
                CommitRequest(appends = listOf(TableAppend("ns", "events", listOf(file("p", 1))))),
            )
        }.isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `unknown table is Validation with full rollback`() {
        val fx = seed()
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "events", listOf(file("s3://b/ok.parquet", 1))),
                            TableAppend("ns", "ghosts", listOf(file("s3://b/g.parquet", 1))),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("ns.ghosts")
        assertThat(snapshotIds(fx.catalogId)).isEmpty()
        assertThat(dataFiles(fx.catalogId)).isEmpty()
        assertThat(allocators(fx.catalogId)).isEqualTo(0L to 1L)
    }

    @Test
    fun `bad field id is Validation with full rollback`() {
        val fx = seed(columnsPerTable = 2)
        val (tableId, _) = fx.tables.getValue("events")
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "events",
                                listOf(file("s3://b/f.parquet", 1, stats = listOf(stats(99, 1)))),
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("field_id 99")
        assertThat(snapshotIds(fx.catalogId)).isEmpty()
        assertThat(dataFiles(fx.catalogId)).isEmpty()
        assertThat(statsRowCount(fx.catalogId)).isEqualTo(0)
        assertThat(tableStats(fx.catalogId, tableId)).isEqualTo(Triple(0L, 0L, 0L))
    }

    @Test
    fun `negative counts and blank path are Validation`() {
        seed()
        val cases =
            listOf(
                FileRegistration("  ", 1, 1),
                FileRegistration("s3://b/f.parquet", -1, 1),
                FileRegistration("s3://b/f.parquet", 1, -1),
                FileRegistration("s3://b/f.parquet", 1, 1, columnStats = listOf(stats(1, -1))),
            )
        for (bad in cases) {
            assertThatThrownBy {
                service.commit(
                    "cat",
                    CommitRequest(appends = listOf(TableAppend("ns", "events", listOf(bad)))),
                )
            }.isInstanceOf(HoglakeException.Validation::class.java)
        }
    }

    @Test
    fun `readSnapshot ahead of head is Validation`() {
        seed()
        assertThatThrownBy {
            service.commit(
                "cat",
                CommitRequest(
                    readSnapshot = 5,
                    appends = listOf(TableAppend("ns", "events", listOf(file("s3://b/f.parquet", 1)))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("ahead")
    }

    @Test
    fun `empty appends is Validation`() {
        seed()
        assertThatThrownBy { service.commit("cat", CommitRequest(appends = emptyList())) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    private fun allocators(catalogId: Long): Pair<Long, Long> =
        jdbi.withHandle<Pair<Long, Long>, Exception> { h ->
            h.createQuery(
                "SELECT last_snapshot_id, next_file_id FROM hog_catalog WHERE catalog_id = ?",
            ).bind(0, catalogId).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
        }

    // ------------------------------------------------------------------
    // Concurrency
    // ------------------------------------------------------------------

    @Test
    fun `concurrent commits serialize with dense snapshots and disjoint row ranges`() {
        val threads = 8
        val commitsPerThread = 4
        val tableNames = listOf("t0", "t1", "t2")
        val fx = seed(tableNames = tableNames)

        val pool = Executors.newFixedThreadPool(threads)
        val start = CountDownLatch(1)
        val failures = java.util.concurrent.ConcurrentLinkedQueue<Throwable>()
        try {
            val futures =
                (0 until threads).map { t ->
                    pool.submit {
                        start.await()
                        for (c in 0 until commitsPerThread) {
                            try {
                                // Mix: mostly single-table appends rotating across
                                // tables (same-table contention), every 4th commit
                                // multi-table.
                                val primary = tableNames[(t + c) % tableNames.size]
                                val appends =
                                    mutableListOf(
                                        TableAppend(
                                            "ns",
                                            primary,
                                            listOf(
                                                file("s3://b/$t-$c-0.parquet", (t + 1L) * 10 + c),
                                                file("s3://b/$t-$c-1.parquet", c + 1L),
                                            ),
                                        ),
                                    )
                                if (c % 4 == 3) {
                                    val secondary = tableNames[(t + c + 1) % tableNames.size]
                                    appends +=
                                        TableAppend(
                                            "ns", secondary,
                                            listOf(file("s3://b/$t-$c-2.parquet", 7)),
                                        )
                                }
                                // Half blind, half with a (stale) readSnapshot: only
                                // table_inserted_into changes exist, so both succeed.
                                val readSnapshot = if (c % 2 == 0) null else 0L
                                service.commit("cat", CommitRequest(readSnapshot, appends))
                            } catch (e: Throwable) {
                                failures.add(e)
                            }
                        }
                    }
                }
            start.countDown()
            futures.forEach { it.get(120, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
        assertThat(failures).isEmpty()

        val totalCommits = threads * commitsPerThread
        // Snapshot ids dense and unique: exactly 1..N.
        assertThat(snapshotIds(fx.catalogId))
            .containsExactlyElementsOf((1L..totalCommits).toList())

        val files = dataFiles(fx.catalogId)
        // File ids dense and unique.
        assertThat(files.map { it.dataFileId })
            .containsExactlyElementsOf((1L..files.size).toList())

        for (name in tableNames) {
            val (tableId, _) = fx.tables.getValue(name)
            val tableFiles = files.filter { it.tableId == tableId }.sortedBy { it.rowIdStart }
            val expectedTotal = tableFiles.sumOf { it.recordCount }
            // Ranges non-overlapping and contiguous: sorted starts tile [0, total).
            var cursor = 0L
            for (f in tableFiles) {
                assertThat(f.rowIdStart)
                    .describedAs("row range gap/overlap at ${f.path}")
                    .isEqualTo(cursor)
                cursor += f.recordCount
            }
            assertThat(cursor).isEqualTo(expectedTotal)

            val (recordCount, fileSizeBytes, nextRowId) = tableStats(fx.catalogId, tableId)
            assertThat(recordCount).isEqualTo(expectedTotal)
            assertThat(nextRowId).isEqualTo(expectedTotal)
            assertThat(fileSizeBytes).isEqualTo(expectedTotal * 100)
        }
    }
}
