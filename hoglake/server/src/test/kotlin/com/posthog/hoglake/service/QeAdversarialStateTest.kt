package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Adversarial state (fuzzing.md layer 3): hostile identifiers, boundary
 * payload shapes, and history-shredding sequences, driven through the
 * services against a real Postgres. Includes the parameterization proof
 * (SQL-injection names round-trip; the catalog survives), CHECK
 * boundaries, zero-width row ranges, giant DDL requests, and the
 * 10-incarnation drop/recreate time-travel gauntlet.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QeAdversarialStateTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alters = AlterService(db.jdbi)
    private val views = ViewService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun file(
        path: String,
        records: Long,
    ) = FileRegistration(path = path, recordCount = records, fileSizeBytes = records)

    private fun catalogId(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, name).mapTo(Long::class.java).one()
        }

    // ---- catalog name CHECK boundaries ------------------------------------

    @Test
    fun `catalog name regex boundaries - 63 ok, 64 rejected, shape edges`() {
        val ok63 = "a" + "b".repeat(62)
        assertThat(ok63).hasSize(63)
        assertThat(catalogs.createCatalog(ok63, "s3://qe/adv").name).isEqualTo(ok63)

        val bad64 = "a" + "b".repeat(63)
        assertThatThrownBy { catalogs.createCatalog(bad64, "s3://qe/adv") }
            .isInstanceOf(HoglakeException.Validation::class.java)

        for (bad in listOf("1abc", "Abc", "abC", "-abc", "_abc", "", "a b", "a.b", "abé")) {
            assertThatThrownBy { catalogs.createCatalog(bad, "s3://qe/adv") }
                .describedAs("catalog name '%s' must be rejected", bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        // Hyphens/underscores/digits after the first char are legal.
        assertThat(catalogs.createCatalog("a-b_c9", "s3://qe/adv").name).isEqualTo("a-b_c9")
    }

    // ---- hostile namespace/table/column identifiers -----------------------

    @Test
    fun `injection unicode slash and 10KB names are 422 at every DDL surface`() {
        // POLICY CHANGE (2026-09-05, confirmed consequential by two
        // reviews): namespace/table/view/column names are now validated
        // against ^[A-Za-z_][A-Za-z0-9_-]{0,127}$ (service Validation +
        // DB CHECKs). This test previously pinned VERBATIM STORAGE of
        // these hostile names; it now pins their rejection — and that
        // the catalog survives every attempt untouched.
        val cat = "adv-names"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val headBefore = catalogs.getCatalog(cat).headSnapshotId

        val hostile =
            listOf(
                "'); DROP TABLE hog_catalog;--",
                "таблица_🦔_ライブ",
                "n".repeat(10_240),
                "a/b",
                "<script>alert(1)</script>",
                "a b",
                "a.b",
                "1leading-digit",
                "-leading-hyphen",
                "",
            )
        for (name in hostile) {
            assertThatThrownBy { catalogs.createNamespace(cat, name) }
                .describedAs("namespace name %s", name.take(32))
                .isInstanceOf(HoglakeException.Validation::class.java)
            assertThatThrownBy {
                catalogs.createTable(cat, "ns", name, listOf(ColumnDef("id", ColType.LONG)))
            }.describedAs("table name %s", name.take(32))
                .isInstanceOf(HoglakeException.Validation::class.java)
            assertThatThrownBy {
                catalogs.createTable(cat, "ns", "ok", listOf(ColumnDef(name, ColType.LONG)))
            }.describedAs("column name %s", name.take(32))
                .isInstanceOf(HoglakeException.Validation::class.java)
            assertThatThrownBy { views.create(cat, "ns", name, "SELECT 1") }
                .describedAs("view name %s", name.take(32))
                .isInstanceOf(HoglakeException.Validation::class.java)
            // ALTER rename surfaces enforce the same policy.
            assertThatThrownBy {
                alters.alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable(name)))
            }.isInstanceOf(HoglakeException.Validation::class.java)
            assertThatThrownBy {
                alters.alterTable(cat, "ns", "t", listOf(AlterOp.RenameColumn("id", name)))
            }.isInstanceOf(HoglakeException.Validation::class.java)
            assertThatThrownBy {
                alters.alterTable(cat, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef(name, ColType.INT))))
            }.isInstanceOf(HoglakeException.Validation::class.java)
        }

        // Boundary: exactly 128 chars is legal, 129 is not; underscore
        // start and mixed case are legal.
        val ok128 = "_" + "A".repeat(127)
        assertThat(catalogs.createTable(cat, "ns", ok128, listOf(ColumnDef("Id_9-x", ColType.LONG))).name)
            .isEqualTo(ok128)
        assertThatThrownBy {
            catalogs.createTable(cat, "ns", "_" + "A".repeat(128), listOf(ColumnDef("id", ColType.LONG)))
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // The catalog itself survived every attempt, and no rejected DDL
        // minted a snapshot (the ok128 create minted exactly one).
        db.jdbi.withHandleUnchecked { h ->
            val tables =
                h.createQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'hog_catalog'",
                ).mapTo(Long::class.java).one()
            assertThat(tables).isEqualTo(1)
        }
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore + 1)
    }

    @Test
    fun `empty-string author and message are stored and returned, not nulled`() {
        val cat = "adv-author"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val r =
            commits.commit(
                cat,
                CommitRequest(
                    appends = listOf(TableAppend("ns", "t", listOf(file("s3://qe/$cat/f.parquet", 1)))),
                    author = "",
                    message = "",
                ),
            )
        val (page, _) = catalogs.listSnapshots(cat, after = r.snapshotId - 1, limit = 1)
        assertThat(page.single().author).isEqualTo("")
        assertThat(page.single().message).isEqualTo("")
    }

    // ---- zero-width row ranges + DV extremes -----------------------------

    @Test
    fun `two zero-record files plus a real one keep ranges consistent`() {
        val cat = "adv-zero"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        val t = catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                file("s3://qe/$cat/z1.parquet", 0),
                                file("s3://qe/$cat/z2.parquet", 0),
                                file("s3://qe/$cat/real.parquet", 5),
                                file("s3://qe/$cat/z3.parquet", 0),
                            ),
                        ),
                    ),
            ),
        )
        val cid = catalogId(cat)
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT path, row_id_start, record_count FROM hog_data_file
                WHERE catalog_id = :c AND table_id = :t ORDER BY data_file_id
                """,
                ).bind("c", cid).bind("t", t.tableId)
                    .map { rs, _ -> Triple(rs.getString(1), rs.getLong(2), rs.getLong(3)) }
                    .list()
            }
        // Zero-width ranges collapse onto the running cursor; the real file
        // owns [0, 5); the trailing zero file sits at 5.
        assertThat(rows.map { it.second }).containsExactly(0L, 0L, 0L, 5L)
        val next =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT next_row_id FROM hog_table_stats WHERE catalog_id = ? AND table_id = ?",
                ).bind(0, cid).bind(1, t.tableId).mapTo(Long::class.java).one()
            }
        assertThat(next).isEqualTo(5)
    }

    @Test
    fun `delete_count equal to record_count exactly is accepted, off by one is not`() {
        val cat = "adv-dv"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val appendResult =
            commits.commit(
                cat,
                CommitRequest(
                    appends = listOf(TableAppend("ns", "t", listOf(file("s3://qe/$cat/f.parquet", 7)))),
                ),
            )
        val fileId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT data_file_id FROM hog_data_file WHERE path = 's3://qe/$cat/f.parquet'",
                ).mapTo(Long::class.java).one()
            }
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = appendResult.snapshotId,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(DeleteFileRegistration(fileId, "s3://qe/$cat/dv8.puffin", 8, 1)),
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        val full =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = appendResult.snapshotId,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(DeleteFileRegistration(fileId, "s3://qe/$cat/dv7.puffin", 7, 1)),
                            ),
                        ),
                ),
            )
        assertThat(full.snapshotId).isGreaterThan(appendResult.snapshotId)
    }

    // ---- giant DDL payloads ----------------------------------------------

    @Test
    fun `alter storm - 50 ops in one request is one snapshot with one change row`() {
        val cat = "adv-storm"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val headBefore = catalogs.getCatalog(cat).headSnapshotId

        val ops = mutableListOf<AlterOp>()
        for (i in 0 until 20) ops += AlterOp.AddColumn(ColumnDef("c$i", ColType.INT))
        for (i in 0 until 10) ops += AlterOp.RenameColumn("c$i", "r$i")
        for (i in 0 until 10) ops += AlterOp.PromoteColumn("r$i", ColType.LONG)
        for (i in 10 until 20) ops += AlterOp.DropColumn("c$i")
        assertThat(ops).hasSize(50)

        val result = alters.alterTable(cat, "ns", "t", ops)
        assertThat(result.columns).hasSize(11) // id + r0..r9
        assertThat(result.columns.filter { it.def.name.startsWith("r") })
            .allSatisfy { assertThat(it.def.type).isEqualTo(ColType.LONG) }

        val headAfter = catalogs.getCatalog(cat).headSnapshotId
        assertThat(headAfter).describedAs("exactly one snapshot").isEqualTo(headBefore + 1)
        val changeRows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT count(*) FROM hog_snapshot_change ch
                JOIN hog_catalog c ON c.catalog_id = ch.catalog_id
                WHERE c.name = :n AND ch.snapshot_id = :s
                """,
                ).bind("n", cat).bind("s", headAfter).mapTo(Long::class.java).one()
            }
        assertThat(changeRows).describedAs("one table_altered change row").isEqualTo(1)
    }

    @Test
    fun `200-column table - dense field ids and ordinals`() {
        val cat = "adv-wide"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        val cols = (0 until 200).map { ColumnDef("col_$it", ColType.entries[it % 13]) }
        val t = catalogs.createTable(cat, "ns", "wide", cols)
        assertThat(t.columns).hasSize(200)
        assertThat(t.columns.map { it.fieldId }).isEqualTo((1L..200L).toList())
        assertThat(t.columns.map { it.ordinal }).isEqualTo((0 until 200).toList())
        val reread = catalogs.getTable(cat, "ns", "wide")
        assertThat(reread.columns.map { it.def.name }).isEqualTo(cols.map { it.name })
    }

    @Test
    fun `type_params nested 50 deep round-trips through jsonb`() {
        val cat = "adv-deep"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        var params: Map<String, Any?> = mapOf("leaf" to 1)
        repeat(50) { params = mapOf("a" to params) }
        val t =
            catalogs.createTable(
                cat,
                "ns",
                "deep",
                listOf(ColumnDef("d", ColType.DECIMAL, typeParams = params)),
            )
        assertThat(t.columns.single().def.typeParams).isEqualTo(params)
        val reread = catalogs.getTable(cat, "ns", "deep")
        var node: Any? = reread.columns.single().def.typeParams
        var depth = 0
        while (node is Map<*, *> && node.containsKey("a")) {
            node = node["a"]
            depth++
        }
        assertThat(depth).describedAs("nesting depth survives storage").isEqualTo(50)
        assertThat(node).isEqualTo(mapOf("leaf" to 1))
    }

    @Test
    fun `view with 1MB of SQL is stored and returned verbatim`() {
        val cat = "adv-view"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        val sql =
            buildString {
                append("SELECT ")
                while (length < 1_048_576) append("'x1234567890', ")
                append("1")
            }
        views.create(cat, "ns", "big", sql)
        assertThat(views.get(cat, "ns", "big").sql).isEqualTo(sql)
    }

    // ---- time travel to snapshot 0 + boundary snapshot params -------------

    @Test
    fun `snapshot 0 reads see the empty catalog, 2^62 params are rejected`() {
        val cat = "adv-zero-snap"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        // At snapshot 0 nothing exists yet: the table lookup 404s.
        assertThatThrownBy { catalogs.getTable(cat, "ns", "t", snapshot = 0) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { catalogs.listFiles(cat, "ns", "t", snapshot = 0) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // changes(from=0) over everything works and returns the whole feed.
        val plan = catalogs.changes(cat, "ns", "t", fromSnapshot = 0)
        assertThat(plan.fromSnapshot).isEqualTo(0)

        val big = 1L shl 62
        assertThatThrownBy { catalogs.getTable(cat, "ns", "t", snapshot = big) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { catalogs.changes(cat, "ns", "t", fromSnapshot = big) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            catalogs.commitOffset(cat, "c", java.util.UUID.randomUUID(), big)
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = big,
                    appends = listOf(TableAppend("ns", "t", listOf(file("s3://x/f.parquet", 1)))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- the incarnation gauntlet -----------------------------------------

    @Test
    fun `ten drop-recreate incarnations - time travel returns each incarnation's schema`() {
        val cat = "adv-incarnation"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")

        data class Incarnation(val creationSnapshot: Long, val uuid: java.util.UUID, val col: String)
        val incarnations = mutableListOf<Incarnation>()
        repeat(10) { i ->
            val col = "col_gen_$i"
            val t = catalogs.createTable(cat, "ns", "phoenix", listOf(ColumnDef(col, ColType.LONG)))
            val head = catalogs.getCatalog(cat).headSnapshotId
            incarnations += Incarnation(head, t.tableUuid, col)
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "phoenix", listOf(file("s3://qe/$cat/g$i.parquet", (i + 1).toLong()))),
                        ),
                ),
            )
            catalogs.dropTable(cat, "ns", "phoenix")
        }
        // All ten identities are distinct.
        assertThat(incarnations.map { it.uuid }.toSet()).hasSize(10)
        // Dead at head.
        assertThatThrownBy { catalogs.getTable(cat, "ns", "phoenix") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // At each incarnation's snapshots (creation AND its append), the
        // RIGHT incarnation answers: matching uuid, matching schema, and
        // its own single file at the append snapshot.
        for ((i, inc) in incarnations.withIndex()) {
            for (at in listOf(inc.creationSnapshot, inc.creationSnapshot + 1)) {
                val seen = catalogs.getTable(cat, "ns", "phoenix", snapshot = at)
                assertThat(seen.tableUuid)
                    .describedAs("incarnation %d at snapshot %d", i, at)
                    .isEqualTo(inc.uuid)
                assertThat(seen.columns.single().def.name).isEqualTo(inc.col)
            }
            val files = catalogs.listFiles(cat, "ns", "phoenix", snapshot = inc.creationSnapshot + 1)
            assertThat(files).hasSize(1)
            assertThat(files.single().recordCount).isEqualTo((i + 1).toLong())
            assertThat(files.single().path).isEqualTo("s3://qe/$cat/g$i.parquet")
            // At the snapshot AFTER the drop, the name resolves to nothing
            // (or to the NEXT incarnation once it exists — never this one).
            val afterDrop = inc.creationSnapshot + 2
            val next = incarnations.getOrNull(i + 1)
            if (next == null || afterDrop < next.creationSnapshot) {
                assertThatThrownBy { catalogs.getTable(cat, "ns", "phoenix", snapshot = afterDrop) }
                    .describedAs("gap after incarnation %d", i)
                    .isInstanceOf(HoglakeException.NotFound::class.java)
            }
        }
    }

    // ---- overflow pedantry -----------------------------------------------

    @Test
    fun `row-id allocator overflow is rejected - per-file cap, sum, advancement, DB backstop`() {
        // Regression pin for the QE-found allocator overflow: the
        // per-append record sum and the next_row_id advancement are
        // Math.addExact-checked (Validation), record_count carries a
        // 2^48 per-file sanity cap, and CHECK (next_row_id >= 0) on
        // hog_table_stats is the DB backstop. The lineage guarantee
        // (monotonic per-table ranges, never reused) can no longer be
        // wrapped past 2^63.
        val cat = "adv-overflow"
        catalogs.createCatalog(cat, "s3://qe/$cat")
        catalogs.createNamespace(cat, "ns")
        val t = catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val cid = catalogId(cat)
        val headBefore = catalogs.getCatalog(cat).headSnapshotId

        fun nextRowId(): Long =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT next_row_id FROM hog_table_stats WHERE catalog_id = ? AND table_id = ?",
                ).bind(0, cid).bind(1, t.tableId).mapTo(Long::class.java).one()
            }

        // Per-file sanity cap: 2^48 + 1 rows is not a real file.
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(file("s3://qe/$cat/huge.parquet", (1L shl 48) + 1))),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("per-file cap")

        // Sum overflow: 32768 files at the cap total exactly 2^63.
        val capped = (0 until 32_768).map { file("s3://qe/$cat/s$it.parquet", 1L shl 48) }
        assertThatThrownBy {
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", capped))))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("record_count sum overflows row-id space")

        // Advancement overflow: a near-max allocator plus a valid append
        // fails loudly instead of wrapping negative, leaving the
        // allocator untouched.
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_table_stats SET next_row_id = :n WHERE catalog_id = :c AND table_id = :t",
            ).bind("n", Long.MAX_VALUE - 10).bind("c", cid).bind("t", t.tableId).execute()
        }
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(TableAppend("ns", "t", listOf(file("s3://qe/$cat/adv.parquet", 1000)))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("record_count sum overflows row-id space")
        assertThat(nextRowId()).isEqualTo(Long.MAX_VALUE - 10)

        // DB backstop: the CHECK refuses a negative allocator outright.
        assertThatThrownBy {
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_table_stats SET next_row_id = -1 WHERE catalog_id = :c AND table_id = :t",
                ).bind("c", cid).bind("t", t.tableId).execute()
            }
        }.hasMessageContaining("next_row_id")

        // Every failure rolled back whole: head unmoved, and after a
        // reset the allocator hands out ranges from where it stood.
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore)
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_table_stats SET next_row_id = 0 WHERE catalog_id = :c AND table_id = :t",
            ).bind("c", cid).bind("t", t.tableId).execute()
        }
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(TableAppend("ns", "t", listOf(file("s3://qe/$cat/after.parquet", 1000)))),
            ),
        )
        val laterStart =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT row_id_start FROM hog_data_file WHERE path = 's3://qe/$cat/after.parquet'",
                ).mapTo(Long::class.java).one()
            }
        assertThat(laterStart).isEqualTo(0)
        assertThat(nextRowId()).isEqualTo(1000)
    }
}
