package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * AlterService: op semantics, in-order application, time travel,
 * partition-spec lifecycle, and the one-snapshot/one-change contract.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val alter = AlterService(db.jdbi)
    private val catalogs = CatalogService(db.jdbi)

    private val counter = AtomicInteger(0)

    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)
    private val nameCol = ColumnDef("name", ColType.STRING)
    private val scoreCol = ColumnDef("score", ColType.FLOAT)
    private val countCol = ColumnDef("count", ColType.INT)
    private val tsCol = ColumnDef("ts", ColType.TIMESTAMPTZ)

    @AfterAll
    fun tearDown() = db.close()

    /** Fresh catalog + namespace + one table with the five stock columns. */
    private fun fixture(): Pair<String, String> {
        val cat = "alter-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(idCol, nameCol, scoreCol, countCol, tsCol))
        return cat to "ns"
    }

    private fun head(cat: String) = catalogs.getCatalog(cat).headSnapshotId

    private fun catId(cat: String): Long = catalogs.getCatalog(cat).catalogId

    // ---- happy paths, one op each ----------------------------------------

    @Test
    fun `add column appends with fresh field_id and next ordinal`() {
        val (cat, ns) = fixture()
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("tags", ColType.STRING)),
                ),
            )
        val added = info.columns.single { it.def.name == "tags" }
        assertThat(added.fieldId).isEqualTo(6) // fields 1..5 existed
        assertThat(added.ordinal).isEqualTo(5)
        assertThat(info.columns).hasSize(6)
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name })
            .containsExactly("id", "name", "score", "count", "ts", "tags")
    }

    @Test
    fun `drop column removes it live but keeps it at the old snapshot`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("name")))
        assertThat(info.columns.map { it.def.name })
            .containsExactly("id", "score", "count", "ts")
        assertThat(catalogs.getTable(cat, ns, "t", snapshot = before).columns.map { it.def.name })
            .containsExactly("id", "name", "score", "count", "ts")
    }

    @Test
    fun `rename column keeps field_id and ordinal, time travel shows old name`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val oldCol = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "name" }

        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        val renamed = info.columns.single { it.def.name == "label" }
        assertThat(renamed.fieldId).isEqualTo(oldCol.fieldId)
        assertThat(renamed.ordinal).isEqualTo(oldCol.ordinal)
        assertThat(renamed.def.type).isEqualTo(ColType.STRING)

        val old =
            catalogs.getTable(cat, ns, "t", snapshot = before)
                .columns.single { it.fieldId == oldCol.fieldId }
        assertThat(old.def.name).isEqualTo("name")
    }

    @Test
    fun `rename column is refused while a live id-less data file exists`() {
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        // A LIVE file flagged missing_field_ids: it binds columns by name,
        // so a rename would silently NULL its history in readers.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, missing_field_ids)
                VALUES (?, 1, ?, 2, 's3://bucket/idless.parquet', 10, 100, 0, true)
                """,
                catId(cat),
                tableId,
            )
        }
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("1 id-less")
            .hasMessageContaining("live data file(s)")
        // The refused request minted nothing: head unchanged, column intact.
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("name")

        // RenameTable is unaffected — table binding rides table_uuid.
        alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameTable("t_renamed")))
        assertThat(catalogs.getTable(cat, ns, "t_renamed").tableId).isEqualTo(tableId)

        // Once the flagged file is retired (end-snapshotted), rename works.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = 4 WHERE catalog_id = ? AND data_file_id = 1",
                catId(cat),
            )
        }
        val info = alter.alterTable(cat, ns, "t_renamed", listOf(AlterOp.RenameColumn("name", "label")))
        assertThat(info.columns.map { it.def.name }).contains("label")
    }

    @Test
    fun `rename column ignores flagged files that are not live`() {
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    end_snapshot, path, record_count, file_size_bytes, row_id_start, missing_field_ids)
                VALUES (?, 1, ?, 2, 3, 's3://bucket/idless-historical.parquet', 10, 100, 0, true)
                """,
                catId(cat),
                tableId,
            )
        }
        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        assertThat(info.columns.map { it.def.name }).contains("label")
    }

    @Test
    fun `promote int to long and float to double`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.PromoteColumn("count", ColType.LONG),
                    AlterOp.PromoteColumn("score", ColType.DOUBLE),
                ),
            )
        assertThat(info.columns.single { it.def.name == "count" }.def.type)
            .isEqualTo(ColType.LONG)
        assertThat(info.columns.single { it.def.name == "score" }.def.type)
            .isEqualTo(ColType.DOUBLE)
        // Time travel: old types at the pre-alter snapshot, same field ids.
        val old = catalogs.getTable(cat, ns, "t", snapshot = before).columns
        assertThat(old.single { it.def.name == "count" }.def.type).isEqualTo(ColType.INT)
        assertThat(old.single { it.def.name == "score" }.def.type).isEqualTo(ColType.FLOAT)
    }

    @Test
    fun `rename column is refused while a live file is still pending hydration`() {
        // Pinned regression (bug hunt #6, the rename-guard TOCTOU): a
        // deferred-stats file has missing_field_ids=false (schema default)
        // until the hydrator's footer read — its id state is UNKNOWN, so
        // the guard must block on 'pending' too, not only on the flag.
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, stats_state)
                VALUES (?, 1, ?, 2, 's3://bucket/deferred.parquet', 10, 100, 0, 'pending')
                """,
                catId(cat),
                tableId,
            )
        }
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("1 not-yet-hydrated")
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("name")

        // Hydration lands an id-bearing verdict: pending -> provided with
        // missing_field_ids=false. The rename is now allowed.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET stats_state = 'provided' WHERE catalog_id = ? AND data_file_id = 1",
                catId(cat),
            )
        }
        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        assertThat(info.columns.map { it.def.name }).contains("label")
    }

    @Test
    fun `rename guard message distinguishes id-less from not-yet-hydrated blockers`() {
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, missing_field_ids)
                VALUES (?, 1, ?, 2, 's3://bucket/idless.parquet', 10, 100, 0, true)
                """,
                catId(cat),
                tableId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, stats_state)
                VALUES (?, 2, ?, 2, 's3://bucket/pending.parquet', 10, 100, 10, 'pending')
                """,
                catId(cat),
                tableId,
            )
        }
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("1 id-less")
            .hasMessageContaining("1 not-yet-hydrated")
    }

    @Test
    fun `promote re-encodes existing 4-byte stats bounds to the new width, values preserved`() {
        // Pinned regression (bug hunt #5): int->long / float->double left
        // hog_file_column_stats bounds in the old 4-byte encoding, which
        // the live-typed decode (compaction bound-merge, readers) rejects.
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        val countField = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "count" }.fieldId
        val scoreField = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "score" }.fieldId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, ?, 2, 's3://bucket/promote.parquet', 10, 100, 0)
                """,
                catId(cat),
                tableId,
            )
            for ((field, lower, upper) in listOf(
                Triple(
                    countField,
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeInt(-7),
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeInt(123),
                ),
                Triple(
                    scoreField,
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeFloat(1.5f),
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeFloat(3.25f),
                ),
            )) {
                h.execute(
                    """
                    INSERT INTO hog_file_column_stats
                        (catalog_id, data_file_id, field_id, value_count, null_count, lower_bound, upper_bound)
                    VALUES (?, 1, ?, 10, 0, ?, ?)
                    """,
                    catId(cat),
                    field,
                    lower,
                    upper,
                )
            }
        }

        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.PromoteColumn("count", ColType.LONG),
                AlterOp.PromoteColumn("score", ColType.DOUBLE),
            ),
        )

        val bounds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, lower_bound, upper_bound FROM hog_file_column_stats
                    WHERE catalog_id = ? AND data_file_id = 1
                    """,
                ).bind(0, catId(cat))
                    .map { rs, _ ->
                        rs.getLong("field_id") to Pair(rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                    }
                    .list().toMap()
            }
        // 8-byte encodings under the NEW types, values preserved exactly
        // (sign extension included — the negative int is the sharp edge).
        assertThat(bounds[countField]!!.first)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeLong(-7L))
        assertThat(bounds[countField]!!.second)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeLong(123L))
        assertThat(bounds[scoreField]!!.first)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeDouble(1.5))
        assertThat(bounds[scoreField]!!.second)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeDouble(3.25))
    }

    @Test
    fun `rename table resolves new name at head and old name at old snapshot`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val uuid = catalogs.getTable(cat, ns, "t").tableUuid

        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameTable("t2")))
        assertThat(info.name).isEqualTo("t2")
        assertThat(info.tableUuid).isEqualTo(uuid)

        assertThat(catalogs.getTable(cat, ns, "t2").tableUuid).isEqualTo(uuid)
        assertThatThrownBy { catalogs.getTable(cat, ns, "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catalogs.getTable(cat, ns, "t", snapshot = before).tableUuid).isEqualTo(uuid)
    }

    @Test
    fun `set partition spec returns spec_id 1 with ordered fields`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(
                        listOf(
                            PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY),
                            PartitionFieldDef(cols["id"]!!.fieldId, Transform.BUCKET, 16),
                            PartitionFieldDef(cols["name"]!!.fieldId, Transform.IDENTITY),
                        ),
                    ),
                ),
            )
        val spec = info.partitionSpec!!
        assertThat(spec.specId).isEqualTo(1)
        assertThat(spec.fields).containsExactly(
            PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY),
            PartitionFieldDef(cols["id"]!!.fieldId, Transform.BUCKET, 16),
            PartitionFieldDef(cols["name"]!!.fieldId, Transform.IDENTITY),
        )
        // key_index mirrors list position in the DB.
        val keyed =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT key_index, source_field_id FROM hog_partition_field
                WHERE catalog_id = :c AND table_id = :t ORDER BY key_index
                """,
                ).bind("c", catId(cat)).bind("t", info.tableId).mapToMap().list()
            }
        assertThat(keyed.map { it["key_index"] }).containsExactly(0, 1, 2)
    }

    // ---- spec lifecycle --------------------------------------------------

    @Test
    fun `replacing the spec increments spec_id and ends the old spec`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val tsField = PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY)
        val idField = PartitionFieldDef(cols["id"]!!.fieldId, Transform.IDENTITY)

        val first = alter.alterTable(cat, ns, "t", listOf(AlterOp.SetPartitionSpec(listOf(tsField))))
        assertThat(first.partitionSpec!!.specId).isEqualTo(1)

        val second = alter.alterTable(cat, ns, "t", listOf(AlterOp.SetPartitionSpec(listOf(idField))))
        assertThat(second.partitionSpec!!.specId).isEqualTo(2)
        assertThat(second.partitionSpec!!.fields).containsExactly(idField)

        val specs =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT spec_id, end_snapshot FROM hog_partition_spec " +
                        "WHERE catalog_id = :c AND table_id = :t ORDER BY spec_id",
                ).bind("c", catId(cat)).bind("t", second.tableId).mapToMap().list()
            }
        assertThat(specs).hasSize(2)
        assertThat(specs[0]["end_snapshot"]).isNotNull()
        assertThat(specs[1]["end_snapshot"]).isNull()

        // Clear back to unpartitioned: no live spec row remains.
        val cleared = alter.alterTable(cat, ns, "t", listOf(AlterOp.SetPartitionSpec(emptyList())))
        assertThat(cleared.partitionSpec).isNull()
        val live =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_partition_spec " +
                        "WHERE catalog_id = :c AND table_id = :t AND end_snapshot IS NULL",
                ).bind("c", catId(cat)).bind("t", second.tableId).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(live).isEqualTo(0)
    }

    @Test
    fun `dropping a live spec source column is rejected`() {
        val (cat, ns) = fixture()
        val ts = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "ts" }
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(ts.fieldId, Transform.DAY))),
            ),
        )
        assertThatThrownBy { alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("ts"))) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("partition spec")
        // Clearing the spec unblocks the drop.
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.SetPartitionSpec(emptyList()),
                AlterOp.DropColumn("ts"),
            ),
        )
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name })
            .doesNotContain("ts")
    }

    @Test
    fun `setting a spec on a column dropped earlier in the request is rejected`() {
        val (cat, ns) = fixture()
        val ts = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "ts" }
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.DropColumn("ts"),
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(ts.fieldId, Transform.DAY))),
                ),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("not a live column")
        // Atomicity: the failed request left nothing behind, ts is intact.
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("ts")
    }

    @Test
    fun `spec validation - transform types and params`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val nameId = cols["name"]!!.fieldId
        val tsId = cols["ts"]!!.fieldId

        // Temporal transform on a string column.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(nameId, Transform.MONTH))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Bucket without a param, bucket with param 0.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(nameId, Transform.BUCKET))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(nameId, Transform.BUCKET, 0))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Param on a non-bucket transform.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(tsId, Transform.DAY, 4))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Unknown field id.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(9999, Transform.IDENTITY))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Bucket is legal on any type; year/month/day/hour on date/ts/tstz.
        val ok =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(
                        listOf(
                            PartitionFieldDef(nameId, Transform.BUCKET, 8),
                            PartitionFieldDef(tsId, Transform.HOUR),
                        ),
                    ),
                ),
            )
        assertThat(ok.partitionSpec!!.fields).hasSize(2)
    }

    // ---- in-order semantics ----------------------------------------------

    @Test
    fun `add then rename the new column in one request`() {
        val (cat, ns) = fixture()
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("tmp", ColType.STRING)),
                    AlterOp.RenameColumn("tmp", "final"),
                ),
            )
        assertThat(info.columns.map { it.def.name }).contains("final").doesNotContain("tmp")
        // Only one live row for the field; the tmp row was never visible.
        val fieldId = info.columns.single { it.def.name == "final" }.fieldId
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT name FROM hog_column WHERE catalog_id = :c AND table_id = :t AND field_id = :f",
                ).bind("c", catId(cat)).bind("t", info.tableId).bind("f", fieldId)
                    .mapTo(String::class.java).list()
            }
        assertThat(rows).containsExactly("final")
    }

    @Test
    fun `drop then reference the dropped column fails`() {
        val (cat, ns) = fixture()
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.DropColumn("name"),
                    AlterOp.RenameColumn("name", "label"),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        // Rolled back: name is still there.
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("name")
    }

    @Test
    fun `double rename in one request leaves one live row and clean history`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val fieldId = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "name" }.fieldId
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.RenameColumn("name", "mid"),
                    AlterOp.RenameColumn("mid", "end"),
                ),
            )
        assertThat(info.columns.map { it.def.name }).contains("end")
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT name, end_snapshot FROM hog_column " +
                        "WHERE catalog_id = :c AND table_id = :t AND field_id = :f ORDER BY begin_snapshot",
                ).bind("c", catId(cat)).bind("t", info.tableId).bind("f", fieldId).mapToMap().list()
            }
        // "mid" never persisted: only the original (ended) and "end" (live).
        assertThat(rows.map { it["name"] }).containsExactly("name", "end")
        assertThat(
            catalogs.getTable(cat, ns, "t", snapshot = before)
                .columns.single { it.fieldId == fieldId }.def.name,
        ).isEqualTo("name")
    }

    @Test
    fun `add then drop the same column in one request leaves no trace`() {
        val (cat, ns) = fixture()
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("ghost", ColType.STRING)),
                    AlterOp.DropColumn("ghost"),
                ),
            )
        assertThat(info.columns.map { it.def.name }).doesNotContain("ghost")
        val count =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_column " +
                        "WHERE catalog_id = :c AND table_id = :t AND name = 'ghost'",
                ).bind("c", catId(cat)).bind("t", info.tableId).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(count).isEqualTo(0)
    }

    @Test
    fun `set spec twice in one request keeps only the final spec`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY))),
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(cols["id"]!!.fieldId, Transform.IDENTITY))),
                ),
            )
        assertThat(info.partitionSpec!!.fields)
            .containsExactly(PartitionFieldDef(cols["id"]!!.fieldId, Transform.IDENTITY))
        val specCount =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_partition_spec WHERE catalog_id = :c AND table_id = :t",
                ).bind("c", catId(cat)).bind("t", info.tableId).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(specCount).isEqualTo(1)
    }

    // ---- rejections ------------------------------------------------------

    @Test
    fun `promotion lattice rejects everything but int-long and float-double`() {
        val (cat, ns) = fixture()
        val illegal =
            listOf(
                // int -> double
                "count" to ColType.DOUBLE,
                // no-op promotion
                "count" to ColType.INT,
                // narrowing long -> int
                "id" to ColType.INT,
                // float -> long
                "score" to ColType.LONG,
                // string -> anything
                "name" to ColType.LONG,
            )
        for ((col, target) in illegal) {
            assertThatThrownBy {
                alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn(col, target)))
            }.isInstanceOf(HoglakeException.Validation::class.java)
        }
    }

    @Test
    fun `column op rejections - duplicate, missing, last column`() {
        val (cat, ns) = fixture()
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.AddColumn(ColumnDef("id", ColType.LONG))))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("nope")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("nope", "x")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("nope", ColType.LONG)))
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Last-column drop.
        catalogs.createTable(cat, ns, "solo", listOf(idCol))
        assertThatThrownBy {
            alter.alterTable(cat, ns, "solo", listOf(AlterOp.DropColumn("id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `rename table to an existing live name conflicts`() {
        val (cat, ns) = fixture()
        catalogs.createTable(cat, ns, "other", listOf(idCol))
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameTable("other")))
        }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    @Test
    fun `unknown catalog, namespace, table are NotFound and empty ops is Validation`() {
        val (cat, ns) = fixture()
        assertThatThrownBy {
            alter.alterTable("no-such", ns, "t", listOf(AlterOp.DropColumn("name")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, "no-such", "t", listOf(AlterOp.DropColumn("name")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "no-such", listOf(AlterOp.DropColumn("name")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", emptyList())
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- commit bookkeeping ----------------------------------------------

    @Test
    fun `each alter bumps schema_version and records exactly one table_altered change`() {
        val (cat, ns) = fixture()
        val before = catalogs.getCatalog(cat)
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("a", ColType.STRING)),
                    AlterOp.AddColumn(ColumnDef("b", ColType.STRING)),
                    AlterOp.RenameColumn("a", "c"),
                ),
            )
        val after = catalogs.getCatalog(cat)
        assertThat(after.headSnapshotId).isEqualTo(before.headSnapshotId + 1)
        assertThat(after.schemaVersion).isEqualTo(before.schemaVersion + 1)

        val (page, _) = catalogs.listSnapshots(cat, after = before.headSnapshotId, limit = 10)
        assertThat(page).hasSize(1)
        val snap = page.single()
        assertThat(snap.snapshotId).isEqualTo(after.headSnapshotId)
        assertThat(snap.schemaVersion).isEqualTo(after.schemaVersion)
        assertThat(snap.changes).hasSize(1)
        assertThat(snap.changes.single().kind).isEqualTo(ChangeKind.TABLE_ALTERED)
        assertThat(snap.changes.single().objectId).isEqualTo(info.tableId)
    }
}
