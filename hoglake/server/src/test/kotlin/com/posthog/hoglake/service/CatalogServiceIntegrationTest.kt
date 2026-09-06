package com.posthog.hoglake.service

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** Catalog / namespace / table DDL lifecycle, incl. time travel. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)

    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)
    private val nameCol = ColumnDef("name", ColType.STRING)

    @AfterAll
    fun tearDown() = db.close()

    // ---- catalogs --------------------------------------------------------

    @Test
    fun `createCatalog mints snapshot 0 with no changes`() {
        val cat = svc.createCatalog("cat-a", "s3://bucket/a")
        assertThat(cat.headSnapshotId).isEqualTo(0)
        assertThat(cat.schemaVersion).isEqualTo(0)
        assertThat(cat.dataPath).isEqualTo("s3://bucket/a")

        assertThat(svc.getCatalog("cat-a")).isEqualTo(cat)

        val (page, hasMore) = svc.listSnapshots("cat-a", after = -1, limit = 10)
        assertThat(hasMore).isFalse()
        assertThat(page).hasSize(1)
        assertThat(page[0].snapshotId).isEqualTo(0)
        assertThat(page[0].schemaVersion).isEqualTo(0)
        assertThat(page[0].changes).isEmpty()
    }

    @Test
    fun `duplicate catalog name conflicts`() {
        svc.createCatalog("cat-dup", "s3://bucket/d1")
        assertThatThrownBy { svc.createCatalog("cat-dup", "s3://bucket/d2") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    @Test
    fun `catalog name failing the schema check is a validation error`() {
        assertThatThrownBy { svc.createCatalog("Bad_Name", "s3://bucket/x") }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `unknown catalog is NotFound`() {
        assertThatThrownBy { svc.getCatalog("no-such-catalog") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.listNamespaces("no-such-catalog") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `listCatalogs returns all catalogs ordered by name`() {
        svc.createCatalog("list-b", "s3://bucket/lb")
        svc.createCatalog("list-a", "s3://bucket/la")
        val names = svc.listCatalogs().map { it.name }
        assertThat(names).containsSubsequence("list-a", "list-b")
    }

    // ---- namespaces ------------------------------------------------------

    @Test
    fun `createNamespace advances head and records a typed change`() {
        svc.createCatalog("ns-cat", "s3://bucket/ns")
        val ns = svc.createNamespace("ns-cat", "analytics")
        assertThat(ns.name).isEqualTo("analytics")

        val cat = svc.getCatalog("ns-cat")
        assertThat(cat.headSnapshotId).isEqualTo(1)
        assertThat(cat.schemaVersion).isEqualTo(1)

        val (page, _) = svc.listSnapshots("ns-cat", after = 0, limit = 10)
        assertThat(page).hasSize(1)
        assertThat(page[0].changes).hasSize(1)
        assertThat(page[0].changes[0].kind).isEqualTo(ChangeKind.NAMESPACE_CREATED)
        assertThat(page[0].changes[0].objectId).isEqualTo(ns.namespaceId)

        assertThat(svc.listNamespaces("ns-cat").map { it.name }).containsExactly("analytics")
    }

    @Test
    fun `duplicate namespace conflicts`() {
        svc.createCatalog("ns-dup-cat", "s3://bucket/nd")
        svc.createNamespace("ns-dup-cat", "ns")
        assertThatThrownBy { svc.createNamespace("ns-dup-cat", "ns") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    // ---- tables ----------------------------------------------------------

    @Test
    fun `createTable assigns 1-based field ids and positional ordinals`() {
        svc.createCatalog("tbl-cat", "s3://bucket/t")
        svc.createNamespace("tbl-cat", "ns")
        val decimalCol =
            ColumnDef(
                "amount",
                ColType.DECIMAL,
                typeParams = mapOf("precision" to 38, "scale" to 9),
                nullable = false,
            )
        val t = svc.createTable("tbl-cat", "ns", "events", listOf(idCol, nameCol, decimalCol))

        assertThat(t.columns).hasSize(3)
        assertThat(t.columns.map { it.fieldId }).containsExactly(1L, 2L, 3L)
        assertThat(t.columns.map { it.ordinal }).containsExactly(0, 1, 2)
        assertThat(t.columns.map { it.def.name }).containsExactly("id", "name", "amount")
        assertThat(t.recordCount).isZero()
        assertThat(t.fileCount).isZero()
        assertThat(t.fileSizeBytes).isZero()

        // Head advanced (catalog=S0, ns=S1, table=S2) with a typed change.
        val cat = svc.getCatalog("tbl-cat")
        assertThat(cat.headSnapshotId).isEqualTo(2)
        val (page, _) = svc.listSnapshots("tbl-cat", after = 1, limit = 10)
        assertThat(page[0].changes).hasSize(1)
        assertThat(page[0].changes[0].kind).isEqualTo(ChangeKind.TABLE_CREATED)
        assertThat(page[0].changes[0].objectId).isEqualTo(t.tableId)

        // Round trip through getTable, incl. jsonb type params.
        val fetched = svc.getTable("tbl-cat", "ns", "events")
        assertThat(fetched.tableUuid).isEqualTo(t.tableUuid)
        assertThat(fetched.columns).isEqualTo(t.columns)
        assertThat(fetched.columns[2].def.typeParams)
            .isEqualTo(mapOf("precision" to 38, "scale" to 9))

        // The field-id allocator advanced past the assigned ids.
        val nextFieldId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT next_field_id FROM hog_table WHERE catalog_id = :cid AND table_id = :tid",
                ).bind("cid", cat.catalogId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            }
        assertThat(nextFieldId).isEqualTo(4)

        // hog_table_stats row exists.
        val statsRows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_table_stats WHERE catalog_id = :cid AND table_id = :tid",
                ).bind("cid", cat.catalogId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            }
        assertThat(statsRows).isEqualTo(1)
    }

    @Test
    fun `duplicate table name in a namespace conflicts, other namespace does not`() {
        svc.createCatalog("tbl-dup-cat", "s3://bucket/td")
        svc.createNamespace("tbl-dup-cat", "ns1")
        svc.createNamespace("tbl-dup-cat", "ns2")
        svc.createTable("tbl-dup-cat", "ns1", "t", listOf(idCol))
        assertThatThrownBy { svc.createTable("tbl-dup-cat", "ns1", "t", listOf(idCol)) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        // Same name in a different namespace is fine.
        val other = svc.createTable("tbl-dup-cat", "ns2", "t", listOf(idCol))
        assertThat(other.namespace).isEqualTo("ns2")
    }

    @Test
    fun `createTable input validation`() {
        svc.createCatalog("tbl-val-cat", "s3://bucket/tv")
        svc.createNamespace("tbl-val-cat", "ns")
        assertThatThrownBy { svc.createTable("tbl-val-cat", "ns", "t", emptyList()) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.createTable("tbl-val-cat", "ns", "t", listOf(idCol, idCol)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.createTable("tbl-val-cat", "nope", "t", listOf(idCol)) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `listTables returns live tables with empty column lists`() {
        svc.createCatalog("tbl-list-cat", "s3://bucket/tl")
        svc.createNamespace("tbl-list-cat", "ns")
        svc.createTable("tbl-list-cat", "ns", "bbb", listOf(idCol))
        svc.createTable("tbl-list-cat", "ns", "aaa", listOf(idCol))
        val tables = svc.listTables("tbl-list-cat", "ns")
        assertThat(tables.map { it.name }).containsExactly("aaa", "bbb")
        assertThat(tables).allSatisfy { assertThat(it.columns).isEmpty() }
    }

    // ---- drop + time travel ----------------------------------------------

    @Test
    fun `dropTable ends all live rows and enables time travel`() {
        svc.createCatalog("drop-cat", "s3://bucket/dr")
        svc.createNamespace("drop-cat", "ns") // S1
        val t = svc.createTable("drop-cat", "ns", "t", listOf(idCol, nameCol)) // S2
        val createdAt = svc.getCatalog("drop-cat").headSnapshotId
        val catId = svc.getCatalog("drop-cat").catalogId

        // A live data file, inserted directly (the commit service is
        // another agent's scope).
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (:cid, 100, :tid, :snap, 's3://bucket/dr/f.parquet', 5, 512, 0)
                """,
            ).bind("cid", catId).bind("tid", t.tableId).bind("snap", createdAt).execute()
        }

        val drop = svc.dropTable("drop-cat", "ns", "t") // S3
        assertThat(drop.snapshotId).isEqualTo(createdAt + 1)

        // Head: gone.
        assertThatThrownBy { svc.getTable("drop-cat", "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(svc.listTables("drop-cat", "ns")).isEmpty()
        assertThatThrownBy { svc.dropTable("drop-cat", "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // Time travel to the creation snapshot: fully visible.
        val back = svc.getTable("drop-cat", "ns", "t", snapshot = createdAt)
        assertThat(back.tableUuid).isEqualTo(t.tableUuid)
        assertThat(back.columns.map { it.def.name }).containsExactly("id", "name")
        assertThat(back.fileCount).isEqualTo(1)
        assertThat(svc.listFiles("drop-cat", "ns", "t", snapshot = createdAt))
            .hasSize(1)

        // Before creation: not visible.
        assertThatThrownBy { svc.getTable("drop-cat", "ns", "t", snapshot = createdAt - 1) }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // The drop end-snapshotted identity, version, columns, and files.
        db.jdbi.withHandleUnchecked { h ->
            val droppedSnapshot =
                h.createQuery(
                    "SELECT dropped_snapshot FROM hog_table WHERE catalog_id = :cid AND table_id = :tid",
                ).bind("cid", catId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            assertThat(droppedSnapshot).isEqualTo(drop.snapshotId)

            for (table in listOf("hog_table_version", "hog_column", "hog_data_file")) {
                val liveRows =
                    h.createQuery(
                        """
                    SELECT count(*) FROM $table
                    WHERE catalog_id = :cid AND table_id = :tid AND end_snapshot IS NULL
                    """,
                    ).bind("cid", catId).bind("tid", t.tableId)
                        .mapTo(Long::class.javaObjectType).one()
                assertThat(liveRows).describedAs("live rows left in %s", table).isZero()

                val endedAtDrop =
                    h.createQuery(
                        """
                    SELECT count(*) FROM $table
                    WHERE catalog_id = :cid AND table_id = :tid AND end_snapshot = :snap
                    """,
                    ).bind("cid", catId).bind("tid", t.tableId).bind("snap", drop.snapshotId)
                        .mapTo(Long::class.javaObjectType).one()
                assertThat(endedAtDrop).describedAs("rows ended at drop in %s", table)
                    .isGreaterThan(0)
            }
        }

        // The drop snapshot carries the typed change.
        val (page, _) = svc.listSnapshots("drop-cat", after = drop.snapshotId - 1, limit = 1)
        assertThat(page[0].changes[0].kind).isEqualTo(ChangeKind.TABLE_DROPPED)
        assertThat(page[0].changes[0].objectId).isEqualTo(t.tableId)
    }

    @Test
    fun `recreating a dropped table mints a new identity`() {
        svc.createCatalog("recreate-cat", "s3://bucket/rc")
        svc.createNamespace("recreate-cat", "ns")
        val v1 = svc.createTable("recreate-cat", "ns", "t", listOf(idCol))
        val preDrop = svc.getCatalog("recreate-cat").headSnapshotId
        svc.dropTable("recreate-cat", "ns", "t")
        val v2 = svc.createTable("recreate-cat", "ns", "t", listOf(idCol, nameCol))

        assertThat(v2.tableId).isNotEqualTo(v1.tableId)
        assertThat(v2.tableUuid).isNotEqualTo(v1.tableUuid)

        // Head sees the new incarnation; time travel sees the old one.
        assertThat(svc.getTable("recreate-cat", "ns", "t").tableUuid).isEqualTo(v2.tableUuid)
        assertThat(svc.getTable("recreate-cat", "ns", "t", snapshot = preDrop).tableUuid)
            .isEqualTo(v1.tableUuid)
    }

    @Test
    fun `snapshot parameter out of range is a validation error`() {
        svc.createCatalog("range-cat", "s3://bucket/rg")
        svc.createNamespace("range-cat", "ns")
        svc.createTable("range-cat", "ns", "t", listOf(idCol))
        val head = svc.getCatalog("range-cat").headSnapshotId
        assertThatThrownBy { svc.getTable("range-cat", "ns", "t", snapshot = head + 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.getTable("range-cat", "ns", "t", snapshot = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }
}
