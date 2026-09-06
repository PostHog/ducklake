package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * Schema-level guarantees: the migration applies cleanly, FK cascades
 * actually cascade, and the per-catalog advisory lock behaves.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `migration applies cleanly and creates every table`() {
        val applied =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT version, success FROM flyway_schema_history ORDER BY installed_rank")
                    .map { rs, _ -> rs.getString("version") to rs.getBoolean("success") }
                    .list()
            }
        assertThat(applied).contains("1" to true)

        val tables =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
                ).mapTo(String::class.javaObjectType).list()
            }
        assertThat(tables).contains(
            "hog_catalog", "hog_snapshot", "hog_snapshot_change", "hog_namespace",
            "hog_table", "hog_table_version", "hog_column", "hog_table_stats",
            "hog_data_file", "hog_file_column_stats", "hog_consumer_offset",
        )
    }

    @Test
    fun `deleting a catalog row cascades through the whole object graph`() {
        val cat = svc.createCatalog("cascade-cat", "s3://bucket/cascade")
        svc.createNamespace("cascade-cat", "ns")
        val t =
            svc.createTable(
                "cascade-cat",
                "ns",
                "t",
                listOf(ColumnDef("id", ColType.LONG, nullable = false)),
            )
        // Rows the DDL path doesn't create: a data file, its column stats,
        // and a consumer offset.
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (:cid, 1, :tid, 2, 's3://bucket/cascade/f1.parquet', 10, 1024, 0)
                """,
            ).bind("cid", cat.catalogId).bind("tid", t.tableId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_column_stats
                    (catalog_id, data_file_id, field_id, value_count, null_count)
                VALUES (:cid, 1, 1, 10, 0)
                """,
            ).bind("cid", cat.catalogId).execute()
        }
        svc.commitOffset("cascade-cat", "consumer-1", t.tableUuid, 2)

        db.jdbi.withHandleUnchecked { h ->
            // hog_table_version -> hog_namespace is declared WITHOUT
            // ON DELETE CASCADE (V1__init.sql), and Postgres checks it
            // while the catalog cascade is mid-flight, so a raw catalog
            // delete fails when live tables exist. Delete the table
            // graph first (exercises the hog_table cascades), then the
            // catalog (exercises the rest).
            h.createUpdate("DELETE FROM hog_table WHERE catalog_id = :cid")
                .bind("cid", cat.catalogId).execute()
            h.createUpdate("DELETE FROM hog_catalog WHERE catalog_id = :cid")
                .bind("cid", cat.catalogId).execute()
        }

        val children =
            listOf(
                "hog_snapshot", "hog_snapshot_change", "hog_namespace", "hog_table",
                "hog_table_version", "hog_column", "hog_table_stats", "hog_data_file",
                "hog_file_column_stats", "hog_consumer_offset",
            )
        for (table in children) {
            val count =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM $table WHERE catalog_id = :cid")
                        .bind("cid", cat.catalogId)
                        .mapTo(Long::class.javaObjectType).one()
                }
            assertThat(count).describedAs("orphans left in %s", table).isZero()
        }
    }

    @Test
    fun `catalog commit lock is visible in pg_locks and excludes other sessions`() {
        val catalogId = 4242L
        db.jdbi.open().use { h ->
            h.begin()
            Locks.acquireCatalogCommitLock(h, catalogId)

            val held =
                h.createQuery(
                    """
                SELECT count(*) FROM pg_locks
                WHERE locktype = 'advisory' AND classid = :classid AND objid = :objid
                """,
                )
                    .bind("classid", Locks.CATALOG_COMMIT_LOCK_CLASS)
                    .bind("objid", catalogId)
                    .mapTo(Long::class.javaObjectType).one()
            assertThat(held).isEqualTo(1)

            // A second session cannot take the same catalog's lock...
            // (Same single-bigint key computation as Locks.kt — the lock
            // key MUST be computed identically everywhere or these probes
            // would contend on a DIFFERENT lock and prove nothing.)
            val contended =
                db.jdbi.inTransactionUnchecked { h2 ->
                    h2.createQuery(
                        "SELECT pg_try_advisory_xact_lock((:classid::bigint << 32) | (:objid::bigint & 4294967295))",
                    )
                        .bind("classid", Locks.CATALOG_COMMIT_LOCK_CLASS)
                        .bind("objid", catalogId)
                        .mapTo(Boolean::class.javaObjectType).one()
                }
            assertThat(contended).isFalse()

            // ...but a different catalog's lock is free.
            val otherCatalog =
                db.jdbi.inTransactionUnchecked { h2 ->
                    h2.createQuery(
                        "SELECT pg_try_advisory_xact_lock((:classid::bigint << 32) | (:objid::bigint & 4294967295))",
                    )
                        .bind("classid", Locks.CATALOG_COMMIT_LOCK_CLASS)
                        .bind("objid", catalogId + 1)
                        .mapTo(Boolean::class.javaObjectType).one()
                }
            assertThat(otherCatalog).isTrue()

            h.rollback()
        }
        // Transaction over -> lock released.
        val free =
            db.jdbi.inTransactionUnchecked { h2 ->
                h2.createQuery(
                    "SELECT pg_try_advisory_xact_lock((:classid::bigint << 32) | (:objid::bigint & 4294967295))",
                )
                    .bind("classid", Locks.CATALOG_COMMIT_LOCK_CLASS)
                    .bind("objid", catalogId)
                    .mapTo(Boolean::class.javaObjectType).one()
            }
        assertThat(free).isTrue()
    }

    @Test
    fun `duplicate live ordinal and negative ordinal are rejected by the DB`() {
        val cat = svc.createCatalog("ordinal-cat", "s3://bucket/o2")
        svc.createNamespace("ordinal-cat", "ns")
        val t =
            svc.createTable(
                "ordinal-cat",
                "ns",
                "t",
                listOf(ColumnDef("id", ColType.LONG), ColumnDef("v", ColType.STRING)),
            )
        // A second LIVE row at ordinal 0 (id's ordinal): unique partial
        // index hog_column_live_ordinal must refuse — a duplicate live
        // ordinal silently corrupts writers that stamp field order.
        org.assertj.core.api.Assertions.assertThatThrownBy {
            db.jdbi.withHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_column " +
                        "(catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal) " +
                        "VALUES (?, ?, 99, 5, 'smuggled', 'long', 0)",
                    cat.catalogId,
                    t.tableId,
                )
            }
        }.hasMessageContaining("hog_column_live_ordinal")
        // An END-SNAPSHOTTED row at the same ordinal is history, not a
        // conflict (the index is partial on end_snapshot IS NULL).
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_column " +
                    "(catalog_id, table_id, field_id, begin_snapshot, end_snapshot, name, col_type, ordinal) " +
                    "VALUES (?, ?, 98, 1, 2, 'old', 'long', 0)",
                cat.catalogId,
                t.tableId,
            )
        }
        // ordinal >= 0 is a CHECK.
        org.assertj.core.api.Assertions.assertThatThrownBy {
            db.jdbi.withHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_column " +
                        "(catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal) " +
                        "VALUES (?, ?, 97, 5, 'negative', 'long', -1)",
                    cat.catalogId,
                    t.tableId,
                )
            }
        }.hasMessageContaining("ordinal")
    }

    @Test
    fun `snapshot change rows refuse a NULL object_id`() {
        // Every change kind names an object; a NULL object_id would be a
        // conflict row the conflict index never matches (an invisible OCC
        // bypass), so the column is NOT NULL outright.
        val cat = svc.createCatalog("objid-cat", "s3://bucket/o3")
        org.assertj.core.api.Assertions.assertThatThrownBy {
            db.jdbi.withHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id) " +
                        "VALUES (?, 0, 'table_created', NULL)",
                    cat.catalogId,
                )
            }
        }.hasMessageContaining("object_id")
    }

    @Test
    fun `offset upsert guard is race-safe at the SQL level`() {
        val cat = svc.createCatalog("offset-sql-cat", "s3://bucket/o")
        svc.createNamespace("offset-sql-cat", "ns")
        val uuid = UUID.randomUUID()
        db.jdbi.withHandleUnchecked { h ->
            val first = OffsetRepo.upsert(h, cat.catalogId, "c", uuid, 1)
            assertThat(first?.committedSnapshot).isEqualTo(1)
            // Regression returns null instead of updating.
            val regressed = OffsetRepo.upsert(h, cat.catalogId, "c", uuid, 0)
            assertThat(regressed).isNull()
            // Equal commit is idempotent-accept.
            val equal = OffsetRepo.upsert(h, cat.catalogId, "c", uuid, 1)
            assertThat(equal?.committedSnapshot).isEqualTo(1)
        }
    }
}
