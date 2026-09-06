package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Pinned regression for bug hunt #16: dropTable must end-snapshot the
 * table's live deletion vectors exactly like its data files — a live DV
 * row on a dropped table is invisible to expiry's range predicates
 * (end_snapshot IS NULL never sinks under the floor), leaking the row
 * and the puffin object forever on retention catalogs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DropDvLifecycleIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)
    private val expiry = ExpiryService(jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `drop end-snapshots live DVs and expiry then queues the DV objects`() {
        val cat = "drop-dv"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val catalogId = catalogs.getCatalog(cat).catalogId

        val dataPath = "s3://bucket/$cat/f.parquet"
        val dvPath = "s3://bucket/$cat/f.dv"
        val appendSnap =
            commits.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(dataPath, 10, 100))))),
            ).snapshotId
        val dataFileId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT data_file_id FROM hog_data_file WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            }
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = appendSnap,
                deletes = listOf(TableDeletes("ns", "t", listOf(DeleteFileRegistration(dataFileId, dvPath, 2, 16)))),
            ),
        )

        val dropSnap = catalogs.dropTable(cat, "ns", "t").snapshotId

        // Both halves end-snapshotted at the drop snapshot.
        val (dvEnd, dataEnd) =
            jdbi.withHandleUnchecked { h ->
                val dv =
                    h.createQuery("SELECT end_snapshot FROM hog_delete_file WHERE catalog_id = ?")
                        .bind(0, catalogId).mapTo(Long::class.javaObjectType).one()
                val data =
                    h.createQuery("SELECT end_snapshot FROM hog_data_file WHERE catalog_id = ?")
                        .bind(0, catalogId).mapTo(Long::class.javaObjectType).one()
                dv to data
            }
        assertThat(dvEnd).isEqualTo(dropSnap)
        assertThat(dataEnd).isEqualTo(dropSnap)

        // Retention catalog: once the floor passes the drop snapshot, the
        // sweep queues BOTH objects for cleanup.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = ?",
                catalogId,
            )
            h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 3600 WHERE catalog_id = ?", catalogId)
        }
        // One more snapshot so the drop snapshot itself can fall below the floor.
        catalogs.createNamespace(cat, "post-drop")
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = ?",
                catalogId,
            )
        }

        val sweep = expiry.runOnce(cat, batchSize = 1000)
        assertThat(sweep.dataFilesQueued).isEqualTo(1)
        assertThat(sweep.deleteFilesQueued).isEqualTo(1)
        val queued =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT path, file_kind FROM hog_file_removal WHERE catalog_id = ? ORDER BY file_kind",
                ).bind(0, catalogId)
                    .map { rs, _ -> rs.getString("path") to rs.getString("file_kind") }
                    .list()
            }
        assertThat(queued).containsExactlyInAnyOrder(dataPath to "data", dvPath to "delete")
    }
}
