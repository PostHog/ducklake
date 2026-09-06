package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.SnapshotChange
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import java.time.OffsetDateTime

/** hog_snapshot + hog_snapshot_change (the typed conflict vocabulary). */
object SnapshotRepo {
    private val snapshotMapper =
        RowMapper { rs, _ ->
            Snapshot(
                snapshotId = rs.getLong("snapshot_id"),
                snapshotTime = rs.getObject("snapshot_time", OffsetDateTime::class.java).toInstant(),
                schemaVersion = rs.getLong("schema_version"),
                author = rs.getString("author"),
                message = rs.getString("commit_message"),
            )
        }

    /**
     * snapshot_time is stamped with clock_timestamp() (statement time),
     * NOT the column default now() (transaction start time): every
     * caller runs inside a transaction that may have QUEUED on the
     * per-catalog advisory commit lock before reaching this insert, and
     * now() would predate that wait — letting a convoyed commit record
     * a time OLDER than an earlier-committed snapshot's. Statement time
     * executes under the lock, so snapshot_time stays monotone with
     * snapshot id (modulo the DB clock stepping backwards), which is
     * the premise TimeTravelRepo.resolveTimestamp documents.
     */
    fun insert(
        handle: Handle,
        catalogId: Long,
        snapshotId: Long,
        schemaVersion: Long,
        author: String? = null,
        message: String? = null,
    ) {
        handle.createUpdate(
            """
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version, author, commit_message)
            VALUES (:catalogId, :snapshotId, clock_timestamp(), :schemaVersion, :author, :message)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("snapshotId", snapshotId)
            .bind("schemaVersion", schemaVersion)
            .bind("author", author)
            .bind("message", message)
            .execute()
    }

    fun insertChange(
        handle: Handle,
        catalogId: Long,
        snapshotId: Long,
        kind: ChangeKind,
        objectId: Long,
    ) {
        handle.createUpdate(
            """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            VALUES (:catalogId, :snapshotId, :kind, :objectId)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("snapshotId", snapshotId)
            .bind("kind", kind.wire)
            .bind("objectId", objectId)
            .execute()
    }

    /**
     * One page of snapshots with id > [after], ordered by id, without
     * change rows (see [changesFor]). [limit] is the raw SQL LIMIT — the
     * service passes limit+1 to detect hasMore.
     */
    fun page(
        handle: Handle,
        catalogId: Long,
        after: Long,
        limit: Int,
    ): List<Snapshot> =
        handle.createQuery(
            """
            SELECT snapshot_id, snapshot_time, schema_version, author, commit_message
            FROM hog_snapshot
            WHERE catalog_id = :catalogId AND snapshot_id > :after
            ORDER BY snapshot_id
            LIMIT :limit
            """,
        )
            .bind("catalogId", catalogId)
            .bind("after", after)
            .bind("limit", limit)
            .map(snapshotMapper)
            .list()

    /**
     * One page of snapshots with id < [before], ordered by id DESC
     * (newest first), without change rows. [limit] is the raw SQL LIMIT
     * — the service passes limit+1 to detect hasMore.
     */
    fun pageBefore(
        handle: Handle,
        catalogId: Long,
        before: Long,
        limit: Int,
    ): List<Snapshot> =
        handle.createQuery(
            """
            SELECT snapshot_id, snapshot_time, schema_version, author, commit_message
            FROM hog_snapshot
            WHERE catalog_id = :catalogId AND snapshot_id < :before
            ORDER BY snapshot_id DESC
            LIMIT :limit
            """,
        )
            .bind("catalogId", catalogId)
            .bind("before", before)
            .bind("limit", limit)
            .map(snapshotMapper)
            .list()

    /**
     * Change rows for a contiguous snapshot-id range, grouped by
     * snapshot id. The caller's page IS contiguous by construction
     * (ordered scan from `> after`), so a range predicate is exact.
     */
    fun changesFor(
        handle: Handle,
        catalogId: Long,
        fromId: Long,
        toId: Long,
    ): Map<Long, List<SnapshotChange>> =
        handle.createQuery(
            """
            SELECT snapshot_id, kind, object_id
            FROM hog_snapshot_change
            WHERE catalog_id = :catalogId AND snapshot_id BETWEEN :fromId AND :toId
            ORDER BY snapshot_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("fromId", fromId)
            .bind("toId", toId)
            .map { rs, _ ->
                rs.getLong("snapshot_id") to
                    SnapshotChange(
                        kind = ChangeKind.fromWire(rs.getString("kind")),
                        objectId = rs.getLong("object_id"),
                    )
            }
            .list()
            .groupBy({ it.first }, { it.second })
}
