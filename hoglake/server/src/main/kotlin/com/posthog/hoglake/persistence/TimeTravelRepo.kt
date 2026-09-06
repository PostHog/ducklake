package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import java.time.Instant

/**
 * Read-side retention lookups: the catalog's expiry floor
 * (hog_catalog.earliest_snapshot_id) and timestamp -> snapshot-id
 * resolution over hog_snapshot. Writes to either column/table belong to
 * the expiry service and the DDL/commit tails, never here.
 */
object TimeTravelRepo {
    /** The catalog's expiry floor: snapshots below this id are gone. */
    fun earliestSnapshotId(
        handle: Handle,
        catalogId: Long,
    ): Long =
        handle.createQuery(
            "SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = :catalogId",
        )
            .bind("catalogId", catalogId)
            .mapTo(Long::class.javaObjectType)
            .one()

    /**
     * Resolve [atTimestamp] to the largest retained snapshot id whose
     * snapshot_time <= it (one indexed max-lookup on the hog_snapshot
     * PK, floored at [earliestSnapshotId]).
     *
     * - before the earliest RETAINED snapshot's time ->
     *   [HoglakeException.Expired] (that history is gone);
     * - between two snapshots -> the lower one;
     * - after head's time -> head (snapshot_time is monotone with id
     *   because every snapshot is minted under the catalog commit lock).
     */
    fun resolveTimestamp(
        handle: Handle,
        catalogId: Long,
        earliestSnapshotId: Long,
        atTimestamp: Instant,
    ): Long =
        handle.createQuery(
            """
            SELECT max(snapshot_id) FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest
              AND snapshot_time <= :ts
            """,
        )
            .bind("catalogId", catalogId)
            .bind("earliest", earliestSnapshotId)
            .bind("ts", atTimestamp.atOffset(java.time.ZoneOffset.UTC))
            .mapTo(Long::class.javaObjectType)
            .findOne()
            .orElse(null)
            ?: throw HoglakeException.Expired(
                "at_timestamp $atTimestamp is before the earliest retained snapshot " +
                    "(earliest_snapshot_id $earliestSnapshotId): that history has been " +
                    "expired; reconcile from a full scan at a retained snapshot",
            )
}
