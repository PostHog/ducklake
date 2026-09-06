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
    /**
     * The expiry floor with its anchor time: [earliestSnapshotTime] is
     * the floor snapshot's snapshot_time as captured by the sweep that
     * advanced the floor (null until expiry first advances it). 410
     * messages cite it so a reconciling consumer knows WHEN its range
     * was lost.
     */
    data class ExpiryFloor(val earliestSnapshotId: Long, val earliestSnapshotTime: Instant?) {
        /** ", reached at <time>" suffix for 410 detail messages; empty pre-expiry. */
        fun reachedAtSuffix(): String = earliestSnapshotTime?.let { ", reached at $it" } ?: ""
    }

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

    /** The floor id + anchor time pair (one row read on hog_catalog). */
    fun expiryFloor(
        handle: Handle,
        catalogId: Long,
    ): ExpiryFloor =
        handle.createQuery(
            """
            SELECT earliest_snapshot_id, earliest_snapshot_time
            FROM hog_catalog WHERE catalog_id = :catalogId
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ ->
                ExpiryFloor(
                    earliestSnapshotId = rs.getLong("earliest_snapshot_id"),
                    earliestSnapshotTime =
                        rs.getObject("earliest_snapshot_time", java.time.OffsetDateTime::class.java)
                            ?.toInstant(),
                )
            }
            .one()

    /**
     * Resolve [atTimestamp] to the largest retained snapshot id whose
     * snapshot_time <= it (one indexed max-lookup on the hog_snapshot
     * PK, floored at [earliestSnapshotId]).
     *
     * - before the earliest RETAINED snapshot's time ->
     *   [HoglakeException.Expired] (that history is gone);
     * - between two snapshots -> the lower one;
     * - after head's time -> head.
     *
     * Ordering premise, stated honestly: snapshot_time is stamped with
     * clock_timestamp() INSIDE the commit tail, after the per-catalog
     * advisory lock is held (SnapshotRepo.insert / CommitService), so
     * times are non-decreasing in snapshot-id order — UNLESS the
     * database clock steps backwards (NTP correction). Under a clock
     * regression max(id) can prefer a later snapshot whose recorded
     * time is <= :ts while an earlier-id snapshot's is not; the result
     * still honors the snapshots' own RECORDED times (the contract),
     * just not wall-clock intuition. Callers get "the largest retained
     * id whose recorded snapshot_time <= ts", nothing stronger.
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
