package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.ConsumerTableOffset
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.time.OffsetDateTime
import java.util.UUID

/** hog_consumer_offset: the per-consumer committed-offset log primitive. */
object OffsetRepo {
    private val offsetMapper =
        RowMapper { rs, _ ->
            ConsumerOffset(
                consumerId = rs.getString("consumer_id"),
                tableUuid = rs.getObject("table_uuid") as UUID,
                committedSnapshot = rs.getLong("committed_snapshot"),
                updatedAt = rs.getObject("updated_at", OffsetDateTime::class.java).toInstant(),
            )
        }

    /**
     * Monotonic upsert: the update only fires when the stored offset is
     * <= the new one, so a regression returns no row -> null. Equal
     * re-commits succeed (idempotent). A consumer_id failing the length
     * CHECK -> [HoglakeException.Validation].
     */
    fun upsert(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset? =
        try {
            handle.createQuery(
                """
                INSERT INTO hog_consumer_offset
                    (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:catalogId, :consumerId, :tableUuid, :snapshotId)
                ON CONFLICT (catalog_id, consumer_id, table_uuid) DO UPDATE
                SET committed_snapshot = EXCLUDED.committed_snapshot,
                    updated_at = now()
                WHERE hog_consumer_offset.committed_snapshot <= EXCLUDED.committed_snapshot
                RETURNING consumer_id, table_uuid, committed_snapshot, updated_at
                """,
            )
                .bind("catalogId", catalogId)
                .bind("consumerId", consumerId)
                .bind("tableUuid", tableUuid)
                .bind("snapshotId", snapshotId)
                .map(offsetMapper)
                .findOne()
                .orElse(null)
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isCheckViolation(e)) {
                throw HoglakeException.Validation("invalid consumer_id (length must be 1..128)")
            }
            throw e
        }

    fun find(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset? =
        handle.createQuery(
            """
            SELECT consumer_id, table_uuid, committed_snapshot, updated_at
            FROM hog_consumer_offset
            WHERE catalog_id = :catalogId AND consumer_id = :consumerId
              AND table_uuid = :tableUuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("consumerId", consumerId)
            .bind("tableUuid", tableUuid)
            .map(offsetMapper)
            .findOne()
            .orElse(null)

    /**
     * Every offset row in the catalog, enriched with the table's most
     * recent name (live version, or the last version before a drop —
     * offsets survive drops by design). Ordered for stable grouping:
     * consumer, then namespace.table.
     */
    fun listAll(
        handle: Handle,
        catalogId: Long,
    ): List<ConsumerTableOffset> =
        handle.createQuery(
            """
            SELECT o.consumer_id, o.table_uuid, o.committed_snapshot,
                   o.updated_at, ns.name AS namespace, tv.name AS table_name,
                   (t.dropped_snapshot IS NOT NULL) AS table_dropped
            FROM hog_consumer_offset o
            LEFT JOIN hog_table t
              ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
            LEFT JOIN LATERAL (
                SELECT v.name, v.namespace_id
                FROM hog_table_version v
                WHERE v.catalog_id = t.catalog_id AND v.table_id = t.table_id
                ORDER BY v.begin_snapshot DESC
                LIMIT 1
            ) tv ON true
            LEFT JOIN hog_namespace ns
              ON ns.catalog_id = o.catalog_id
             AND ns.namespace_id = tv.namespace_id
            WHERE o.catalog_id = :catalogId
            ORDER BY o.consumer_id, ns.name NULLS LAST, tv.name NULLS LAST,
                     o.table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ ->
                ConsumerTableOffset(
                    consumerId = rs.getString("consumer_id"),
                    tableUuid = rs.getObject("table_uuid") as UUID,
                    committedSnapshot = rs.getLong("committed_snapshot"),
                    updatedAt =
                        rs.getObject("updated_at", OffsetDateTime::class.java).toInstant(),
                    namespace = rs.getString("namespace"),
                    tableName = rs.getString("table_name"),
                    tableDropped = rs.getBoolean("table_dropped"),
                )
            }
            .list()

    fun list(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
    ): List<ConsumerOffset> =
        handle.createQuery(
            """
            SELECT consumer_id, table_uuid, committed_snapshot, updated_at
            FROM hog_consumer_offset
            WHERE catalog_id = :catalogId AND consumer_id = :consumerId
            ORDER BY table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("consumerId", consumerId)
            .map(offsetMapper)
            .list()
}
