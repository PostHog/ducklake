package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ConsumerOffset
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
