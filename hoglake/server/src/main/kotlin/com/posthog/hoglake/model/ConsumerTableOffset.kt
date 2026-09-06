package com.posthog.hoglake.model

import java.time.Instant
import java.util.UUID

/**
 * One consumer-offset row enriched for listing: the table's most recent
 * name/namespace (offsets deliberately outlive drops so consumers SEE
 * incarnation changes — a dropped table still lists, flagged). Name and
 * namespace are null only for a uuid no incarnation ever carried
 * (pre-guard garbage; commitOffset refuses new ones).
 */
data class ConsumerTableOffset(
    val consumerId: String,
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
    val namespace: String?,
    val tableName: String?,
    val tableDropped: Boolean,
)
