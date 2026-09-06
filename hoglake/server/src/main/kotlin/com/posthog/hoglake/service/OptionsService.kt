package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * A PATCH-style field: distinguishes "not mentioned" (leave unchanged)
 * from "explicitly set" — where the set value may itself be null (for
 * snapshot_retention_seconds, null = expiry disabled).
 */
sealed interface PatchField<out T> {
    /** Field absent from the request: leave the stored value unchanged. */
    data object Absent : PatchField<Nothing>

    /** Field present; [value] null means "set to NULL". */
    data class Set<T>(val value: T?) : PatchField<T>
}

/**
 * The lifecycle-relevant slice of a hog_catalog row. Shared by the
 * options/expiry/cleanup services (CatalogRepo's CatalogInfo predates
 * the V3 retention columns and cannot grow them from here).
 */
internal data class LifecycleCatalog(
    val catalogId: Long,
    val name: String,
    val headSnapshotId: Long,
    val snapshotRetentionSeconds: Long?,
    val consumerFloor: Boolean,
    val earliestSnapshotId: Long,
) {
    fun options() =
        CatalogOptions(
            snapshotRetentionSeconds = snapshotRetentionSeconds,
            consumerFloor = consumerFloor,
            earliestSnapshotId = earliestSnapshotId,
        )

    companion object {
        fun find(
            handle: Handle,
            catalog: String,
        ): LifecycleCatalog? =
            handle.createQuery(
                """
                SELECT catalog_id, name, last_snapshot_id,
                       snapshot_retention_seconds, consumer_floor, earliest_snapshot_id
                FROM hog_catalog WHERE name = :name
                """,
            )
                .bind("name", catalog)
                .map { rs, _ ->
                    LifecycleCatalog(
                        catalogId = rs.getLong("catalog_id"),
                        name = rs.getString("name"),
                        headSnapshotId = rs.getLong("last_snapshot_id"),
                        snapshotRetentionSeconds =
                            rs.getLong("snapshot_retention_seconds")
                                .let { if (rs.wasNull()) null else it },
                        consumerFloor = rs.getBoolean("consumer_floor"),
                        earliestSnapshotId = rs.getLong("earliest_snapshot_id"),
                    )
                }
                .findOne()
                .orElse(null)

        fun require(
            handle: Handle,
            catalog: String,
        ): LifecycleCatalog = find(handle, catalog) ?: throw HoglakeException.NotFound("catalog '$catalog'")
    }
}

/**
 * The catalog options surface (GET/PATCH /v1/catalogs/{catalog}/options):
 * retention as a catalog property the service enforces continuously
 * (README.md §6), never a cron bolted on outside it.
 *
 * PATCH semantics: a field absent from the request is unchanged; an
 * explicit null snapshot_retention_seconds disables expiry; a value
 * must be positive (mirrors the schema CHECK) or the patch is rejected
 * with Validation before touching the row.
 */
class OptionsService(private val jdbi: Jdbi) {
    fun get(catalog: String): CatalogOptions =
        jdbi.withHandleUnchecked { h -> LifecycleCatalog.require(h, catalog).options() }

    fun patch(
        catalog: String,
        snapshotRetentionSeconds: PatchField<Long>,
        consumerFloor: Boolean?,
    ): CatalogOptions =
        Audit.audited(
            "options_patch",
            catalog,
            null,
            detail = { opts ->
                "snapshot_retention_seconds=${opts.snapshotRetentionSeconds} " +
                    "consumer_floor=${opts.consumerFloor}"
            },
        ) {
            if (snapshotRetentionSeconds is PatchField.Set) {
                val v = snapshotRetentionSeconds.value
                if (v != null && v <= 0) {
                    throw HoglakeException.Validation(
                        "snapshot_retention_seconds must be positive (got $v); " +
                            "null disables expiry",
                    )
                }
            }
            jdbi.inTransactionUnchecked { h ->
                val cat = LifecycleCatalog.require(h, catalog)
                if (snapshotRetentionSeconds is PatchField.Set) {
                    h.createUpdate(
                        """
                    UPDATE hog_catalog SET snapshot_retention_seconds = :v
                    WHERE catalog_id = :catalogId
                    """,
                    )
                        .apply {
                            val v = snapshotRetentionSeconds.value
                            if (v == null) bindNull("v", java.sql.Types.BIGINT) else bind("v", v)
                        }
                        .bind("catalogId", cat.catalogId)
                        .execute()
                }
                if (consumerFloor != null) {
                    h.createUpdate(
                        "UPDATE hog_catalog SET consumer_floor = :v WHERE catalog_id = :catalogId",
                    )
                        .bind("v", consumerFloor)
                        .bind("catalogId", cat.catalogId)
                        .execute()
                }
                LifecycleCatalog.require(h, catalog).options()
            }
        }
}
