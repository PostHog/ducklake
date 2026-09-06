package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.Transform
import org.jdbi.v3.core.Handle

/**
 * hog_partition_spec / hog_partition_field reads. Spec visibility
 * follows the standard versioned-row rule (visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot));
 * at most one spec row is visible per table per snapshot by
 * construction (AlterService end-snapshots the old spec when a new one
 * is set). Fields are immutable per (table_id, spec_id) and ordered by
 * key_index.
 */
object SpecRepo {
    /**
     * The table's partition spec visible at [snapshot], or null when the
     * table is unpartitioned there. A zero-field spec never exists
     * (SetPartitionSpec([]) just retires the old spec), so a returned
     * spec always has >= 1 field.
     */
    fun specAt(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): PartitionSpec? {
        val specId =
            h.createQuery(
                """
            SELECT spec_id FROM hog_partition_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .mapTo(Long::class.javaObjectType)
                .findOne()
                .orElse(null) ?: return null
        val fields =
            h.createQuery(
                """
            SELECT source_field_id, transform, transform_param
            FROM hog_partition_field
            WHERE catalog_id = :catalogId AND table_id = :tableId AND spec_id = :specId
            ORDER BY key_index
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("specId", specId)
                .map { rs, _ ->
                    PartitionFieldDef(
                        sourceFieldId = rs.getLong("source_field_id"),
                        transform = Transform.fromWire(rs.getString("transform")),
                        transformParam = rs.getObject("transform_param")?.let { (it as Number).toInt() },
                    )
                }
                .list()
        return PartitionSpec(specId, fields)
    }
}
