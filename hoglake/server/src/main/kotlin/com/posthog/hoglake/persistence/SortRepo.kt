package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.SortSpec
import org.jdbi.v3.core.Handle

/**
 * hog_sort_spec / hog_sort_field reads — SpecRepo's sibling. Spec
 * visibility follows the standard versioned-row rule (visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot));
 * at most one sort spec is visible per table per snapshot by
 * construction (AlterService end-snapshots the old spec when a new one
 * is set). Fields are immutable per (table_id, sort_id) and ordered by
 * key_index.
 */
object SortRepo {
    /**
     * The table's sort order visible at [snapshot], or null when the
     * table is unsorted there. A zero-field spec never exists
     * (SetSortOrder([]) just retires the old spec), so a returned spec
     * always has >= 1 field.
     */
    fun sortSpecAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): SortSpec? {
        val sortId =
            handle.createQuery(
                """
            SELECT sort_id FROM hog_sort_spec
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
            handle.createQuery(
                """
            SELECT source_field_id, direction, null_order
            FROM hog_sort_field
            WHERE catalog_id = :catalogId AND table_id = :tableId AND sort_id = :sortId
            ORDER BY key_index
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("sortId", sortId)
                .map { rs, _ ->
                    SortFieldDef(
                        sourceFieldId = rs.getLong("source_field_id"),
                        direction = SortDirection.fromWire(rs.getString("direction")),
                        nullOrder = NullOrder.fromWire(rs.getString("null_order")),
                    )
                }
                .list()
        return SortSpec(sortId, fields)
    }
}
