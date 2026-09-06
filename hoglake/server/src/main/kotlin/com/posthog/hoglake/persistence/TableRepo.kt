package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.util.UUID

/** Resolved table identity + the version row that made it visible. */
data class TableRow(
    val tableId: Long,
    val tableUuid: UUID,
    val namespaceId: Long,
    val name: String,
)

/** Rollup row from hog_table_stats. */
data class TableStatsRow(
    val recordCount: Long,
    val fileSizeBytes: Long,
    val nextRowId: Long,
)

/**
 * hog_table (immutable identity) + hog_table_version (versioned
 * name/namespace) + hog_column + hog_table_stats.
 *
 * Versioned-row visibility everywhere: a row is visible at snapshot S
 * iff begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot).
 */
object TableRepo {
    private val tableRowMapper =
        RowMapper { rs, _ ->
            TableRow(
                tableId = rs.getLong("table_id"),
                tableUuid = rs.getObject("table_uuid") as UUID,
                namespaceId = rs.getLong("namespace_id"),
                name = rs.getString("name"),
            )
        }

    private val columnMapper =
        RowMapper { rs, _ ->
            Column(
                fieldId = rs.getLong("field_id"),
                ordinal = rs.getInt("ordinal"),
                def =
                    ColumnDef(
                        name = rs.getString("name"),
                        type = ColType.fromWire(rs.getString("col_type")),
                        typeParams = Pg.fromJson(rs.getString("type_params")),
                        nullable = rs.getBoolean("nullable"),
                    ),
            )
        }

    /** Insert the identity row; returns the generated table_uuid. */
    fun insertTable(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        createdSnapshot: Long,
    ): UUID =
        handle.createQuery(
            """
            INSERT INTO hog_table (catalog_id, table_id, created_snapshot)
            VALUES (:catalogId, :tableId, :createdSnapshot)
            RETURNING table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("createdSnapshot", createdSnapshot)
            .map { rs, _ -> rs.getObject("table_uuid") as UUID }
            .one()

    /**
     * Allocate [count] consecutive field ids from hog_table.next_field_id
     * (1-based). Returns the first allocated id. Caller holds the
     * catalog commit lock.
     */
    fun allocateFieldIds(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        count: Int,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_table SET next_field_id = next_field_id + :count
            WHERE catalog_id = :catalogId AND table_id = :tableId
            RETURNING next_field_id - :count AS first_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("count", count)
            .mapTo(Long::class.javaObjectType)
            .one()

    /**
     * Insert the initial (or post-rename) version row. The live-name
     * unique index backstops the duplicate check ->
     * [HoglakeException.AlreadyExists].
     */
    fun insertVersion(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        beginSnapshot: Long,
        namespaceId: Long,
        name: String,
    ) {
        try {
            handle.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                VALUES (:catalogId, :tableId, :beginSnapshot, :namespaceId, :name)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("beginSnapshot", beginSnapshot)
                .bind("namespaceId", namespaceId)
                .bind("name", name)
                .execute()
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isUniqueViolation(e)) {
                throw HoglakeException.AlreadyExists("table '$name' already exists")
            }
            throw e
        }
    }

    fun insertColumns(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        beginSnapshot: Long,
        columns: List<Column>,
    ) {
        val batch =
            handle.prepareBatch(
                """
            INSERT INTO hog_column
                (catalog_id, table_id, field_id, begin_snapshot, name, col_type,
                 type_params, nullable, ordinal)
            VALUES (:catalogId, :tableId, :fieldId, :beginSnapshot, :name, :colType,
                    :typeParams::jsonb, :nullable, :ordinal)
            """,
            )
        for (c in columns) {
            batch
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", c.fieldId)
                .bind("beginSnapshot", beginSnapshot)
                .bind("name", c.def.name)
                .bind("colType", c.def.type.wire)
                .bind("typeParams", Pg.toJson(c.def.typeParams))
                .bind("nullable", c.def.nullable)
                .bind("ordinal", c.ordinal)
                .add()
        }
        batch.execute()
    }

    /** Create the one-per-table rollup/allocator row, all zeros. */
    fun insertStatsRow(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
    ) {
        handle.createUpdate(
            """
            INSERT INTO hog_table_stats (catalog_id, table_id)
            VALUES (:catalogId, :tableId)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .execute()
    }

    /** Resolve a table by (namespace, name) visible at [snapshot]. */
    fun findAt(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        name: String,
        snapshot: Long,
    ): TableRow? =
        handle.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name
            FROM hog_table_version tv
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId
              AND tv.namespace_id = :namespaceId
              AND tv.name = :name
              AND tv.begin_snapshot <= :snapshot
              AND (tv.end_snapshot IS NULL OR :snapshot < tv.end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .bind("name", name)
            .bind("snapshot", snapshot)
            .map(tableRowMapper)
            .findOne()
            .orElse(null)

    /** Resolve a live (head-visible) table by (namespace, name). */
    fun findLive(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        name: String,
    ): TableRow? =
        handle.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name
            FROM hog_table_version tv
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId
              AND tv.namespace_id = :namespaceId
              AND tv.name = :name
              AND tv.end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .bind("name", name)
            .map(tableRowMapper)
            .findOne()
            .orElse(null)

    /** All live tables in a namespace, ordered by name. */
    fun listLive(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
    ): List<TableRow> =
        handle.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name
            FROM hog_table_version tv
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId
              AND tv.namespace_id = :namespaceId
              AND tv.end_snapshot IS NULL
            ORDER BY tv.name
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .map(tableRowMapper)
            .list()

    /** Column definitions visible at [snapshot], ordered by ordinal. */
    fun columnsAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): List<Column> =
        handle.createQuery(
            """
            SELECT field_id, name, col_type, type_params, nullable, ordinal
            FROM hog_column
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            ORDER BY ordinal
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .map(columnMapper)
            .list()

    fun stats(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
    ): TableStatsRow =
        handle.createQuery(
            """
            SELECT record_count, file_size_bytes, next_row_id
            FROM hog_table_stats
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ ->
                TableStatsRow(
                    recordCount = rs.getLong("record_count"),
                    fileSizeBytes = rs.getLong("file_size_bytes"),
                    nextRowId = rs.getLong("next_row_id"),
                )
            }
            .findOne()
            .orElse(TableStatsRow(0, 0, 0))

    /**
     * Drop bookkeeping at [snapshot]: sets hog_table.dropped_snapshot
     * and end-snapshots the live version and column rows. Data files
     * are end-snapshotted by [FileRepo.endLiveFiles].
     */
    fun markDropped(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        handle.createUpdate(
            """
            UPDATE hog_table SET dropped_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
        handle.createUpdate(
            """
            UPDATE hog_table_version SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
        handle.createUpdate(
            """
            UPDATE hog_column SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
    }
}
