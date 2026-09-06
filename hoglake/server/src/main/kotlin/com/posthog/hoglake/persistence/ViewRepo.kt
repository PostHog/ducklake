package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ViewInfo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.util.UUID

/**
 * hog_view: single versioned table (begin/end snapshot), one live row
 * per (namespace, name) enforced by the hog_view_live_name partial
 * index. The view id allocator lives on hog_catalog.next_view_id;
 * advancing it is only legal under the catalog commit lock.
 */
object ViewRepo {
    /** Maps a hog_view row; the namespace NAME is supplied by the caller. */
    private fun viewMapper(namespace: String) =
        RowMapper { rs, _ ->
            ViewInfo(
                viewId = rs.getLong("view_id"),
                viewUuid = rs.getObject("view_uuid") as UUID,
                namespace = namespace,
                name = rs.getString("name"),
                dialect = rs.getString("dialect"),
                sql = rs.getString("sql"),
            )
        }

    /** Allocate the next view id. Caller holds the commit lock. */
    fun allocateViewId(
        handle: Handle,
        catalogId: Long,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_catalog SET next_view_id = next_view_id + 1
            WHERE catalog_id = :catalogId
            RETURNING next_view_id - 1 AS allocated
            """,
        )
            .bind("catalogId", catalogId)
            .mapTo(Long::class.javaObjectType)
            .one()

    /**
     * Insert a view row live from [beginSnapshot]. The live-name unique
     * index backstops the service-level duplicate check ->
     * [HoglakeException.AlreadyExists]. Returns the generated view_uuid.
     */
    fun insert(
        handle: Handle,
        catalogId: Long,
        viewId: Long,
        namespaceId: Long,
        name: String,
        dialect: String,
        sql: String,
        beginSnapshot: Long,
    ): UUID =
        try {
            handle.createQuery(
                """
                INSERT INTO hog_view (catalog_id, view_id, namespace_id, name,
                                      dialect, sql, begin_snapshot)
                VALUES (:catalogId, :viewId, :namespaceId, :name,
                        :dialect, :sql, :beginSnapshot)
                RETURNING view_uuid
                """,
            )
                .bind("catalogId", catalogId)
                .bind("viewId", viewId)
                .bind("namespaceId", namespaceId)
                .bind("name", name)
                .bind("dialect", dialect)
                .bind("sql", sql)
                .bind("beginSnapshot", beginSnapshot)
                .map { rs, _ -> rs.getObject("view_uuid") as UUID }
                .one()
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isUniqueViolation(e)) {
                throw HoglakeException.AlreadyExists("view '$name' already exists")
            }
            throw e
        }

    /** Resolve a live (head-visible) view by name. [namespace] is only carried into the result. */
    fun findLiveByName(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        namespace: String,
        name: String,
    ): ViewInfo? =
        handle.createQuery(
            """
            SELECT view_id, view_uuid, name, dialect, sql FROM hog_view
            WHERE catalog_id = :catalogId AND namespace_id = :namespaceId
              AND name = :name AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .bind("name", name)
            .map(viewMapper(namespace))
            .findOne()
            .orElse(null)

    /** All live views in a namespace, ordered by name. */
    fun listLive(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        namespace: String,
    ): List<ViewInfo> =
        handle.createQuery(
            """
            SELECT view_id, view_uuid, name, dialect, sql FROM hog_view
            WHERE catalog_id = :catalogId AND namespace_id = :namespaceId
              AND end_snapshot IS NULL
            ORDER BY name
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .map(viewMapper(namespace))
            .list()

    /** Drop tail: end-snapshot the view's live row. */
    fun endLive(
        handle: Handle,
        catalogId: Long,
        viewId: Long,
        snapshot: Long,
    ) {
        handle.createUpdate(
            """
            UPDATE hog_view SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND view_id = :viewId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("viewId", viewId)
            .bind("snapshot", snapshot)
            .execute()
    }
}
