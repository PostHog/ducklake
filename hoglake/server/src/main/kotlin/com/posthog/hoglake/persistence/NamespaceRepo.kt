package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/**
 * hog_namespace. Not snapshot-versioned in v1 (rename disallowed, drop
 * requires emptiness); liveness is the `dropped` flag.
 */
object NamespaceRepo {
    /**
     * Insert a namespace row. The live-name unique index backstops the
     * service-level duplicate check -> [HoglakeException.AlreadyExists].
     */
    fun insert(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        name: String,
    ): NamespaceInfo =
        try {
            handle.createUpdate(
                """
                INSERT INTO hog_namespace (catalog_id, namespace_id, name)
                VALUES (:catalogId, :namespaceId, :name)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("namespaceId", namespaceId)
                .bind("name", name)
                .execute()
                .let { NamespaceInfo(namespaceId, name) }
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isUniqueViolation(e)) {
                throw HoglakeException.AlreadyExists("namespace '$name' already exists")
            }
            throw e
        }

    fun findLiveByName(
        handle: Handle,
        catalogId: Long,
        name: String,
    ): NamespaceInfo? =
        handle.createQuery(
            """
            SELECT namespace_id, name FROM hog_namespace
            WHERE catalog_id = :catalogId AND name = :name AND NOT dropped
            """,
        )
            .bind("catalogId", catalogId)
            .bind("name", name)
            .map { rs, _ -> NamespaceInfo(rs.getLong("namespace_id"), rs.getString("name")) }
            .findOne()
            .orElse(null)

    fun listLive(
        handle: Handle,
        catalogId: Long,
    ): List<NamespaceInfo> =
        handle.createQuery(
            """
            SELECT namespace_id, name FROM hog_namespace
            WHERE catalog_id = :catalogId AND NOT dropped
            ORDER BY name
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ -> NamespaceInfo(rs.getLong("namespace_id"), rs.getString("name")) }
            .list()
}
