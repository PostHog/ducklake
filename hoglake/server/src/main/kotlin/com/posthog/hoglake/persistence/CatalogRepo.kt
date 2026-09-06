package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/** (new snapshot id, new schema version) minted by [CatalogRepo.allocateSnapshot]. */
data class SnapshotAlloc(val snapshotId: Long, val schemaVersion: Long)

/**
 * hog_catalog: identity, head pointers, and the per-catalog id
 * allocators. Allocator advances are only legal inside a transaction
 * that holds the catalog commit lock ([Locks.acquireCatalogCommitLock]);
 * callers own that discipline.
 */
object CatalogRepo {
    private val catalogMapper =
        RowMapper { rs, _ ->
            CatalogInfo(
                catalogId = rs.getLong("catalog_id"),
                name = rs.getString("name"),
                dataPath = rs.getString("data_path"),
                headSnapshotId = rs.getLong("last_snapshot_id"),
                schemaVersion = rs.getLong("schema_version"),
            )
        }

    /**
     * Insert a new catalog with all allocators at their defaults.
     * Duplicate name -> [HoglakeException.AlreadyExists]; a name failing
     * the schema CHECK -> [HoglakeException.Validation].
     */
    fun insert(
        handle: Handle,
        name: String,
        dataPath: String,
    ): CatalogInfo =
        try {
            handle.createQuery(
                """
                INSERT INTO hog_catalog (name, data_path)
                VALUES (:name, :dataPath)
                RETURNING catalog_id, name, data_path, last_snapshot_id, schema_version
                """,
            )
                .bind("name", name)
                .bind("dataPath", dataPath)
                .map(catalogMapper)
                .one()
        } catch (e: UnableToExecuteStatementException) {
            when {
                Pg.isUniqueViolation(e) ->
                    throw HoglakeException.AlreadyExists("catalog '$name' already exists")
                Pg.isCheckViolation(e) ->
                    throw HoglakeException.Validation(
                        "invalid catalog name '$name' (must match ^[a-z][a-z0-9_-]{0,62}$)",
                    )
                else -> throw e
            }
        }

    fun findByName(
        handle: Handle,
        name: String,
    ): CatalogInfo? =
        handle.createQuery(
            """
            SELECT catalog_id, name, data_path, last_snapshot_id, schema_version
            FROM hog_catalog WHERE name = :name
            """,
        )
            .bind("name", name)
            .map(catalogMapper)
            .findOne()
            .orElse(null)

    fun listAll(handle: Handle): List<CatalogInfo> =
        handle.createQuery(
            """
            SELECT catalog_id, name, data_path, last_snapshot_id, schema_version
            FROM hog_catalog ORDER BY name
            """,
        )
            .map(catalogMapper)
            .list()

    /**
     * Mint the next snapshot id (and schema version) for a DDL/commit
     * tail. Caller must hold the catalog commit lock.
     */
    fun allocateSnapshot(
        handle: Handle,
        catalogId: Long,
    ): SnapshotAlloc =
        handle.createQuery(
            """
            UPDATE hog_catalog
            SET last_snapshot_id = last_snapshot_id + 1,
                schema_version   = schema_version + 1
            WHERE catalog_id = :catalogId
            RETURNING last_snapshot_id, schema_version
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ -> SnapshotAlloc(rs.getLong("last_snapshot_id"), rs.getLong("schema_version")) }
            .one()

    /** Allocate the next namespace id. Caller holds the commit lock. */
    fun allocateNamespaceId(
        handle: Handle,
        catalogId: Long,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_catalog SET next_namespace_id = next_namespace_id + 1
            WHERE catalog_id = :catalogId
            RETURNING next_namespace_id - 1 AS allocated
            """,
        )
            .bind("catalogId", catalogId)
            .mapTo(Long::class.javaObjectType)
            .one()

    /** Allocate the next table id. Caller holds the commit lock. */
    fun allocateTableId(
        handle: Handle,
        catalogId: Long,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_catalog SET next_table_id = next_table_id + 1
            WHERE catalog_id = :catalogId
            RETURNING next_table_id - 1 AS allocated
            """,
        )
            .bind("catalogId", catalogId)
            .mapTo(Long::class.javaObjectType)
            .one()
}
