package com.posthog.hoglake.service

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ViewInfo
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.persistence.ViewRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * View DDL + reads, following the CatalogService commit pattern: one
 * transaction that (1) takes the per-catalog commit lock, (2) allocates
 * the next snapshot off hog_catalog, (3) records the snapshot + typed
 * change row (view_created / view_dropped with object_id = view_id),
 * then (4) writes the hog_view row.
 *
 * The view SQL is stored VERBATIM: the server never parses, validates,
 * or rewrites it — it is an opaque string in the declared dialect,
 * interpreted only by the query engine that reads it back.
 */
class ViewService(private val jdbi: Jdbi) {
    fun create(
        catalog: String,
        namespace: String,
        name: String,
        sql: String,
        dialect: String = "trino",
    ): ViewInfo =
        Audit.audited(
            "view_create",
            catalog,
            "$namespace.$name",
            detail = { "dialect=$dialect" },
        ) {
            Identifiers.validate("view", name)
            if (sql.isBlank()) throw HoglakeException.Validation("view sql must not be blank")
            if (dialect.isBlank()) throw HoglakeException.Validation("view dialect must not be blank")
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns = requireNamespace(h, cat.catalogId, catalog, namespace)
                if (ViewRepo.findLiveByName(h, cat.catalogId, ns.namespaceId, ns.name, name) != null) {
                    throw HoglakeException.AlreadyExists(
                        "view '$name' already exists in namespace '$namespace'",
                    )
                }
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                val viewId = ViewRepo.allocateViewId(h, cat.catalogId)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.VIEW_CREATED,
                    viewId,
                )
                val viewUuid =
                    ViewRepo.insert(
                        h,
                        cat.catalogId,
                        viewId,
                        ns.namespaceId,
                        name,
                        dialect,
                        sql,
                        alloc.snapshotId,
                    )
                ViewInfo(
                    viewId = viewId,
                    viewUuid = viewUuid,
                    namespace = ns.name,
                    name = name,
                    dialect = dialect,
                    sql = sql,
                )
            }
        }

    fun get(
        catalog: String,
        namespace: String,
        name: String,
    ): ViewInfo =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val ns = requireNamespace(h, cat.catalogId, catalog, namespace)
            ViewRepo.findLiveByName(h, cat.catalogId, ns.namespaceId, ns.name, name)
                ?: throw HoglakeException.NotFound(
                    "view '$namespace.$name' in catalog '$catalog'",
                )
        }

    /** Live views in the namespace, ordered by name. */
    fun list(
        catalog: String,
        namespace: String,
    ): List<ViewInfo> =
        jdbi.withHandleUnchecked { h ->
            val cat = requireCatalog(h, catalog)
            val ns = requireNamespace(h, cat.catalogId, catalog, namespace)
            ViewRepo.listLive(h, cat.catalogId, ns.namespaceId, ns.name)
        }

    fun drop(
        catalog: String,
        namespace: String,
        name: String,
    ): CommitResult =
        Audit.audited(
            "view_drop",
            catalog,
            "$namespace.$name",
            detail = { "snapshot=${it.snapshotId}" },
        ) {
            jdbi.inTransactionUnchecked { h ->
                val cat = requireCatalog(h, catalog)
                Locks.acquireCatalogCommitLock(h, cat.catalogId)
                val ns = requireNamespace(h, cat.catalogId, catalog, namespace)
                val view =
                    ViewRepo.findLiveByName(h, cat.catalogId, ns.namespaceId, ns.name, name)
                        ?: throw HoglakeException.NotFound(
                            "view '$namespace.$name' in catalog '$catalog'",
                        )
                val alloc = CatalogRepo.allocateSnapshot(h, cat.catalogId)
                SnapshotRepo.insert(h, cat.catalogId, alloc.snapshotId, alloc.schemaVersion)
                SnapshotRepo.insertChange(
                    h,
                    cat.catalogId,
                    alloc.snapshotId,
                    ChangeKind.VIEW_DROPPED,
                    view.viewId,
                )
                ViewRepo.endLive(h, cat.catalogId, view.viewId, alloc.snapshotId)
                CommitResult(snapshotId = alloc.snapshotId, schemaVersion = alloc.schemaVersion)
            }
        }

    private fun requireCatalog(
        h: Handle,
        name: String,
    ) = CatalogRepo.findByName(h, name)
        ?: throw HoglakeException.NotFound("catalog '$name'")

    private fun requireNamespace(
        h: Handle,
        catalogId: Long,
        catalog: String,
        name: String,
    ): NamespaceInfo =
        NamespaceRepo.findLiveByName(h, catalogId, name)
            ?: throw HoglakeException.NotFound("namespace '$name' in catalog '$catalog'")
}
