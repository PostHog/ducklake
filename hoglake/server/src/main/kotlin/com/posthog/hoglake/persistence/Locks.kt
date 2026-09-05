package com.posthog.hoglake.persistence

import org.jdbi.v3.core.Handle

/**
 * Advisory-lock discipline for the catalog. Every DDL / commit tail in
 * the codebase serializes writers per catalog through
 * [acquireCatalogCommitLock]; the lock is transaction-scoped
 * (pg_advisory_xact_lock) so release is tied to commit/rollback and can
 * never be leaked by a code path that forgets to unlock.
 */
object Locks {
    /**
     * classid discriminator for the per-catalog commit lock (the first
     * int of the two-int advisory lock form). The second int is the
     * catalog_id.
     */
    const val CATALOG_COMMIT_LOCK_CLASS: Int = 4740871

    /**
     * Take the per-catalog commit lock for the current transaction.
     * Blocks until the holder commits or rolls back. Must be called
     * inside an open transaction (xact-scoped locks are meaningless
     * outside one; Postgres raises an error).
     */
    fun acquireCatalogCommitLock(handle: Handle, catalogId: Long) {
        handle.createQuery("SELECT pg_advisory_xact_lock($CATALOG_COMMIT_LOCK_CLASS, ?::int)")
            .bind(0, catalogId)
            .mapToMap()
            .one()
    }
}
