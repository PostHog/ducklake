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
     * Discriminator for the per-catalog commit lock: the upper 32 bits
     * of the single-bigint advisory lock key. The lower 32 bits are the
     * catalog_id (masked to 32 bits, so the key stays collision-free
     * until 2^32 catalogs). The two-int lock form is NOT used: its
     * second argument is an int4, which errors for catalog_id > 2^31.
     */
    const val CATALOG_COMMIT_LOCK_CLASS: Int = 4740871

    /**
     * Take the per-catalog commit lock for the current transaction.
     * Blocks until the holder commits or rolls back. Must be called
     * inside an open transaction (xact-scoped locks are meaningless
     * outside one; Postgres raises an error).
     *
     * The lock key MUST be computed identically everywhere — any copy of
     * this SQL (tests probing contention included) has to build the same
     * `(class << 32) | (catalog_id & 0xFFFFFFFF)` bigint, or commit
     * serialization silently breaks (two writers would take DIFFERENT
     * locks for the same catalog and interleave the commit tail).
     */
    fun acquireCatalogCommitLock(
        handle: Handle,
        catalogId: Long,
    ) {
        handle.createQuery(
            "SELECT pg_advisory_xact_lock(($CATALOG_COMMIT_LOCK_CLASS::bigint << 32) | (?::bigint & 4294967295))",
        )
            .bind(0, catalogId)
            .mapToMap()
            .one()
    }
}
