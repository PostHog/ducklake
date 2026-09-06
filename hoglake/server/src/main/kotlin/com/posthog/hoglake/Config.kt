package com.posthog.hoglake

/** Process configuration, environment-sourced. No config files in v1. */
data class Config(
    val port: Int = env("HOGLAKE_PORT", "8080").toInt(),
    /**
     * Human-facing name for THIS hoglake instance (e.g. "GigaHog"),
     * shown by the webui so operators can tell deployments apart.
     * Empty = unnamed.
     */
    val instanceName: String = env("HOGLAKE_INSTANCE_NAME", ""),
    val jdbcUrl: String = env("HOGLAKE_JDBC_URL", "jdbc:postgresql://localhost:5432/hoglake"),
    val dbUser: String = env("HOGLAKE_DB_USER", "hoglake"),
    val dbPassword: String = env("HOGLAKE_DB_PASSWORD", "hoglake"),
    val dbPoolSize: Int = env("HOGLAKE_DB_POOL_SIZE", "10").toInt(),
    /** S3/MinIO endpoint for the hydrator; empty = AWS default resolution. */
    val s3Endpoint: String = env("HOGLAKE_S3_ENDPOINT", ""),
    val s3Region: String = env("HOGLAKE_S3_REGION", "us-east-1"),
    val s3AccessKey: String = env("HOGLAKE_S3_ACCESS_KEY", ""),
    val s3SecretKey: String = env("HOGLAKE_S3_SECRET_KEY", ""),
    val s3PathStyle: Boolean = env("HOGLAKE_S3_PATH_STYLE", "true").toBoolean(),
    /** Hydrator poll interval; 0 disables the background loop (tests drive it directly). */
    val hydratorIntervalMs: Long = env("HOGLAKE_HYDRATOR_INTERVAL_MS", "5000").toLong(),
    /**
     * Cap on the hydrator's whole-object fallback fetch (used when a
     * registration has no usable footer_size): a larger file is marked
     * 'failed' (structural) instead of being buffered on the heap —
     * the OOM guard. Default 256 MiB.
     */
    val hydratorMaxWholeObjectBytes: Long =
        env("HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES", "${256L * 1024 * 1024}").toLong(),
    /** Expiry sweep interval; 0 disables. Sweeps are incremental (bounded per run). */
    val expiryIntervalMs: Long = env("HOGLAKE_EXPIRY_INTERVAL_MS", "60000").toLong(),
    /** Max snapshots expired per sweep per catalog (incremental expiry). */
    val expiryBatchSize: Int = env("HOGLAKE_EXPIRY_BATCH", "10000").toInt(),
    /** Cleanup drain interval; 0 disables. */
    val cleanupIntervalMs: Long = env("HOGLAKE_CLEANUP_INTERVAL_MS", "60000").toLong(),
    /** Queue entries drained per cleanup run; S3 deletes sub-batch at 500. */
    val cleanupBatchSize: Int = env("HOGLAKE_CLEANUP_BATCH", "2000").toInt(),
    /**
     * How long drained hog_file_removal rows (the soft-deleted cleanup
     * ledger) are kept before the sweep purges them. Default 30 days.
     */
    val removalLedgerRetentionSeconds: Long =
        env("HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS", "${30L * 24 * 60 * 60}").toLong(),
    /** Catalog-health gauge sample interval; <= 0 disables the sampler loop. */
    val metricsIntervalMs: Long = env("HOGLAKE_METRICS_INTERVAL_MS", "15000").toLong(),
    /**
     * Commit admission bound (B2): lock_timeout on the commit
     * transaction while it queues on the per-catalog advisory commit
     * lock. Expiry -> typed CommitQueueTimeout -> HTTP 503 with
     * Retry-After (retryable backpressure). 0 disables (unbounded wait).
     */
    val commitLockTimeoutMs: Long = env("HOGLAKE_COMMIT_LOCK_TIMEOUT_MS", "30000").toLong(),
    /**
     * Compaction sweep interval; default 0 = OFF for now (the manual
     * /maintenance/compact trigger still works). Rate-awareness is by
     * construction: tiny bites (see the batch knobs), never a storm.
     */
    val compactionIntervalMs: Long = env("HOGLAKE_COMPACTION_INTERVAL_MS", "0").toLong(),
    /** Compaction output target size; also the "small file" threshold for inputs. */
    val compactionTargetBytes: Long = env("HOGLAKE_COMPACTION_TARGET_BYTES", "${512L * 1024 * 1024}").toLong(),
    /** Minimum input files before a group is worth rewriting. */
    val compactionMinInputFiles: Int = env("HOGLAKE_COMPACTION_MIN_INPUT_FILES", "4").toInt(),
    /** Groups rewritten per run per catalog — the commit-storm guard. */
    val compactionMaxGroupsPerRun: Int = env("HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN", "1").toInt(),
) {
    companion object {
        private fun env(
            name: String,
            default: String,
        ): String = System.getenv(name)?.takeIf { it.isNotBlank() } ?: default

        fun fromEnv(): Config = Config()
    }
}
