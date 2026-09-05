package com.posthog.hoglake

/** Process configuration, environment-sourced. No config files in v1. */
data class Config(
    val port: Int = env("HOGLAKE_PORT", "8080").toInt(),
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
) {
    companion object {
        private fun env(name: String, default: String): String =
            System.getenv(name)?.takeIf { it.isNotBlank() } ?: default

        fun fromEnv(): Config = Config()
    }
}
