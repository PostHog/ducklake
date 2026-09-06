package com.posthog.hoglake

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.postgres.PostgresPlugin
import javax.sql.DataSource

/**
 * The service owns ALL catalog connections (README.md §7): one pool,
 * statement timeouts, no third-party sessions. Migrations run at startup
 * under an advisory lock so concurrent replicas don't race DDL.
 */
object Database {
    const val MIGRATION_LOCK_KEY: Long = 0x486F674C616B6531 // "HogLake1"

    fun dataSource(cfg: Config): HikariDataSource {
        val hc =
            HikariConfig().apply {
                jdbcUrl = cfg.jdbcUrl
                username = cfg.dbUser
                password = cfg.dbPassword
                maximumPoolSize = cfg.dbPoolSize
                poolName = "hoglake"
                // Fail fast when the catalog is unreachable — a request must
                // 500/503 quickly, never hang on connection acquisition.
                connectionTimeout = 5_000
                validationTimeout = 2_500
                // No idle-in-transaction squatters, ever (README.md §7).
                connectionInitSql =
                    "SET idle_in_transaction_session_timeout = '30s'; SET statement_timeout = '60s'"
            }
        return HikariDataSource(hc)
    }

    fun migrate(ds: DataSource) {
        ds.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("SELECT pg_advisory_lock($MIGRATION_LOCK_KEY)")
            }
            try {
                Flyway.configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .load()
                    .migrate()
            } finally {
                conn.createStatement().use { st ->
                    st.execute("SELECT pg_advisory_unlock($MIGRATION_LOCK_KEY)")
                }
            }
        }
    }

    fun jdbi(ds: DataSource): Jdbi =
        Jdbi.create(ds)
            .installPlugin(KotlinPlugin())
            .installPlugin(PostgresPlugin())
}
