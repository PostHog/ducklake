package com.posthog.hoglake.testing

import com.posthog.hoglake.Database
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jdbi.v3.core.Jdbi
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Shared Postgres fixture for integration tests. One container per JVM
 * (Testcontainers reuses it across classes); each test class gets a
 * fresh database via [freshDatabase] so tests never share state.
 *
 * Usage:
 *   @Tag("integration")
 *   class MyIntegrationTest {
 *       private val db = PgTestSupport.freshDatabase()
 *       // db.jdbi ...
 *   }
 */
object PgTestSupport {
    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer("postgres:16")
            .withUsername("hoglake")
            .withPassword("hoglake")
            .also { it.start() }
    }

    private var dbCounter = 0

    class TestDb(val dataSource: HikariDataSource, val jdbi: Jdbi, val jdbcUrl: String) :
        AutoCloseable {
        override fun close() = dataSource.close()
    }

    /**
     * Create a new empty database and apply [sql] verbatim (no
     * migrations). Used by the schema-equivalence check to materialize
     * the canonical schema.sql.
     */
    @Synchronized
    fun freshDatabaseRaw(sql: String): TestDb {
        val db = freshEmpty()
        db.jdbi.useHandle<Exception> { h -> h.createScript(sql).execute() }
        return db
    }

    /** Create a new empty database, run migrations, return a pooled Jdbi. */
    @Synchronized
    fun freshDatabase(): TestDb = freshEmpty().also { Database.migrate(it.dataSource) }

    @Synchronized
    private fun freshEmpty(): TestDb {
        val name = "hoglake_test_${dbCounter++}"
        container.createConnection("").use { conn ->
            conn.createStatement().use { it.execute("CREATE DATABASE $name") }
        }
        val url = container.jdbcUrl.replace("/${container.databaseName}", "/$name")
        val ds =
            HikariDataSource(
                HikariConfig().apply {
                    jdbcUrl = url
                    username = container.username
                    password = container.password
                    maximumPoolSize = 8
                    poolName = name
                },
            )
        return TestDb(ds, Database.jdbi(ds), url)
    }
}
