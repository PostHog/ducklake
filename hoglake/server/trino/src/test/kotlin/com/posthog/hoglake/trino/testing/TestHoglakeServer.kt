package com.posthog.hoglake.trino.testing

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking

/**
 * Boots the hoglake control plane in-process for connector integration
 * tests: migrate the given Postgres, assemble the App, serve on an
 * ephemeral port. Background loops stay off — the connector only needs
 * the request path. (Pattern mirrors the root project's Main.kt; the
 * root test fixtures are not importable across subprojects.)
 */
class TestHoglakeServer private constructor(
    private val server: EmbeddedServer<*, *>,
    private val dataSource: AutoCloseable,
    val port: Int,
) : AutoCloseable {

    val baseUri: String get() = "http://127.0.0.1:$port"

    override fun close() {
        server.stop(gracePeriodMillis = 100, timeoutMillis = 2_000)
        dataSource.close()
    }

    companion object {
        @JvmStatic
        fun start(jdbcUrl: String, dbUser: String, dbPassword: String): TestHoglakeServer {
            val cfg = Config(
                port = 0,
                jdbcUrl = jdbcUrl,
                dbUser = dbUser,
                dbPassword = dbPassword,
                hydratorIntervalMs = 0,
                expiryIntervalMs = 0,
                cleanupIntervalMs = 0,
                metricsIntervalMs = 0,
            )
            val ds = Database.dataSource(cfg)
            Database.migrate(ds)
            val app = App.build(cfg, Database.jdbi(ds))
            val server = embeddedServer(Netty, port = 0) { app.module(this) }.start(wait = false)
            val port = runBlocking { server.engine.resolvedConnectors().first().port }
            return TestHoglakeServer(server, ds, port)
        }
    }
}
