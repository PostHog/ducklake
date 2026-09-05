package com.posthog.hoglake

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

private val log = KotlinLogging.logger {}

fun main() {
    val cfg = Config.fromEnv()
    val ds = Database.dataSource(cfg)
    Database.migrate(ds)
    val jdbi = Database.jdbi(ds)

    val app = App.build(cfg, jdbi)
    val background = app.startBackground()
    Runtime.getRuntime().addShutdownHook(
        Thread({ background.close() }, "hoglake-shutdown"),
    )
    log.info { "hoglake listening on :${cfg.port}" }
    embeddedServer(Netty, port = cfg.port) { app.module(this) }
        .start(wait = true)
}
