package com.posthog.hoglake

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.service.CatalogService
import io.ktor.http.ContentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import org.jdbi.v3.core.Jdbi

/**
 * Application assembly: wires config + Jdbi into services and installs
 * the Ktor module. The api package registers the /v1 routes; this file
 * owns the cross-cutting pieces (serialization, logging, error mapping,
 * health, spec serving) and the background hydrator.
 */
class App private constructor(
    val cfg: Config,
    val jdbi: Jdbi,
) {
    private val catalogService = CatalogService(jdbi)
    private val commitService = CommitService(jdbi)

    companion object {
        fun build(cfg: Config, jdbi: Jdbi): App = App(cfg, jdbi)
    }

    fun module(app: Application) {
        app.install(ContentNegotiation) {
            jackson {
                // The wire is snake_case with ISO-8601 date-times and
                // base64 byte fields, per openapi/hoglake.yaml.
                registerKotlinModule()
                registerModule(JavaTimeModule())
                propertyNamingStrategy = PropertyNamingStrategies.SNAKE_CASE
                disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                setSerializationInclusion(JsonInclude.Include.NON_NULL)
            }
        }
        app.install(CallLogging)
        app.install(StatusPages) { installErrorMapping() }
        app.routing {
            get("/healthz") {
                call.respondText("ok")
            }
            get("/openapi.yaml") {
                val spec = javaClass.getResource("/openapi/hoglake.yaml")!!.readText()
                call.respondText(spec, ContentType.parse("application/yaml"))
            }
        }
        app.installApiRoutes(catalogService, commitService)
    }

    /**
     * Start the background pieces (the hydrator sweep loop over its own
     * ObjectStore). The returned handle stops the loop and closes the
     * store; cfg.hydratorIntervalMs <= 0 leaves the loop off.
     */
    fun startBackground(): AutoCloseable {
        val store = ObjectStore(cfg)
        val loop = Hydrator(jdbi, store).startLoop(cfg.hydratorIntervalMs)
        return AutoCloseable {
            loop.close()
            store.close()
        }
    }
}
