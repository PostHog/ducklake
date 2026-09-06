package com.posthog.hoglake

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.posthog.hoglake.api.installAlterRoutes
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installMaintenanceRoutes
import com.posthog.hoglake.api.installPartitionStatsRoutes
import com.posthog.hoglake.api.installPublicationRoutes
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.api.installViewRoutes
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.RequestId
import com.posthog.hoglake.observability.requestId
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.PartitionStatsService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.VerifyService
import com.posthog.hoglake.service.ViewService
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
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
    private val commitService = CommitService(jdbi, commitLockTimeoutMs = cfg.commitLockTimeoutMs)
    private val alterService = AlterService(jdbi)
    private val scanService = ScanService(jdbi)
    private val viewService = ViewService(jdbi)
    private val optionsService = OptionsService(jdbi)
    private val expiryService = ExpiryService(jdbi)
    private val verifyService = VerifyService(jdbi)

    /** Same threshold CompactionService plans with: debt == sweepable files. */
    private val partitionStatsService =
        PartitionStatsService(jdbi, smallFileThresholdBytes = cfg.compactionTargetBytes)
    private val removalStore = RemovalStore(cfg)
    private val cleanupService =
        CleanupService(jdbi, removalStore, ledgerRetentionSeconds = cfg.removalLedgerRetentionSeconds)

    /** Shared read/put store: hydrator footer reads + compaction rewrites. */
    private val objectStore = ObjectStore(cfg)

    /** One hydrator: the background sweep loop AND the rehydrate route. */
    private val hydrator = Hydrator(jdbi, objectStore, maxWholeObjectBytes = cfg.hydratorMaxWholeObjectBytes)
    private val compactionService =
        CompactionService(
            jdbi,
            objectStore,
            CompactionConfig(
                targetBytes = cfg.compactionTargetBytes,
                minInputFiles = cfg.compactionMinInputFiles,
                maxGroupsPerRun = cfg.compactionMaxGroupsPerRun,
            ),
        )

    /**
     * The process meter registry: Ktor http server metrics land here via
     * the MicrometerMetrics plugin, catalog-health gauges via
     * [catalogMetrics], and source-incremented counters via the
     * [Metrics] facade (bound in [build]). GET /metrics scrapes it.
     * /metrics itself is filtered out of the http request metrics.
     */
    val meterRegistry: PrometheusMeterRegistry =
        PrometheusMeterRegistry(PrometheusConfig.DEFAULT).apply {
            config().meterFilter(
                MeterFilter.deny { id ->
                    id.name.startsWith("ktor.http.server.requests") &&
                        id.getTag("route") == "/metrics"
                },
            )
        }

    /** Catalog-health gauge sampler; tests drive [CatalogMetrics.sampleOnce] directly. */
    val catalogMetrics = CatalogMetrics(jdbi, meterRegistry)

    companion object {
        fun build(
            cfg: Config,
            jdbi: Jdbi,
        ): App =
            App(cfg, jdbi).also {
                Metrics.bind(it.meterRegistry)
            }
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
        app.install(RequestId)
        app.install(CallLogging) {
            // Coroutine-safe MDC propagation of the request id, so audit
            // lines emitted inside request handling carry request_id.
            mdc(Audit.REQUEST_ID_MDC) { call -> call.requestId }
        }
        app.install(MicrometerMetrics) {
            registry = meterRegistry
            // Unmatched request paths must NOT each mint a `route` tag —
            // hostile path scans would otherwise grow series without bound.
            distinctNotRegisteredRoutes = false
        }
        app.install(StatusPages) { installErrorMapping() }
        app.routing {
            // Prometheus scrape endpoint — unauthenticated like /healthz,
            // and excluded from the http request metrics + audit log.
            get("/metrics") {
                call.respondText(
                    this@App.meterRegistry.scrape(),
                    ContentType.parse("text/plain; version=0.0.4; charset=utf-8"),
                )
            }
            // Process liveness only — never touches the database.
            get("/livez") {
                call.respondText("ok")
            }
            // Readiness: the catalog must be reachable. A dead pool must
            // read as unhealthy, not hang (the zombie-server incident:
            // /healthz said ok while every /v1 call hung on a dead PG).
            get("/healthz") {
                val ok =
                    runCatching {
                        jdbi.withHandle<Int, Exception> { h ->
                            h.createQuery("SELECT 1").mapTo(Int::class.javaObjectType).one()
                        }
                    }.isSuccess
                if (ok) {
                    call.respondText("ok")
                } else {
                    call.respondText("db unreachable", status = HttpStatusCode.ServiceUnavailable)
                }
            }
            get("/openapi.yaml") {
                val spec = javaClass.getResource("/openapi/hoglake.yaml")!!.readText()
                call.respondText(spec, ContentType.parse("application/yaml"))
            }
        }
        app.installApiRoutes(catalogService, commitService, cfg.instanceName)
        app.installAlterRoutes(alterService)
        app.installScanRoutes(scanService)
        app.installViewRoutes(viewService)
        app.installMaintenanceRoutes(
            optionsService,
            expiryService,
            cleanupService,
            compactionService,
            verifyService,
            hydrator,
        )
        app.installPartitionStatsRoutes(partitionStatsService)
        app.installPublicationRoutes()
    }

    /**
     * Start the background loops as coroutines under one supervisor
     * scope ([BackgroundLoops]): structured, bounded cancellation on
     * close, per-loop failure isolation, `intervalMs <= 0` = disabled.
     * The returned handle stops every loop and closes the stores.
     */
    fun startBackground(): AutoCloseable {
        val loops = BackgroundLoops()
        loops.register("hydrator", cfg.hydratorIntervalMs) { hydrator.runOnce() }
        loops.register("expiry", cfg.expiryIntervalMs) {
            expiryService.runOnceAllCatalogs(cfg.expiryBatchSize)
        }
        loops.register("cleanup", cfg.cleanupIntervalMs) {
            cleanupService.runOnceAllCatalogs(cfg.cleanupBatchSize)
        }
        // Default interval 0 = off for now; the manual trigger stays live.
        loops.register("compaction", cfg.compactionIntervalMs) {
            compactionService.runOnceAllCatalogs()
        }
        loops.register("metrics", cfg.metricsIntervalMs) { catalogMetrics.sampleOnce() }
        return AutoCloseable {
            loops.close()
            removalStore.close()
            objectStore.close()
        }
    }
}
