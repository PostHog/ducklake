package com.posthog.hoglake.commit

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * B2 — commit admission observability: the advisory-lock wait histogram
 * records on the commit path, and a commit parked behind a held catalog
 * lock past HOGLAKE_COMMIT_LOCK_TIMEOUT_MS surfaces as typed
 * backpressure — HTTP 503 `commit_queue_timeout` with a Retry-After
 * header, never a generic 500.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitAdmissionIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    /** Tiny admission bound so the held-lock test times out fast. */
    private val app =
        App.build(
            Config(hydratorIntervalMs = 0, metricsIntervalMs = 0, commitLockTimeoutMs = 250),
            db.jdbi,
        )
    private val catalogs = CatalogService(db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

    @BeforeEach
    fun bindRegistry() {
        // The facade is process-global; re-point it at our registry.
        Metrics.bind(app.meterRegistry)
    }

    private fun seed(catalog: String): Long {
        catalogs.createCatalog(catalog, "s3://adm/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        return db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, catalog).mapTo(Long::class.java).one()
        }
    }

    private fun appendRequest(path: String) =
        CommitRequest(
            appends =
                listOf(
                    TableAppend("ns", "t", listOf(FileRegistration(path, 5, 50))),
                ),
        )

    @Test
    fun `the lock-wait histogram records every commit's advisory-lock acquisition`() {
        seed("adm-histo")
        val timerBefore =
            app.meterRegistry.find("hoglake_commit_lock_wait").timer()?.count() ?: 0L

        CommitService(db.jdbi).commit("adm-histo", appendRequest("s3://adm/adm-histo/f1.parquet"))

        val timer = app.meterRegistry.get("hoglake_commit_lock_wait").timer()
        assertThat(timer.count()).isGreaterThan(timerBefore)
        // Prometheus exposition carries the _seconds histogram family.
        val scrape = app.meterRegistry.scrape()
        assertThat(scrape).contains("hoglake_commit_lock_wait_seconds_count")
        assertThat(scrape).contains("hoglake_commit_lock_wait_seconds_bucket")
    }

    @Test
    fun `a commit parked behind a held catalog lock returns 503 commit_queue_timeout with Retry-After`() {
        val catalogId = seed("adm-park")
        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val holder =
            thread(name = "adm-lock-holder") {
                db.jdbi.useTransaction<Exception> { h ->
                    Locks.acquireCatalogCommitLock(h, catalogId)
                    held.countDown()
                    release.await(30, TimeUnit.SECONDS)
                }
            }
        try {
            assertThat(held.await(10, TimeUnit.SECONDS)).isTrue()
            testApplication {
                application { app.module(this) }
                val response =
                    client.post("/v1/catalogs/adm-park/commit") {
                        contentType(ContentType.Application.Json)
                        setBody(
                            """
                            {"appends":[{"namespace":"ns","table":"t","files":[
                                {"path":"s3://adm/adm-park/parked.parquet",
                                 "record_count":1,"file_size_bytes":10}]}]}
                            """,
                        )
                    }
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.headers["Retry-After"])
                    .describedAs("503 must carry Retry-After (explicit, retryable backpressure)")
                    .isNotNull()
                val body = json.readTree(response.bodyAsText())
                assertThat(body["error"].asText()).isEqualTo("commit_queue_timeout")
                assertThat(body["detail"].asText()).contains("commit admission timed out")
            }
            // The typed failure ticks the commit counter as result=timeout.
            val counted =
                app.meterRegistry.get("hoglake_commits_total")
                    .tag("catalog", "adm-park")
                    .tag("result", "timeout")
                    .counter()
                    .count()
            assertThat(counted).isEqualTo(1.0)
        } finally {
            release.countDown()
            holder.join(30_000)
        }
        // Once the lock is free the identical commit succeeds (retryable).
        val result = CommitService(db.jdbi).commit("adm-park", appendRequest("s3://adm/adm-park/retry.parquet"))
        assertThat(result.snapshotId).isGreaterThan(0)
    }
}
