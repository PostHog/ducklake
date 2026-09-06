package com.posthog.hoglake.observability

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * /metrics end to end: the Prometheus endpoint, the catalog-health
 * gauges after one sampler pass over seeded state, and the
 * source-incremented commit counters (committed / conflict /
 * validation), all asserted on the scrape text.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ObservabilityIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

    @BeforeEach
    fun bindRegistry() {
        // Another test class may have built an App since ours; the
        // facade is process-global, so re-point it at our registry.
        Metrics.bind(app.meterRegistry)
    }

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    /** The value of a series in the scrape text, matched on name + all given label pairs. */
    private fun seriesValue(
        scrape: String,
        name: String,
        vararg labels: Pair<String, String>,
    ): Double? =
        scrape.lineSequence()
            .filter { it.startsWith(name) && !it.startsWith("#") }
            .filter { line -> labels.all { (k, v) -> line.contains("$k=\"$v\"") } }
            .map { it.substringAfterLast(' ').toDouble() }
            .firstOrNull()

    /** catalog + namespace + table; returns the table_uuid. */
    private suspend fun seed(
        client: HttpClient,
        catalog: String,
    ): String {
        assertThat(
            client.postJson(
                "/v1/catalogs",
                """{"name": "$catalog", "data_path": "s3://hog/$catalog"}""",
            ).status,
        ).isEqualTo(HttpStatusCode.Created)
        assertThat(
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""").status,
        ).isEqualTo(HttpStatusCode.Created)
        val table =
            client.postJson(
                "/v1/catalogs/$catalog/namespaces/ns/tables",
                """
            {"name": "events", "columns": [
                {"name": "id", "type": "long", "nullable": false},
                {"name": "ts", "type": "timestamp"}
            ]}
            """,
            )
        assertThat(table.status).isEqualTo(HttpStatusCode.Created)
        return body(table)["table_uuid"].asText()
    }

    private suspend fun commitOneFile(
        client: HttpClient,
        catalog: String,
        path: String,
        readSnapshot: Long? = null,
    ): HttpResponse =
        client.postJson(
            "/v1/catalogs/$catalog/commit",
            """
        {
          ${if (readSnapshot != null) "\"read_snapshot\": $readSnapshot," else ""}
          "appends": [{"namespace": "ns", "table": "events",
                       "files": [{"path": "$path", "record_count": 10,
                                  "file_size_bytes": 1000}]}]
        }
        """,
        )

    @Test
    fun `metrics endpoint exposes catalog gauges after one sampler pass`() =
        api { client ->
            val catalog = "gauges"
            val tableUuid = seed(client, catalog) // snapshots 0..2
            assertThat(commitOneFile(client, catalog, "s3://hog/$catalog/f1.parquet").status)
                .isEqualTo(HttpStatusCode.OK) // snapshot 3, stats pending (no column_stats)
            // A consumer committed at snapshot 2 -> lag 1 against head 3.
            val offset =
                client.put("/v1/catalogs/$catalog/consumers/duckling/offsets/$tableUuid") {
                    contentType(ContentType.Application.Json)
                    setBody("""{"snapshot_id": 2}""")
                }
            assertThat(offset.status).isEqualTo(HttpStatusCode.OK)
            // One queued removal (the gauge reads the queue, however it filled)
            // plus one DRAINED ledger row, which must NOT count as depth.
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT catalog_id, 's3://hog/$catalog/old.parquet', 'data', 'snapshot_expiry'
                FROM hog_catalog WHERE name = :name
                """,
                ).bind("name", catalog).execute()
                h.createUpdate(
                    """
                INSERT INTO hog_file_removal
                    (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                SELECT catalog_id, 's3://hog/$catalog/done.parquet', 'data', 'snapshot_expiry',
                       now(), 'deleted'
                FROM hog_catalog WHERE name = :name
                """,
                ).bind("name", catalog).execute()
                // Flag the committed live file id-less -> the field-id gauge.
                h.createUpdate(
                    """
                UPDATE hog_data_file SET missing_field_ids = true
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :name)
                """,
                ).bind("name", catalog).execute()
                // One hydration-failed file (historical, so it stays out of
                // the live-scoped field-id gauge) -> the stats-failed gauge
                // (B1 mirrors the pending gauge).
                h.createUpdate(
                    """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           end_snapshot, path, record_count, file_size_bytes,
                                           row_id_start, stats_state)
                SELECT f.catalog_id, 999, f.table_id, 1, 2,
                       's3://hog/$catalog/failed.parquet', 0, 0, 0, 'failed'
                FROM hog_data_file f
                WHERE f.catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :name)
                """,
                ).bind("name", catalog).execute()
            }

            app.catalogMetrics.sampleOnce()

            val response = client.get("/metrics")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val scrape = response.bodyAsText()

            val cat = "catalog" to catalog
            assertThat(seriesValue(scrape, "hoglake_head_snapshot_id", cat)).isEqualTo(3.0)
            assertThat(seriesValue(scrape, "hoglake_earliest_snapshot_id", cat)).isEqualTo(0.0)
            assertThat(seriesValue(scrape, "hoglake_snapshot_count", cat)).isEqualTo(4.0)
            assertThat(seriesValue(scrape, "hoglake_head_age_seconds", cat))
                .isGreaterThanOrEqualTo(0.0).isLessThan(600.0)
            assertThat(seriesValue(scrape, "hoglake_removal_queue_depth", cat)).isEqualTo(1.0)
            assertThat(seriesValue(scrape, "hoglake_stats_pending_files", cat)).isEqualTo(1.0)
            assertThat(seriesValue(scrape, "hoglake_stats_failed_files", cat)).isEqualTo(1.0)
            assertThat(seriesValue(scrape, "hoglake_missing_field_id_files", cat)).isEqualTo(1.0)
            assertThat(seriesValue(scrape, "hoglake_table_count", cat)).isEqualTo(1.0)
            assertThat(
                seriesValue(scrape, "hoglake_consumer_lag_snapshots", cat, "consumer" to "duckling"),
            ).isEqualTo(1.0)
            // lag_max is ALWAYS emitted alongside the per-consumer series
            // (under the cardinality cap too) so the family never flaps.
            assertThat(seriesValue(scrape, "hoglake_consumer_lag_max", cat)).isEqualTo(1.0)
            // Sampler staleness gauge: a successful sample stamps now().
            assertThat(seriesValue(scrape, "hoglake_metrics_last_sample_epoch"))
                .isGreaterThan((System.currentTimeMillis() / 1000 - 600).toDouble())

            // Ktor http server metrics come free; /metrics itself is filtered out.
            val second = client.get("/metrics").bodyAsText()
            assertThat(second).contains("ktor_http_server_requests")
            assertThat(second).doesNotContain("route=\"/metrics\"")
        }

    @Test
    fun `commit counters count committed, conflict, and validation at the source`() =
        api { client ->
            val catalog = "counters"
            seed(client, catalog) // snapshots 0..2
            assertThat(commitOneFile(client, catalog, "s3://hog/$catalog/a.parquet").status)
                .isEqualTo(HttpStatusCode.OK) // snapshot 3
            // DDL after snapshot 3, then a commit reading at 3 -> 409 conflict.
            val alter =
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables/events/alter",
                    """{"ops": [{"op": "add_column", "column": {"name": "extra", "type": "int"}}]}""",
                )
            assertThat(alter.status).isEqualTo(HttpStatusCode.OK) // snapshot 4
            val conflicted =
                commitOneFile(client, catalog, "s3://hog/$catalog/b.parquet", readSnapshot = 3)
            assertThat(conflicted.status).isEqualTo(HttpStatusCode.Conflict)
            // Empty commit -> 422 validation.
            val invalid = client.postJson("/v1/catalogs/$catalog/commit", "{}")
            assertThat(invalid.status).isEqualTo(HttpStatusCode.UnprocessableEntity)

            val scrape = client.get("/metrics").bodyAsText()
            val cat = "catalog" to catalog
            assertThat(seriesValue(scrape, "hoglake_commits_total", cat, "result" to "committed"))
                .isEqualTo(1.0)
            assertThat(seriesValue(scrape, "hoglake_commits_total", cat, "result" to "conflict"))
                .isEqualTo(1.0)
            assertThat(seriesValue(scrape, "hoglake_commits_total", cat, "result" to "validation"))
                .isEqualTo(1.0)
        }

    @Test
    fun `an unexpected mid-commit failure ticks result=error and returns 500`() =
        api { client ->
            val catalog = "errcount"
            seed(client, catalog)
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "DELETE FROM hog_table_stats WHERE catalog_id = " +
                        "(SELECT catalog_id FROM hog_catalog WHERE name = ?)",
                    catalog,
                )
            }
            val boom = commitOneFile(client, catalog, "s3://hog/$catalog/x.parquet")
            assertThat(boom.status).isEqualTo(HttpStatusCode.InternalServerError)
            val scrape = client.get("/metrics").bodyAsText()
            assertThat(
                seriesValue(scrape, "hoglake_commits_total", "catalog" to catalog, "result" to "error"),
            ).isEqualTo(1.0)
        }

    @Test
    fun `a consumer offset with a defunct table uuid cannot distort the lag series`() =
        api { client ->
            val catalog = "lagjoin"
            val tableUuid = seed(client, catalog) // head = 2
            client.put("/v1/catalogs/$catalog/consumers/real/offsets/$tableUuid") {
                contentType(ContentType.Application.Json)
                setBody("""{"snapshot_id": 2}""")
            }
            // A leftover offset row whose table identity no longer exists
            // (e.g. the hog_table row expired away) — inserted directly,
            // since the PUT validates uuids now. The hog_table join must
            // keep it out of the lag series entirely.
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                SELECT catalog_id, 'ghost', '00000000-0000-4000-8000-00000000dead', 0
                FROM hog_catalog WHERE name = ?
                """,
                    catalog,
                )
            }
            app.catalogMetrics.sampleOnce()
            val scrape = client.get("/metrics").bodyAsText()
            val cat = "catalog" to catalog
            assertThat(seriesValue(scrape, "hoglake_consumer_lag_snapshots", cat, "consumer" to "real"))
                .isEqualTo(0.0)
            assertThat(seriesValue(scrape, "hoglake_consumer_lag_snapshots", cat, "consumer" to "ghost"))
                .describedAs("defunct-uuid offset excluded from lag")
                .isNull()
            // lag_max reflects only join-surviving offsets.
            assertThat(seriesValue(scrape, "hoglake_consumer_lag_max", cat)).isEqualTo(0.0)
        }

    @Test
    fun `sampler failures increment the error counter`() {
        val registry =
            io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT,
            )
        val deadDb = PgTestSupport.freshDatabase()
        val metrics = CatalogMetrics(deadDb.jdbi, registry)
        deadDb.close() // pool gone: the next sample must fail
        org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
            metrics.sampleOnce()
        }
        assertThat(registry.get("hoglake_metrics_sample_errors_total").counter().count())
            .isEqualTo(1.0)
        // Never-sampled staleness gauge stays at 0.
        assertThat(registry.get("hoglake_metrics_last_sample_epoch").gauge().value())
            .isEqualTo(0.0)
    }
}
