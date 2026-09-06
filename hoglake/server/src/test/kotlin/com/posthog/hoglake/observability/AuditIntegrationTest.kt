package com.posthog.hoglake.observability

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The audit log end to end: events on the dedicated "hoglake.audit"
 * logger are captured through the real LogstashEncoder (so the
 * assertion is on actual JSON lines, exactly what stdout would carry)
 * and checked for the structured fields — plus the transaction-boundary
 * rule: a failed commit emits its failure outcome and never a
 * 'committed' line.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuditIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    /** Encodes each event to a JSON line at append time (MDC still live). */
    private class CapturingAppender(private val encoder: LogstashEncoder) :
        AppenderBase<ILoggingEvent>() {
        val lines = CopyOnWriteArrayList<String>()

        override fun append(event: ILoggingEvent) {
            lines += String(encoder.encode(event))
        }
    }

    private lateinit var capture: CapturingAppender

    @BeforeEach
    fun attachAppender() {
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        val encoder =
            LogstashEncoder().apply {
                context = ctx
                // Mirror logback.xml: request_id comes from the structured
                // argument, not MDC (which would duplicate the JSON key).
                setIncludeMdc(false)
                start()
            }
        capture =
            CapturingAppender(encoder).apply {
                context = ctx
                start()
            }
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).addAppender(capture)
    }

    @AfterEach
    fun detachAppender() {
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).detachAppender(capture)
        capture.stop()
    }

    private fun events(): List<JsonNode> = capture.lines.map { json.readTree(it) }

    private fun eventsFor(action: String): List<JsonNode> = events().filter { it["action"]?.asText() == action }

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
        requestId: String? = null,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            requestId?.let { header("X-Request-Id", it) }
            setBody(body)
        }

    private suspend fun seed(
        client: HttpClient,
        catalog: String,
    ) {
        assertThat(
            client.postJson(
                "/v1/catalogs",
                """{"name": "$catalog", "data_path": "s3://hog/$catalog"}""",
            ).status,
        ).isEqualTo(HttpStatusCode.Created)
        assertThat(
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""").status,
        ).isEqualTo(HttpStatusCode.Created)
    }

    @Test
    fun `create-table, commit, conflict, and expiry all emit structured audit lines`() =
        api { client ->
            val catalog = "audited"
            seed(client, catalog)

            // -- create table (with a caller-supplied request id) --------------
            val create =
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables",
                    """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
                    requestId = "req-42",
                )
            assertThat(create.status).isEqualTo(HttpStatusCode.Created)
            val tableCreate = eventsFor("table_create").single { it["catalog"].asText() == catalog }
            assertThat(tableCreate["outcome"].asText()).isEqualTo("ok")
            assertThat(tableCreate["object"].asText()).isEqualTo("ns.events")
            assertThat(tableCreate["actor"].asText()).isEqualTo("anonymous")
            assertThat(tableCreate["request_id"].asText()).isEqualTo("req-42")

            // -- successful commit --------------------------------------------
            val commit =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
            {"appends": [{"namespace": "ns", "table": "events",
                          "files": [{"path": "s3://hog/$catalog/a.parquet",
                                     "record_count": 5, "file_size_bytes": 100}]}]}
            """,
                )
            assertThat(commit.status).isEqualTo(HttpStatusCode.OK)
            val committed =
                eventsFor("commit").single {
                    it["catalog"].asText() == catalog && it["outcome"].asText() == "committed"
                }
            assertThat(committed["detail"].asText()).contains("snapshot=3", "files=1", "deletes=0")
            // Generated request id (no header sent) still lands on the line.
            assertThat(committed["request_id"].asText()).isNotBlank()

            // -- conflicting commit (DDL after its read snapshot) --------------
            val alter =
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables/events/alter",
                    """{"ops": [{"op": "add_column", "column": {"name": "extra", "type": "int"}}]}""",
                )
            assertThat(alter.status).isEqualTo(HttpStatusCode.OK)
            val conflicted =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
            {"read_snapshot": 3,
             "appends": [{"namespace": "ns", "table": "events",
                          "files": [{"path": "s3://hog/$catalog/b.parquet",
                                     "record_count": 5, "file_size_bytes": 100}]}]}
            """,
                )
            assertThat(conflicted.status).isEqualTo(HttpStatusCode.Conflict)
            val conflictLine =
                eventsFor("commit").single {
                    it["catalog"].asText() == catalog && it["outcome"].asText() == "conflict"
                }
            assertThat(conflictLine["detail"].asText()).contains("concurrent DDL")

            // -- expiry run ----------------------------------------------------
            val expire = client.post("/v1/catalogs/$catalog/maintenance/expire")
            assertThat(expire.status).isEqualTo(HttpStatusCode.OK)
            val expiry = eventsFor("expiry").single { it["catalog"].asText() == catalog }
            assertThat(expiry["outcome"].asText()).isEqualTo("ok")
            assertThat(expiry["detail"].asText()).contains("snapshots_expired=0")

            // Every captured line already proved JSON-parseable via events();
            // spot-check the audit lines all carry the core fields.
            events().forEach { line ->
                assertThat(line["action"].asText()).isNotBlank()
                assertThat(line["outcome"].asText()).isNotBlank()
                assertThat(line["actor"].asText()).isEqualTo("anonymous")
            }
        }

    @Test
    fun `a validation-failed commit emits outcome=validation and never a committed line`() =
        api { client ->
            val catalog = "rollback"
            seed(client, catalog)
            assertThat(
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables",
                    """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
                ).status,
            ).isEqualTo(HttpStatusCode.Created)

            val headBefore =
                json.readTree(
                    client.get("/v1/catalogs/$catalog").bodyAsText(),
                )["head_snapshot_id"].asLong()

            // Commit that fails validation INSIDE the transaction (unknown
            // table): the rollback must leave exactly one audit line, the
            // validation outcome — no 'committed' line ever appears because
            // the event is only emitted after the transaction concludes.
            val invalid =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
                {"appends": [{"namespace": "ns", "table": "nope",
                              "files": [{"path": "s3://x", "record_count": 1,
                                         "file_size_bytes": 1}]}]}
                """,
                )
            assertThat(invalid.status).isEqualTo(HttpStatusCode.UnprocessableEntity)

            val commitLines = eventsFor("commit").filter { it["catalog"].asText() == catalog }
            assertThat(commitLines).hasSize(1)
            assertThat(commitLines.single()["outcome"].asText()).isEqualTo("validation")
            assertThat(commitLines.single()["detail"].asText()).contains("unknown table")

            // And the catalog head never moved: nothing was committed for
            // the line to describe.
            val headAfter =
                json.readTree(
                    client.get("/v1/catalogs/$catalog").bodyAsText(),
                )["head_snapshot_id"].asLong()
            assertThat(headAfter).isEqualTo(headBefore)
        }
}
