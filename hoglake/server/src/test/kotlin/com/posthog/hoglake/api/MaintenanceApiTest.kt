package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.patch
import io.ktor.client.request.post
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
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Wire-level tests for the lifecycle surface: GET/PATCH /options (the
 * absent-vs-null tri-state over raw JSON) and the manual maintenance
 * triggers — snake_case bodies, spec status codes, installed via
 * [installMaintenanceRoutes] exactly as App.kt will wire it.
 *
 * No object store is needed: the cleanup endpoint is exercised against
 * an empty queue (its S3 client is constructed but never called), and
 * expiry is metadata-only by design.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaintenanceApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    /** Never contacted: every request in this class drains an empty queue. */
    private val removals =
        RemovalStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    @AfterAll
    fun tearDown() {
        removals.close()
        db.close()
    }

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                app.module(this)
                installMaintenanceRoutes(
                    OptionsService(db.jdbi),
                    ExpiryService(db.jdbi),
                    CleanupService(db.jdbi, removals),
                )
            }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String = "",
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun HttpClient.patchJson(
        url: String,
        body: String,
    ): HttpResponse =
        patch(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    private suspend fun assertApiError(
        response: HttpResponse,
        status: HttpStatusCode,
        code: String,
    ) {
        assertThat(response.status).isEqualTo(status)
        assertThat(body(response)["error"].asText()).isEqualTo(code)
    }

    private suspend fun HttpClient.createCatalog(name: String) {
        val r = postJson("/v1/catalogs", """{"name": "$name", "data_path": "s3://b/$name"}""")
        assertThat(r.status).isEqualTo(HttpStatusCode.Created)
    }

    // ---- options -----------------------------------------------------------

    @Test
    fun `options defaults and patch tri-state semantics`() =
        api { client ->
            client.createCatalog("mnt-opts")

            // Defaults: no retention (omitted under NON_NULL), floor on, earliest 0.
            val defaults =
                body(
                    client.get("/v1/catalogs/mnt-opts/options").also {
                        assertThat(it.status).isEqualTo(HttpStatusCode.OK)
                    },
                )
            assertThat(defaults.path("snapshot_retention_seconds").isMissingNode).isTrue()
            assertThat(defaults["consumer_floor"].asBoolean()).isTrue()
            assertThat(defaults["earliest_snapshot_id"].asLong()).isEqualTo(0)

            // Set a value; the absent consumer_floor is untouched.
            val set =
                body(
                    client.patchJson(
                        "/v1/catalogs/mnt-opts/options",
                        """{"snapshot_retention_seconds": 3600}""",
                    ).also { assertThat(it.status).isEqualTo(HttpStatusCode.OK) },
                )
            assertThat(set["snapshot_retention_seconds"].asLong()).isEqualTo(3600)
            assertThat(set["consumer_floor"].asBoolean()).isTrue()

            // Absent retention is unchanged while floor flips.
            val flip =
                body(
                    client.patchJson("/v1/catalogs/mnt-opts/options", """{"consumer_floor": false}"""),
                )
            assertThat(flip["snapshot_retention_seconds"].asLong()).isEqualTo(3600)
            assertThat(flip["consumer_floor"].asBoolean()).isFalse()

            // Explicit null disables expiry.
            val disabled =
                body(
                    client.patchJson(
                        "/v1/catalogs/mnt-opts/options",
                        """{"snapshot_retention_seconds": null}""",
                    ).also { assertThat(it.status).isEqualTo(HttpStatusCode.OK) },
                )
            assertThat(disabled.path("snapshot_retention_seconds").isMissingNode).isTrue()
            assertThat(disabled["consumer_floor"].asBoolean()).isFalse()
        }

    @Test
    fun `options rejects bad values with the spec status codes`() =
        api { client ->
            client.createCatalog("mnt-bad")

            // Value error -> 422.
            assertApiError(
                client.patchJson("/v1/catalogs/mnt-bad/options", """{"snapshot_retention_seconds": 0}"""),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            // Shape errors -> 400.
            assertApiError(
                client.patchJson("/v1/catalogs/mnt-bad/options", """{"snapshot_retention_seconds": "soon"}"""),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.patchJson("/v1/catalogs/mnt-bad/options", """{"consumer_floor": null}"""),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.patchJson("/v1/catalogs/mnt-bad/options", """[1, 2]"""),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            // Unknown catalog -> 404 on both verbs.
            assertApiError(
                client.get("/v1/catalogs/mnt-nope/options"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertApiError(
                client.patchJson("/v1/catalogs/mnt-nope/options", """{"consumer_floor": true}"""),
                HttpStatusCode.NotFound,
                "not_found",
            )
        }

    // ---- maintenance/expire ------------------------------------------------

    @Test
    fun `expire endpoint runs a real sweep`() =
        api { client ->
            client.createCatalog("mnt-expire") // S0
            // Two namespaces -> S1, S2 (head).
            for (ns in listOf("a", "b")) {
                val r = client.postJson("/v1/catalogs/mnt-expire/namespaces", """{"name": "$ns"}""")
                assertThat(r.status).isEqualTo(HttpStatusCode.Created)
            }
            // Age everything an hour, then retain 60s.
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    """
                UPDATE hog_snapshot SET snapshot_time = now() - make_interval(secs => 3600)
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = 'mnt-expire')
                """,
                )
            }
            client.patchJson("/v1/catalogs/mnt-expire/options", """{"snapshot_retention_seconds": 60}""")

            val result =
                body(
                    client.postJson("/v1/catalogs/mnt-expire/maintenance/expire").also {
                        assertThat(it.status).isEqualTo(HttpStatusCode.OK)
                    },
                )
            assertThat(result["snapshots_expired"].asLong()).isEqualTo(2)
            assertThat(result["data_files_queued"].asLong()).isEqualTo(0)
            assertThat(result["delete_files_queued"].asLong()).isEqualTo(0)
            assertThat(result["new_earliest_snapshot_id"].asLong()).isEqualTo(2) // head survives
            assertThat(result.path("floored_by_consumer").isMissingNode).isTrue()

            assertThat(
                body(client.get("/v1/catalogs/mnt-expire/options"))["earliest_snapshot_id"].asLong(),
            ).isEqualTo(2)
        }

    @Test
    fun `expire respects the batch override and validates it`() =
        api { client ->
            client.createCatalog("mnt-expire-batch") // S0
            for (ns in listOf("a", "b", "c")) {
                client.postJson("/v1/catalogs/mnt-expire-batch/namespaces", """{"name": "$ns"}""")
            }
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    """
                UPDATE hog_snapshot SET snapshot_time = now() - make_interval(secs => 3600)
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = 'mnt-expire-batch')
                """,
                )
            }
            client.patchJson(
                "/v1/catalogs/mnt-expire-batch/options",
                """{"snapshot_retention_seconds": 60}""",
            )

            val first = body(client.postJson("/v1/catalogs/mnt-expire-batch/maintenance/expire?batch=1"))
            assertThat(first["snapshots_expired"].asLong()).isEqualTo(1)
            assertThat(first["new_earliest_snapshot_id"].asLong()).isEqualTo(1)

            assertApiError(
                client.postJson("/v1/catalogs/mnt-expire-batch/maintenance/expire?batch=0"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            assertApiError(
                client.postJson("/v1/catalogs/mnt-expire-batch/maintenance/expire?batch=lots"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.postJson("/v1/catalogs/mnt-nope/maintenance/expire"),
                HttpStatusCode.NotFound,
                "not_found",
            )
        }

    // ---- maintenance/cleanup -----------------------------------------------

    @Test
    fun `cleanup endpoint drains and validates like the spec says`() =
        api { client ->
            client.createCatalog("mnt-clean")

            val result =
                body(
                    client.postJson("/v1/catalogs/mnt-clean/maintenance/cleanup").also {
                        assertThat(it.status).isEqualTo(HttpStatusCode.OK)
                    },
                )
            assertThat(result["removed"].asLong()).isEqualTo(0)
            assertThat(result["missing"].asLong()).isEqualTo(0)
            assertThat(result["still_referenced"].asLong()).isEqualTo(0)

            assertApiError(
                client.postJson("/v1/catalogs/mnt-clean/maintenance/cleanup?batch=-1"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            assertApiError(
                client.postJson("/v1/catalogs/mnt-nope/maintenance/cleanup"),
                HttpStatusCode.NotFound,
                "not_found",
            )
        }
}
