package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.delete
import io.ktor.client.request.get
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
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Wire-level tests for the read-parity surface: the changefeed's
 * delete_files array, the view routes (installed via
 * [installViewRoutes], the same installer App.kt should wire), the 410
 * expired contract, and at_timestamp parameter handling — raw JSON,
 * snake_case names, spec status codes.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadParityApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                app.module(this)
                installViewRoutes(ViewService(db.jdbi))
            }
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

    private suspend fun assertApiError(
        response: HttpResponse,
        status: HttpStatusCode,
        code: String,
    ): JsonNode {
        assertThat(response.status).isEqualTo(status)
        assertThat(response.contentType()?.contentSubtype).isEqualTo("json")
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo(code)
        return node
    }

    /** catalog + namespace + one-column table, all over the wire. */
    private suspend fun HttpClient.seed(catalog: String) {
        postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://hog/$catalog"}""")
        postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
        postJson(
            "/v1/catalogs/$catalog/namespaces/ns/tables",
            """{"name": "t", "columns": [{"name": "id", "type": "long"}]}""",
        ) // snapshot 2
    }

    @Test
    fun `changes carries the delete_files feed`() =
        api { client ->
            client.seed("cf")
            // Append at snapshot 3, DV on the appended file at snapshot 4.
            client.postJson(
                "/v1/catalogs/cf/commit",
                """
            {"appends": [{"namespace": "ns", "table": "t", "files": [
               {"path": "s3://hog/cf/f1.parquet", "record_count": 10, "file_size_bytes": 100}]}]}
            """,
            )
            client.postJson(
                "/v1/catalogs/cf/commit",
                """
            {"read_snapshot": 3, "deletes": [{"namespace": "ns", "table": "t", "files": [
               {"data_file_id": 1, "path": "s3://hog/cf/dv1.puffin",
                "delete_count": 2, "file_size_bytes": 64}]}]}
            """,
            )

            val plan = body(client.get("/v1/catalogs/cf/namespaces/ns/tables/t/changes?from_snapshot=0"))
            assertThat(plan["from_snapshot"].asLong()).isEqualTo(0)
            assertThat(plan["to_snapshot"].asLong()).isEqualTo(4)
            assertThat(plan["files"]).hasSize(1)
            assertThat(plan["delete_files"]).hasSize(1)
            plan["delete_files"][0].let { dv ->
                assertThat(dv["delete_file_id"].asLong()).isEqualTo(2)
                assertThat(dv["data_file_id"].asLong()).isEqualTo(1)
                assertThat(dv["path"].asText()).isEqualTo("s3://hog/cf/dv1.puffin")
                assertThat(dv["file_format"].asText()).isEqualTo("puffin-dv")
                assertThat(dv["delete_count"].asLong()).isEqualTo(2)
                assertThat(dv["file_size_bytes"].asLong()).isEqualTo(64)
                assertThat(dv["begin_snapshot"].asLong()).isEqualTo(4)
            }

            // from is exclusive for both lists: (3, 4] has the DV but not the append.
            val tail =
                body(
                    client.get("/v1/catalogs/cf/namespaces/ns/tables/t/changes?from_snapshot=3"),
                )
            assertThat(tail["files"]).isEmpty()
            assertThat(tail["delete_files"]).hasSize(1)
        }

    @Test
    fun `views roundtrip on the wire`() =
        api { client ->
            client.seed("vw")
            val created =
                client.postJson(
                    "/v1/catalogs/vw/namespaces/ns/views",
                    """{"name": "top", "sql": "SELECT id FROM t LIMIT 5"}""",
                )
            assertThat(created.status).isEqualTo(HttpStatusCode.Created)
            body(created).let { v ->
                assertThat(v["name"].asText()).isEqualTo("top")
                assertThat(v["namespace"].asText()).isEqualTo("ns")
                assertThat(v["dialect"].asText()).isEqualTo("trino")
                assertThat(v["sql"].asText()).isEqualTo("SELECT id FROM t LIMIT 5")
                assertThat(v["view_uuid"].asText()).isNotBlank()
            }

            // Duplicate -> 409.
            assertApiError(
                client.postJson(
                    "/v1/catalogs/vw/namespaces/ns/views",
                    """{"name": "top", "sql": "SELECT 1"}""",
                ),
                HttpStatusCode.Conflict,
                "already_exists",
            )
            // Blank sql -> 422.
            assertApiError(
                client.postJson(
                    "/v1/catalogs/vw/namespaces/ns/views",
                    """{"name": "empty", "sql": " "}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )

            assertThat(body(client.get("/v1/catalogs/vw/namespaces/ns/views/top"))["sql"].asText())
                .isEqualTo("SELECT id FROM t LIMIT 5")
            assertThat(body(client.get("/v1/catalogs/vw/namespaces/ns/views")).map { it["name"].asText() })
                .containsExactly("top")

            // DELETE returns the recording snapshot; the view is then 404.
            val dropped = client.delete("/v1/catalogs/vw/namespaces/ns/views/top")
            assertThat(dropped.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(dropped)["snapshot_id"].asLong()).isEqualTo(4)
            assertApiError(
                client.get("/v1/catalogs/vw/namespaces/ns/views/top"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertThat(body(client.get("/v1/catalogs/vw/namespaces/ns/views"))).isEmpty()
        }

    @Test
    fun `expired ranges and snapshots are 410 with the expired error body`() =
        api { client ->
            client.seed("ex")
            // Raise the floor to 2 via direct SQL (the expiry service owns this in production).
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_catalog SET earliest_snapshot_id = 2 WHERE name = 'ex'",
                ).execute()
            }

            val gone =
                assertApiError(
                    client.get("/v1/catalogs/ex/namespaces/ns/tables/t/changes?from_snapshot=0"),
                    HttpStatusCode.Gone,
                    "expired",
                )
            assertThat(gone["detail"].asText()).contains("earliest retained snapshot is 2")

            assertApiError(
                client.get("/v1/catalogs/ex/namespaces/ns/tables/t?snapshot=1"),
                HttpStatusCode.Gone,
                "expired",
            )
            assertApiError(
                client.get("/v1/catalogs/ex/namespaces/ns/tables/t/files?snapshot=0"),
                HttpStatusCode.Gone,
                "expired",
            )
            assertApiError(
                client.get("/v1/catalogs/ex/namespaces/ns/tables/t/scan?snapshot=1"),
                HttpStatusCode.Gone,
                "expired",
            )
            // Head reads and retained snapshots still serve.
            assertThat(client.get("/v1/catalogs/ex/namespaces/ns/tables/t").status)
                .isEqualTo(HttpStatusCode.OK)
            assertThat(client.get("/v1/catalogs/ex/namespaces/ns/tables/t?snapshot=2").status)
                .isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `at_timestamp parameter handling on the wire`() =
        api { client ->
            client.seed("ts")
            // A far-future timestamp resolves to head on all three GETs.
            val future = "2999-01-01T00:00:00Z"
            assertThat(
                client.get("/v1/catalogs/ts/namespaces/ns/tables/t?at_timestamp=$future").status,
            ).isEqualTo(HttpStatusCode.OK)
            assertThat(
                client.get("/v1/catalogs/ts/namespaces/ns/tables/t/files?at_timestamp=$future").status,
            ).isEqualTo(HttpStatusCode.OK)
            assertThat(
                client.get("/v1/catalogs/ts/namespaces/ns/tables/t/scan?at_timestamp=$future").status,
            ).isEqualTo(HttpStatusCode.OK)

            // Unparseable ISO instant -> 400.
            assertApiError(
                client.get("/v1/catalogs/ts/namespaces/ns/tables/t/files?at_timestamp=yesterday"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            // Both snapshot and at_timestamp -> 422.
            assertApiError(
                client.get(
                    "/v1/catalogs/ts/namespaces/ns/tables/t/files?snapshot=1&at_timestamp=$future",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            assertApiError(
                client.get(
                    "/v1/catalogs/ts/namespaces/ns/tables/t/scan?snapshot=1&at_timestamp=$future",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }
}
