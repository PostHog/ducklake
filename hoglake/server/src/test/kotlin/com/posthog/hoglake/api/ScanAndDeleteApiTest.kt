package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
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
 * Wire-level tests for the milestone-2 surface: commits carrying deletes
 * and partition_values, and the /scan planning endpoint — raw JSON,
 * snake_case names, spec status codes. The scan route is installed via
 * [installScanRoutes], the same installer App.kt wires in production.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScanAndDeleteApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                app.module(this)
                installScanRoutes(ScanService(db.jdbi))
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
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo(code)
        return node
    }

    /** Give the named table a live 2-field partition spec via direct SQL (no AlterService). */
    private fun seedSpec(
        catalogName: String,
        tableName: String,
    ) {
        db.jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                    .bind(0, catalogName).mapTo(Long::class.java).one()
            val tableId =
                h.createQuery(
                    """
                SELECT table_id FROM hog_table_version
                 WHERE catalog_id = ? AND name = ? AND end_snapshot IS NULL
                """,
                ).bind(0, catalogId).bind(1, tableName).mapTo(Long::class.java).one()
            val fieldIds =
                h.createQuery(
                    """
                SELECT field_id FROM hog_column
                 WHERE catalog_id = ? AND table_id = ? AND end_snapshot IS NULL
                 ORDER BY ordinal
                """,
                ).bind(0, catalogId).bind(1, tableId).mapTo(Long::class.java).list()
            h.createUpdate(
                """
                INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot)
                VALUES (?, ?, 1, 0)
                """,
            ).bind(0, catalogId).bind(1, tableId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index,
                                                 source_field_id, transform)
                VALUES (:c, :t, 1, 0, :f0, 'day'), (:c, :t, 1, 1, :f1, 'identity')
                """,
            ).bind("c", catalogId).bind("t", tableId)
                .bind("f0", fieldIds[0]).bind("f1", fieldIds[1]).execute()
        }
    }

    @Test
    fun `commit with partition values and deletes then scan`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "sc", "data_path": "s3://hog/sc"}""")
            client.postJson("/v1/catalogs/sc/namespaces", """{"name": "ns"}""")
            client.postJson(
                "/v1/catalogs/sc/namespaces/ns/tables",
                """{"name": "events", "columns": [
                 {"name": "day", "type": "string"}, {"name": "uid", "type": "string"}]}""",
            ) // snapshot 2
            seedSpec("sc", "events")

            // Partitioned append (snapshot 3): partition_values on the wire,
            // including an explicit null element.
            val commit =
                client.postJson(
                    "/v1/catalogs/sc/commit",
                    """
            {"read_snapshot": 2, "appends": [{"namespace": "ns", "table": "events", "files": [
               {"path": "s3://hog/sc/f1.parquet", "record_count": 10, "file_size_bytes": 1000,
                "partition_values": ["2026-01-01", "x"]},
               {"path": "s3://hog/sc/f2.parquet", "record_count": 5, "file_size_bytes": 500,
                "partition_values": ["2026-01-02", null]}]}]}
            """,
                )
            assertThat(commit.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(commit)["snapshot_id"].asLong()).isEqualTo(3)

            // Delete commit (snapshot 4): a DV on data file 1.
            val deleteCommit =
                client.postJson(
                    "/v1/catalogs/sc/commit",
                    """
            {"read_snapshot": 3, "deletes": [{"namespace": "ns", "table": "events", "files": [
               {"data_file_id": 1, "path": "s3://hog/sc/dv1.puffin",
                "delete_count": 4, "file_size_bytes": 64}]}]}
            """,
                )
            assertThat(deleteCommit.status).isEqualTo(HttpStatusCode.OK)
            body(deleteCommit).let { r ->
                assertThat(r["snapshot_id"].asLong()).isEqualTo(4)
                assertThat(r["schema_version"].asLong()).isEqualTo(2)
            }

            // Scan at head: [f1 + DV, f2 without DV], ordered by row_id_start.
            val scan = body(client.get("/v1/catalogs/sc/namespaces/ns/tables/events/scan"))
            assertThat(scan).hasSize(2)
            scan[0].let { sf ->
                val df = sf["data_file"]
                assertThat(df["data_file_id"].asLong()).isEqualTo(1)
                assertThat(df["path"].asText()).isEqualTo("s3://hog/sc/f1.parquet")
                assertThat(df["file_format"].asText()).isEqualTo("parquet")
                assertThat(df["record_count"].asLong()).isEqualTo(10)
                assertThat(df["file_size_bytes"].asLong()).isEqualTo(1000)
                assertThat(df["row_id_start"].asLong()).isEqualTo(0)
                assertThat(df["stats_state"].asText()).isEqualTo("pending")
                assertThat(df["begin_snapshot"].asLong()).isEqualTo(3)
                assertThat(df["spec_id"].asLong()).isEqualTo(1)
                assertThat(df["partition_values"].map { if (it.isNull) null else it.asText() })
                    .containsExactly("2026-01-01", "x")

                val dv = sf["delete_file"]
                assertThat(dv["delete_file_id"].asLong()).isEqualTo(3)
                assertThat(dv["data_file_id"].asLong()).isEqualTo(1)
                assertThat(dv["path"].asText()).isEqualTo("s3://hog/sc/dv1.puffin")
                assertThat(dv["file_format"].asText()).isEqualTo("puffin-dv")
                assertThat(dv["delete_count"].asLong()).isEqualTo(4)
                assertThat(dv["file_size_bytes"].asLong()).isEqualTo(64)
                assertThat(dv["begin_snapshot"].asLong()).isEqualTo(4)
            }
            scan[1].let { sf ->
                assertThat(sf["data_file"]["row_id_start"].asLong()).isEqualTo(10)
                assertThat(sf["data_file"]["partition_values"][1].isNull).isTrue()
                // NON_NULL inclusion: no delete_file key at all.
                assertThat(sf.has("delete_file")).isFalse()
            }

            // Time travel: before the delete, f1 has no DV.
            val at3 = body(client.get("/v1/catalogs/sc/namespaces/ns/tables/events/scan?snapshot=3"))
            assertThat(at3).hasSize(2)
            assertThat(at3[0].has("delete_file")).isFalse()

            // A second, stale delete (read_snapshot older than the live DV) -> 409.
            assertApiError(
                client.postJson(
                    "/v1/catalogs/sc/commit",
                    """
                {"read_snapshot": 3, "deletes": [{"namespace": "ns", "table": "events", "files": [
                   {"data_file_id": 1, "path": "s3://hog/sc/dv2.puffin",
                    "delete_count": 6, "file_size_bytes": 64}]}]}
                """,
                ),
                HttpStatusCode.Conflict,
                "commit_conflict",
            )

            // Deletes without read_snapshot -> 422.
            assertApiError(
                client.postJson(
                    "/v1/catalogs/sc/commit",
                    """
                {"deletes": [{"namespace": "ns", "table": "events", "files": [
                   {"data_file_id": 1, "path": "s3://hog/sc/dv3.puffin",
                    "delete_count": 6, "file_size_bytes": 64}]}]}
                """,
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )

            // Partition arity mismatch over the wire -> 422.
            assertApiError(
                client.postJson(
                    "/v1/catalogs/sc/commit",
                    """
                {"appends": [{"namespace": "ns", "table": "events", "files": [
                   {"path": "s3://hog/sc/f3.parquet", "record_count": 1, "file_size_bytes": 1,
                    "partition_values": ["just-one"]}]}]}
                """,
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }

    @Test
    fun `scan resolution errors on the wire`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "se", "data_path": "s3://hog/se"}""")
            assertApiError(
                client.get("/v1/catalogs/nope/namespaces/ns/tables/t/scan"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertApiError(
                client.get("/v1/catalogs/se/namespaces/ns/tables/t/scan"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertApiError(
                client.get("/v1/catalogs/se/namespaces/ns/tables/t/scan?snapshot=abc"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }
}
