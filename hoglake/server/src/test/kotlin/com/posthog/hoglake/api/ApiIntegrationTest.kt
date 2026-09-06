package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.delete
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
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Base64

/**
 * Wire-level tests of the /v1 REST API against the OpenAPI contract:
 * raw JSON in, raw JSON out, asserted on snake_case field names and
 * spec status codes — never round-tripped through the server's DTOs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

    // ---- harness ---------------------------------------------------------

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

    private suspend fun HttpClient.putJson(
        url: String,
        body: String,
    ): HttpResponse =
        put(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    /** Errors are always ApiError JSON — never Ktor's default page. */
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

    private val isoTimestamp = Regex("""^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*""")

    // ---- cross-cutting ---------------------------------------------------

    @Test
    fun `healthz and openapi yaml are served`() =
        api { client ->
            val health = client.get("/healthz")
            assertThat(health.status).isEqualTo(HttpStatusCode.OK)
            assertThat(health.bodyAsText()).isEqualTo("ok")

            val spec = client.get("/openapi.yaml")
            assertThat(spec.status).isEqualTo(HttpStatusCode.OK)
            assertThat(spec.contentType()?.contentSubtype).isEqualTo("yaml")
            assertThat(spec.bodyAsText()).startsWith("openapi:")
        }

    // ---- the full lifecycle ----------------------------------------------

    @Test
    fun `full lifecycle - catalog to commit to changes to offsets`() =
        api { client ->
            // Create catalog -> 201, snapshot 0, schema_version 0.
            val created =
                client.postJson(
                    "/v1/catalogs",
                    """{"name": "lake", "data_path": "s3://hog/lake"}""",
                )
            assertThat(created.status).isEqualTo(HttpStatusCode.Created)
            body(created).let { c ->
                assertThat(c["name"].asText()).isEqualTo("lake")
                assertThat(c["data_path"].asText()).isEqualTo("s3://hog/lake")
                assertThat(c["head_snapshot_id"].asLong()).isEqualTo(0)
                assertThat(c["schema_version"].asLong()).isEqualTo(0)
            }
            assertThat(body(client.get("/v1/catalogs")).map { it["name"].asText() })
                .contains("lake")
            assertThat(client.get("/v1/catalogs/lake").status).isEqualTo(HttpStatusCode.OK)

            // Namespace -> 201 (snapshot 1).
            val ns = client.postJson("/v1/catalogs/lake/namespaces", """{"name": "analytics"}""")
            assertThat(ns.status).isEqualTo(HttpStatusCode.Created)
            assertThat(body(ns)["name"].asText()).isEqualTo("analytics")
            assertThat(body(client.get("/v1/catalogs/lake/namespaces")).map { it["name"].asText() })
                .containsExactly("analytics")

            // Table -> 201 (snapshot 2) with server-assigned field_ids/ordinals.
            val table =
                client.postJson(
                    "/v1/catalogs/lake/namespaces/analytics/tables",
                    """
            {"name": "events", "columns": [
              {"name": "id", "type": "long", "nullable": false},
              {"name": "amount", "type": "decimal", "type_params": {"precision": 10, "scale": 2}},
              {"name": "uid", "type": "uuid"}
            ]}
            """,
                )
            assertThat(table.status).isEqualTo(HttpStatusCode.Created)
            val tableNode = body(table)
            val tableUuid = tableNode["table_uuid"].asText()
            val idFieldId = tableNode["columns"][0]["field_id"].asLong()
            assertThat(tableNode["name"].asText()).isEqualTo("events")
            assertThat(tableNode["namespace"].asText()).isEqualTo("analytics")
            assertThat(tableUuid).matches("[0-9a-f-]{36}")
            assertThat(tableNode["columns"]).hasSize(3)
            tableNode["columns"][0].let { col ->
                assertThat(col["name"].asText()).isEqualTo("id")
                assertThat(col["type"].asText()).isEqualTo("long")
                assertThat(col["nullable"].asBoolean()).isFalse()
                assertThat(col["ordinal"].asInt()).isEqualTo(0)
            }
            assertThat(tableNode["columns"][1]["type_params"]["scale"].asInt()).isEqualTo(2)
            assertThat(tableNode["columns"][2]["type"].asText()).isEqualTo("uuid")
            assertThat(tableNode["record_count"].asLong()).isEqualTo(0)

            // Table listing carries TableSummary (name + table_uuid).
            val listed = body(client.get("/v1/catalogs/lake/namespaces/analytics/tables"))
            assertThat(listed).hasSize(1)
            assertThat(listed[0]["name"].asText()).isEqualTo("events")
            assertThat(listed[0]["table_uuid"].asText()).isEqualTo(tableUuid)

            // Commit with provided stats -> 200 (snapshot 3; appends do not
            // bump schema_version). Bounds travel as base64 (format: byte).
            val lower = "AQAAAAAAAAA=" // little-endian long 1
            val upper = "ZAAAAAAAAAA=" // little-endian long 100
            val commit1 =
                client.postJson(
                    "/v1/catalogs/lake/commit",
                    """
            {"read_snapshot": 2,
             "author": "api-test", "message": "first append",
             "appends": [{"namespace": "analytics", "table": "events", "files": [
               {"path": "s3://hog/lake/events/f1.parquet", "record_count": 100,
                "file_size_bytes": 4096, "footer_size": 512,
                "column_stats": [
                  {"field_id": $idFieldId, "value_count": 100, "null_count": 0,
                   "size_bytes": 800, "lower_bound": "$lower", "upper_bound": "$upper"}
                ]}]}]}
            """,
                )
            assertThat(commit1.status).isEqualTo(HttpStatusCode.OK)
            body(commit1).let { r ->
                assertThat(r["snapshot_id"].asLong()).isEqualTo(3)
                assertThat(r["schema_version"].asLong()).isEqualTo(2)
            }

            // The base64 bounds decoded to the exact bytes in the catalog.
            val (storedLower, storedUpper) =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT lower_bound, upper_bound FROM hog_file_column_stats WHERE field_id = ?",
                    )
                        .bind(0, idFieldId)
                        .map { rs, _ -> rs.getBytes(1) to rs.getBytes(2) }
                        .one()
                }
            assertThat(storedLower).isEqualTo(Base64.getDecoder().decode(lower))
            assertThat(storedUpper).isEqualTo(Base64.getDecoder().decode(upper))

            // Deferred-stats commit -> 200 (snapshot 4), file lands pending.
            val commit2 =
                client.postJson(
                    "/v1/catalogs/lake/commit",
                    """
            {"appends": [{"namespace": "analytics", "table": "events", "files": [
               {"path": "s3://hog/lake/events/f2.parquet",
                "record_count": 50, "file_size_bytes": 2048}]}]}
            """,
                )
            assertThat(commit2.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(commit2)["snapshot_id"].asLong()).isEqualTo(4)

            // Files at head: row-id ranges and stats states on the wire.
            val files = body(client.get("/v1/catalogs/lake/namespaces/analytics/tables/events/files"))
            assertThat(files).hasSize(2)
            val byPath = files.associateBy { it["path"].asText() }
            byPath.getValue("s3://hog/lake/events/f1.parquet").let { f ->
                assertThat(f["data_file_id"].asLong()).isEqualTo(1)
                assertThat(f["file_format"].asText()).isEqualTo("parquet")
                assertThat(f["record_count"].asLong()).isEqualTo(100)
                assertThat(f["file_size_bytes"].asLong()).isEqualTo(4096)
                assertThat(f["footer_size"].asLong()).isEqualTo(512)
                assertThat(f["row_id_start"].asLong()).isEqualTo(0)
                assertThat(f["stats_state"].asText()).isEqualTo("provided")
                assertThat(f["begin_snapshot"].asLong()).isEqualTo(3)
            }
            byPath.getValue("s3://hog/lake/events/f2.parquet").let { f ->
                assertThat(f["row_id_start"].asLong()).isEqualTo(100)
                assertThat(f["stats_state"].asText()).isEqualTo("pending")
                assertThat(f["begin_snapshot"].asLong()).isEqualTo(4)
            }

            // Rolled-up table stats at head.
            body(client.get("/v1/catalogs/lake/namespaces/analytics/tables/events")).let { t ->
                assertThat(t["record_count"].asLong()).isEqualTo(150)
                assertThat(t["file_count"].asLong()).isEqualTo(2)
                assertThat(t["file_size_bytes"].asLong()).isEqualTo(4096 + 2048L)
            }

            // Changefeed plan over (from, to].
            val plan =
                body(
                    client.get(
                        "/v1/catalogs/lake/namespaces/analytics/tables/events/changes?from_snapshot=0",
                    ),
                )
            assertThat(plan["table_uuid"].asText()).isEqualTo(tableUuid)
            assertThat(plan["from_snapshot"].asLong()).isEqualTo(0)
            assertThat(plan["to_snapshot"].asLong()).isEqualTo(4)
            assertThat(plan["files"]).hasSize(2)
            val tail =
                body(
                    client.get(
                        "/v1/catalogs/lake/namespaces/analytics/tables/events/changes" +
                            "?from_snapshot=3&to_snapshot=4",
                    ),
                )
            assertThat(tail["files"]).hasSize(1)
            assertThat(tail["files"][0]["path"].asText()).isEqualTo("s3://hog/lake/events/f2.parquet")

            // Snapshot pagination: ids > after, limit-sized pages, has_more.
            val page1 = body(client.get("/v1/catalogs/lake/snapshots?after=0&limit=2"))
            assertThat(page1["snapshots"].map { it["snapshot_id"].asLong() }).containsExactly(1, 2)
            assertThat(page1["has_more"].asBoolean()).isTrue()
            assertThat(page1["snapshots"][0]["snapshot_time"].asText()).matches(isoTimestamp.pattern)
            assertThat(page1["snapshots"][1]["changes"].map { it["kind"].asText() })
                .containsExactly("table_created")

            val page2 = body(client.get("/v1/catalogs/lake/snapshots?after=2&limit=2"))
            assertThat(page2["snapshots"].map { it["snapshot_id"].asLong() }).containsExactly(3, 4)
            assertThat(page2["has_more"].asBoolean()).isFalse()
            page2["snapshots"][0].let { s ->
                assertThat(s["author"].asText()).isEqualTo("api-test")
                assertThat(s["message"].asText()).isEqualTo("first append")
                assertThat(s["changes"].map { it["kind"].asText() })
                    .containsExactly("table_inserted_into")
            }

            // Consumer offsets: PUT stores, regression is 409.
            val offset =
                client.putJson(
                    "/v1/catalogs/lake/consumers/reader-1/offsets/$tableUuid",
                    """{"snapshot_id": 4}""",
                )
            assertThat(offset.status).isEqualTo(HttpStatusCode.OK)
            body(offset).let { o ->
                assertThat(o["consumer_id"].asText()).isEqualTo("reader-1")
                assertThat(o["table_uuid"].asText()).isEqualTo(tableUuid)
                assertThat(o["committed_snapshot"].asLong()).isEqualTo(4)
                assertThat(o["updated_at"].asText()).matches(isoTimestamp.pattern)
            }
            val offsets = body(client.get("/v1/catalogs/lake/consumers/reader-1/offsets"))
            assertThat(offsets).hasSize(1)
            assertThat(offsets[0]["committed_snapshot"].asLong()).isEqualTo(4)

            val regressed =
                client.putJson(
                    "/v1/catalogs/lake/consumers/reader-1/offsets/$tableUuid",
                    """{"snapshot_id": 3}""",
                )
            assertApiError(regressed, HttpStatusCode.Conflict, "offset_regression")
        }

    // ---- DDL commits + time travel ---------------------------------------

    @Test
    fun `drop table returns commit result and time travel still sees it`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "tt", "data_path": "s3://hog/tt"}""")
            client.postJson("/v1/catalogs/tt/namespaces", """{"name": "ns"}""")
            client.postJson(
                "/v1/catalogs/tt/namespaces/ns/tables",
                """{"name": "t", "columns": [{"name": "id", "type": "long"}]}""",
            ) // snapshot 2

            val dropped = client.delete("/v1/catalogs/tt/namespaces/ns/tables/t")
            assertThat(dropped.status).isEqualTo(HttpStatusCode.OK)
            body(dropped).let { r ->
                assertThat(r["snapshot_id"].asLong()).isEqualTo(3)
                assertThat(r["schema_version"].asLong()).isEqualTo(3)
            }

            // Gone at head, still visible at the pre-drop snapshot.
            assertApiError(
                client.get("/v1/catalogs/tt/namespaces/ns/tables/t"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            val travelled = client.get("/v1/catalogs/tt/namespaces/ns/tables/t?snapshot=2")
            assertThat(travelled.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(travelled)["name"].asText()).isEqualTo("t")

            val filesAt2 = client.get("/v1/catalogs/tt/namespaces/ns/tables/t/files?snapshot=2")
            assertThat(filesAt2.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(filesAt2)).isEmpty()
            assertApiError(
                client.get("/v1/catalogs/tt/namespaces/ns/tables/t/files"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertThat(body(client.get("/v1/catalogs/tt/namespaces/ns/tables"))).isEmpty()
        }

    // ---- commit conflicts + validation -----------------------------------

    @Test
    fun `commit with stale read_snapshot conflicts with DDL - 409`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "cfl", "data_path": "s3://hog/cfl"}""")
            client.postJson("/v1/catalogs/cfl/namespaces", """{"name": "ns"}""")
            client.postJson(
                "/v1/catalogs/cfl/namespaces/ns/tables",
                """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
            ) // head = 2

            // Concurrent DDL after the writer's read snapshot: a table_dropped
            // change on the touched (still-live) table. Seeded via SQL because
            // an actual API drop would make the append fail table resolution
            // (422 unknown table) before the conflict check — the conflict
            // check keys on object_id, not liveness.
            seedDdlChange("cfl", "events", "table_dropped") // snapshot 3

            val append = """
            {%READ%"appends": [{"namespace": "ns", "table": "events", "files": [
              {"path": "s3://hog/cfl/f.parquet", "record_count": 1, "file_size_bytes": 10}]}]}
        """
            val conflicted =
                client.postJson(
                    "/v1/catalogs/cfl/commit",
                    append.replace("%READ%", """"read_snapshot": 2, """),
                )
            val error = assertApiError(conflicted, HttpStatusCode.Conflict, "commit_conflict")
            assertThat(error["detail"].asText()).contains("ns.events")

            // The same append as a blind commit (no read_snapshot) sails through.
            val blind = client.postJson("/v1/catalogs/cfl/commit", append.replace("%READ%", ""))
            assertThat(blind.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(blind)["snapshot_id"].asLong()).isEqualTo(4)
        }

    @Test
    fun `commit validation failures - 422 with ApiError body`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "vld", "data_path": "s3://hog/vld"}""")

            // Unknown table.
            val unknown =
                client.postJson(
                    "/v1/catalogs/vld/commit",
                    """
            {"appends": [{"namespace": "nope", "table": "missing", "files": [
              {"path": "s3://hog/vld/f.parquet", "record_count": 1, "file_size_bytes": 1}]}]}
            """,
                )
            val err = assertApiError(unknown, HttpStatusCode.UnprocessableEntity, "validation")
            assertThat(err["detail"].asText()).contains("nope.missing")

            // Empty appends / empty files.
            assertApiError(
                client.postJson("/v1/catalogs/vld/commit", """{"appends": []}"""),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            assertApiError(
                client.postJson(
                    "/v1/catalogs/vld/commit",
                    """{"appends": [{"namespace": "n", "table": "t", "files": []}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }

    @Test
    fun `unknown column type in createTable - 422`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "badcol", "data_path": "s3://hog/badcol"}""")
            client.postJson("/v1/catalogs/badcol/namespaces", """{"name": "ns"}""")
            val bad =
                client.postJson(
                    "/v1/catalogs/badcol/namespaces/ns/tables",
                    """{"name": "t", "columns": [{"name": "c", "type": "varchar"}]}""",
                )
            val err = assertApiError(bad, HttpStatusCode.UnprocessableEntity, "validation")
            assertThat(err["detail"].asText()).contains("varchar")
        }

    // ---- malformed input -> 400 ------------------------------------------

    @Test
    fun `malformed request bodies - 400 with ApiError body`() =
        api { client ->
            // Truncated JSON.
            assertApiError(
                client.postJson("/v1/catalogs", """{"name": "x" """),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            // Missing required field (data_path).
            assertApiError(
                client.postJson("/v1/catalogs", """{"name": "x"}"""),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            // Non-base64 bound in commit stats (format: byte).
            assertApiError(
                client.postJson(
                    "/v1/catalogs/any/commit",
                    """
                {"appends": [{"namespace": "n", "table": "t", "files": [
                  {"path": "s3://b/f.parquet", "record_count": 1, "file_size_bytes": 1,
                   "column_stats": [{"field_id": 1, "value_count": 1, "null_count": 0,
                                     "lower_bound": "!!!not base64!!!"}]}]}]}
                """,
                ),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }

    @Test
    fun `unparseable query and path params - 400`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "qp", "data_path": "s3://hog/qp"}""")

            assertApiError(
                client.get("/v1/catalogs/qp/namespaces/n/tables/t?snapshot=abc"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.get("/v1/catalogs/qp/namespaces/n/tables/t/changes"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.get("/v1/catalogs/qp/namespaces/n/tables/t/changes?from_snapshot=1e3"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.get("/v1/catalogs/qp/snapshots?after=abc"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.get("/v1/catalogs/qp/snapshots?limit=lots"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            assertApiError(
                client.putJson(
                    "/v1/catalogs/qp/consumers/c/offsets/not-a-uuid",
                    """{"snapshot_id": 0}""",
                ),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            // Parseable but out-of-contract limit is a validation error.
            assertApiError(
                client.get("/v1/catalogs/qp/snapshots?limit=0"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }

    // ---- 404s and duplicates ---------------------------------------------

    @Test
    fun `missing resources - 404 with ApiError body`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "nf", "data_path": "s3://hog/nf"}""")

            assertApiError(client.get("/v1/catalogs/nope"), HttpStatusCode.NotFound, "not_found")
            assertApiError(
                client.get("/v1/catalogs/nope/namespaces"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertApiError(
                client.get("/v1/catalogs/nf/namespaces/nope/tables/t"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertApiError(
                client.delete("/v1/catalogs/nf/namespaces/nope/tables/t"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            assertApiError(
                client.postJson(
                    "/v1/catalogs/nope/commit",
                    """
                {"appends": [{"namespace": "n", "table": "t", "files": [
                  {"path": "s3://b/f.parquet", "record_count": 1, "file_size_bytes": 1}]}]}
                """,
                ),
                HttpStatusCode.NotFound,
                "not_found",
            )
        }

    @Test
    fun `duplicate catalog namespace and table - 409`() =
        api { client ->
            val mk = """{"name": "dup", "data_path": "s3://hog/dup"}"""
            assertThat(client.postJson("/v1/catalogs", mk).status).isEqualTo(HttpStatusCode.Created)
            assertApiError(
                client.postJson("/v1/catalogs", mk),
                HttpStatusCode.Conflict,
                "already_exists",
            )

            client.postJson("/v1/catalogs/dup/namespaces", """{"name": "ns"}""")
            assertApiError(
                client.postJson("/v1/catalogs/dup/namespaces", """{"name": "ns"}"""),
                HttpStatusCode.Conflict,
                "already_exists",
            )

            val mkTable = """{"name": "t", "columns": [{"name": "id", "type": "long"}]}"""
            client.postJson("/v1/catalogs/dup/namespaces/ns/tables", mkTable)
            assertApiError(
                client.postJson("/v1/catalogs/dup/namespaces/ns/tables", mkTable),
                HttpStatusCode.Conflict,
                "already_exists",
            )
        }

    // ---- helpers ---------------------------------------------------------

    /**
     * Mint a snapshot with one DDL change row via direct SQL — the same
     * concurrent-DDL simulation CommitServiceTest uses (the conflict
     * check keys on hog_snapshot_change.object_id).
     */
    private fun seedDdlChange(
        catalogName: String,
        tableName: String,
        kind: String,
    ) {
        db.jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                    .bind(0, catalogName)
                    .mapTo(Long::class.java)
                    .one()
            val tableId =
                h.createQuery(
                    """
                SELECT table_id FROM hog_table_version
                 WHERE catalog_id = ? AND name = ? AND end_snapshot IS NULL
                """,
                )
                    .bind(0, catalogId)
                    .bind(1, tableName)
                    .mapTo(Long::class.java)
                    .one()
            val snapshotId =
                h.createQuery(
                    """
                UPDATE hog_catalog SET last_snapshot_id = last_snapshot_id + 1
                 WHERE catalog_id = ? RETURNING last_snapshot_id
                """,
                )
                    .bind(0, catalogId)
                    .mapTo(Long::class.java)
                    .one()
            h.createUpdate(
                """
                INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version)
                SELECT catalog_id, ?, schema_version FROM hog_catalog WHERE catalog_id = ?
                """,
            )
                .bind(0, snapshotId)
                .bind(1, catalogId)
                .execute()
            h.createUpdate(
                """
                INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
                VALUES (?, ?, ?, ?)
                """,
            )
                .bind(0, catalogId)
                .bind(1, snapshotId)
                .bind(2, kind)
                .bind(3, tableId)
                .execute()
        }
    }
}
