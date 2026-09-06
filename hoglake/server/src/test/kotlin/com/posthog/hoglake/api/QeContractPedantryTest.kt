package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.patch
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

/**
 * Contract pedantry against openapi/hoglake.yaml (raw JSON on the
 * wire): exact status codes at the 400/404/409/410/422 boundaries the
 * spec draws, spec-required fields present on every response schema,
 * pure snake_case everywhere, and NON_NULL-omission behavior audited
 * against the spec's required lists. QE-found server/spec disagreements
 * are fixed and pinned here as regression tests.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QeContractPedantryTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

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

    private suspend fun HttpClient.patchJson(
        url: String,
        body: String,
    ): HttpResponse =
        patch(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(r: HttpResponse): JsonNode = json.readTree(r.bodyAsText())

    private suspend fun expectError(
        r: HttpResponse,
        status: HttpStatusCode,
        code: String,
    ) {
        assertThat(r.status).describedAs("status for %s", r.call.request.url).isEqualTo(status)
        val node = body(r)
        assertThat(node["error"].asText()).describedAs("ApiError.error").isEqualTo(code)
    }

    /** Every key, recursively, must be pure snake_case (no camelCase leaks). */
    private fun assertSnakeCase(
        node: JsonNode,
        at: String = "$",
    ) {
        when {
            node.isObject ->
                node.fields().forEach { (k, v) ->
                    assertThat(k)
                        .describedAs("field name at %s must be snake_case", at)
                        .matches("^[a-z0-9_]+$")
                    assertSnakeCase(v, "$at.$k")
                }
            node.isArray -> node.forEachIndexed { i, v -> assertSnakeCase(v, "$at[$i]") }
        }
    }

    private fun assertRequired(
        node: JsonNode,
        schema: String,
        vararg fields: String,
    ) {
        for (f in fields) {
            assertThat(node.has(f))
                .describedAs("spec-required field '%s' missing from %s: %s", f, schema, node)
                .isTrue()
        }
    }

    // ---- fixture ---------------------------------------------------------

    /** One catalog with the full object graph, built once, wire-only. */
    private var built = false
    private lateinit var tableUuid: String

    private fun ensureFixture() =
        api { client ->
            if (built) return@api
            client.postJson("/v1/catalogs", """{"name":"pedantry","data_path":"s3://qe/p"}""")
            client.postJson("/v1/catalogs/pedantry/namespaces", """{"name":"ns"}""")
            val t =
                body(
                    client.postJson(
                        "/v1/catalogs/pedantry/namespaces/ns/tables",
                        """{"name":"t","columns":[
                    {"name":"id","type":"long","nullable":false},
                    {"name":"ts","type":"timestamptz"}]}""",
                    ),
                )
            tableUuid = t["table_uuid"].asText()
            client.postJson(
                "/v1/catalogs/pedantry/commit",
                """{"appends":[{"namespace":"ns","table":"t","files":[
                {"path":"s3://qe/p/f1.parquet","record_count":10,"file_size_bytes":100}]}]}""",
            ) // snapshot 3
            client.postJson(
                "/v1/catalogs/pedantry/commit",
                """{"read_snapshot":3,
                "deletes":[{"namespace":"ns","table":"t","files":[
                {"data_file_id":1,"path":"s3://qe/p/dv1.puffin","delete_count":2,
                 "file_size_bytes":16}]}]}""",
            ) // snapshot 4
            client.postJson(
                "/v1/catalogs/pedantry/namespaces/ns/views",
                """{"name":"v","sql":"SELECT 1"}""",
            ) // snapshot 5
            client.putJson(
                "/v1/catalogs/pedantry/consumers/c1/offsets/$tableUuid",
                """{"snapshot_id":4}""",
            )
            built = true
        }

    // ---- response-schema sweep: required fields + snake_case --------------

    @Test
    fun `every response schema carries its spec-required fields in snake_case`() {
        ensureFixture()
        api { client ->
            val catalog = body(client.get("/v1/catalogs/pedantry"))
            assertSnakeCase(catalog)
            assertRequired(catalog, "Catalog", "name", "data_path", "head_snapshot_id", "schema_version")

            val list = body(client.get("/v1/catalogs"))
            assertSnakeCase(list)
            assertThat(list.isArray).isTrue()

            val ns = body(client.get("/v1/catalogs/pedantry/namespaces/ns"))
            assertSnakeCase(ns)
            assertRequired(ns, "Namespace", "name")

            val tables = body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables"))
            assertSnakeCase(tables)
            assertRequired(tables[0], "TableSummary", "name", "table_uuid")

            val table = body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables/t"))
            assertSnakeCase(table)
            assertRequired(
                table, "Table",
                "name", "namespace", "table_uuid", "columns", "record_count",
                "file_count", "file_size_bytes",
            )
            assertRequired(table["columns"][0], "Column", "name", "type", "field_id", "ordinal")

            val files = body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables/t/files"))
            assertSnakeCase(files)
            assertRequired(
                files[0], "DataFile",
                "data_file_id", "path", "file_format", "record_count",
                "file_size_bytes", "row_id_start", "stats_state", "begin_snapshot",
            )

            val scan = body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables/t/scan"))
            assertSnakeCase(scan)
            assertRequired(scan[0], "ScanFile", "data_file")
            assertRequired(
                scan[0]["delete_file"], "DeleteFile",
                "delete_file_id", "data_file_id", "path", "file_format",
                "delete_count", "file_size_bytes", "begin_snapshot",
            )

            val changes =
                body(
                    client.get("/v1/catalogs/pedantry/namespaces/ns/tables/t/changes?from_snapshot=0"),
                )
            assertSnakeCase(changes)
            assertRequired(
                changes,
                "ChangePlan",
                "table_uuid",
                "from_snapshot",
                "to_snapshot",
                "files",
                "delete_files",
            )

            val snapshots = body(client.get("/v1/catalogs/pedantry/snapshots"))
            assertSnakeCase(snapshots)
            assertRequired(snapshots, "SnapshotPage", "snapshots", "has_more")
            assertRequired(
                snapshots["snapshots"][0],
                "Snapshot",
                "snapshot_id",
                "snapshot_time",
                "schema_version",
            )

            val offsets = body(client.get("/v1/catalogs/pedantry/consumers/c1/offsets"))
            assertSnakeCase(offsets)
            assertRequired(
                offsets[0],
                "ConsumerOffset",
                "consumer_id",
                "table_uuid",
                "committed_snapshot",
                "updated_at",
            )

            val view = body(client.get("/v1/catalogs/pedantry/namespaces/ns/views/v"))
            assertSnakeCase(view)
            assertRequired(view, "View", "name", "namespace", "view_uuid", "dialect", "sql")

            val options = body(client.get("/v1/catalogs/pedantry/options"))
            assertSnakeCase(options)
            assertRequired(options, "CatalogOptions", "consumer_floor", "earliest_snapshot_id")
            // NON_NULL audit: snapshot_retention_seconds is nullable AND
            // not spec-required, so omitting it while unset is legal.
            assertThat(options.has("snapshot_retention_seconds")).isFalse()

            val expire = body(client.postJson("/v1/catalogs/pedantry/maintenance/expire", ""))
            assertSnakeCase(expire)
            assertRequired(
                expire,
                "ExpiryResult",
                "snapshots_expired",
                "data_files_queued",
                "delete_files_queued",
                "new_earliest_snapshot_id",
            )

            val cleanup = body(client.postJson("/v1/catalogs/pedantry/maintenance/cleanup", ""))
            assertSnakeCase(cleanup)
            assertRequired(cleanup, "CleanupResult", "removed", "missing", "still_referenced")

            // Commit result: spec requires snapshot_id; schema_version rides along.
            val commit =
                body(
                    client.postJson(
                        "/v1/catalogs/pedantry/commit",
                        """{"appends":[{"namespace":"ns","table":"t","files":[
                        {"path":"s3://qe/p/f2.parquet","record_count":1,"file_size_bytes":1}]}]}""",
                    ),
                )
            assertSnakeCase(commit)
            assertRequired(commit, "CommitResult", "snapshot_id")
        }
    }

    @Test
    fun `getTable returns partition_spec for a partitioned table, omits it otherwise`() {
        ensureFixture()
        api { client ->
            val altered =
                body(
                    client.postJson(
                        "/v1/catalogs/pedantry/namespaces/ns/tables/t/alter",
                        """{"ops":[{"op":"set_partition_spec","fields":[
                        {"source_field_id":1,"transform":"identity"}]}]}""",
                    ),
                )
            // The alter response carries the spec, as the spec's Table says.
            assertThat(altered.has("partition_spec")).isTrue()
            assertRequired(altered["partition_spec"], "PartitionSpec", "spec_id", "fields")
            assertRequired(
                altered["partition_spec"]["fields"][0],
                "PartitionField",
                "source_field_id",
                "transform",
            )
            // GET table mirrors it: same shape, loaded live at the read
            // snapshot (regression pin for the QE-found asymmetry where
            // TableDto had no partition_spec at all).
            val got = body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables/t"))
            assertThat(got.has("partition_spec"))
                .describedAs("partition_spec present on getTable")
                .isTrue()
            assertSnakeCase(got)
            assertThat(got["partition_spec"]).isEqualTo(altered["partition_spec"])
            val partitionedAt = body(client.get("/v1/catalogs/pedantry"))["head_snapshot_id"].asLong()
            // Restore the unpartitioned state for the other tests; the
            // spec then disappears from head reads (NON_NULL omission)...
            client.postJson(
                "/v1/catalogs/pedantry/namespaces/ns/tables/t/alter",
                """{"ops":[{"op":"set_partition_spec","fields":[]}]}""",
            )
            val unpartitioned = body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables/t"))
            assertThat(unpartitioned.has("partition_spec"))
                .describedAs("partition_spec omitted when unpartitioned")
                .isFalse()
            // ...but time travel to the partitioned snapshot still sees it
            // (visible-at-snapshot semantics, like every other read).
            val travelled =
                body(
                    client.get(
                        "/v1/catalogs/pedantry/namespaces/ns/tables/t?snapshot=$partitionedAt",
                    ),
                )
            assertThat(travelled["partition_spec"]).isEqualTo(altered["partition_spec"])
        }
    }

    // ---- 404 vs 422 vs 409 vs 410 vs 400 boundaries -----------------------

    @Test
    fun `unknown catalog vs unknown table vs unknown column draw 404-422-422`() {
        ensureFixture()
        api { client ->
            val appendBody = """{"appends":[{"namespace":"ns","table":"%T%","files":[
                {"path":"s3://qe/p/x.parquet","record_count":1,"file_size_bytes":1}]}]}"""
            // Unknown catalog -> 404.
            expectError(
                client.postJson("/v1/catalogs/ghost/commit", appendBody.replace("%T%", "t")),
                HttpStatusCode.NotFound,
                "not_found",
            )
            // Known catalog, unknown table -> 422 (commit validates, per spec).
            expectError(
                client.postJson("/v1/catalogs/pedantry/commit", appendBody.replace("%T%", "ghost")),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            // Known table, unknown column (field_id) in stats -> 422.
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/commit",
                    """{"appends":[{"namespace":"ns","table":"t","files":[
                        {"path":"s3://qe/p/x.parquet","record_count":1,"file_size_bytes":1,
                         "column_stats":[{"field_id":999,"value_count":1,"null_count":0}]}]}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            // Alter draws the same line: catalog/namespace/table missing ->
            // 404; unknown COLUMN inside a known table -> 422.
            val drop = """{"ops":[{"op":"drop_column","name":"ghost"}]}"""
            expectError(
                client.postJson("/v1/catalogs/ghost/namespaces/ns/tables/t/alter", drop),
                HttpStatusCode.NotFound,
                "not_found",
            )
            expectError(
                client.postJson("/v1/catalogs/pedantry/namespaces/ghost/tables/t/alter", drop),
                HttpStatusCode.NotFound,
                "not_found",
            )
            expectError(
                client.postJson("/v1/catalogs/pedantry/namespaces/ns/tables/ghost/alter", drop),
                HttpStatusCode.NotFound,
                "not_found",
            )
            expectError(
                client.postJson("/v1/catalogs/pedantry/namespaces/ns/tables/t/alter", drop),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }
    }

    @Test
    fun `snapshot boundaries - beyond head 422, negative 422, below floor 410, both params 422`() {
        api { client ->
            client.postJson("/v1/catalogs", """{"name":"floors","data_path":"s3://qe/fl"}""")
            client.postJson("/v1/catalogs/floors/namespaces", """{"name":"ns"}""")
            client.postJson(
                "/v1/catalogs/floors/namespaces/ns/tables",
                """{"name":"t","columns":[{"name":"id","type":"long"}]}""",
            ) // head = 2
            repeat(3) { i ->
                client.postJson(
                    "/v1/catalogs/floors/commit",
                    """{"appends":[{"namespace":"ns","table":"t","files":[
                        {"path":"s3://qe/fl/f$i.parquet","record_count":1,"file_size_bytes":1}]}]}""",
                )
            } // head = 5
            // Raise the expiry floor to 3 the way a sweep would.
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    UPDATE hog_catalog SET earliest_snapshot_id = 3 WHERE name = 'floors';
                    """,
                ).execute()
                h.createUpdate(
                    "DELETE FROM hog_snapshot WHERE snapshot_id < 3 " +
                        "AND catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = 'floors')",
                ).execute()
            }

            val base = "/v1/catalogs/floors/namespaces/ns/tables/t"
            for (path in listOf(base, "$base/files", "$base/scan")) {
                expectError(client.get("$path?snapshot=6"), HttpStatusCode.UnprocessableEntity, "validation")
                expectError(client.get("$path?snapshot=-1"), HttpStatusCode.UnprocessableEntity, "validation")
                expectError(client.get("$path?snapshot=2"), HttpStatusCode.Gone, "expired")
                expectError(
                    client.get("$path?snapshot=4&at_timestamp=2026-01-01T00:00:00Z"),
                    HttpStatusCode.UnprocessableEntity,
                    "validation",
                )
                expectError(
                    client.get("$path?at_timestamp=1999-01-01T00:00:00Z"),
                    HttpStatusCode.Gone,
                    "expired",
                )
                // Head reads never hit the floor.
                assertThat(client.get(path).status).isEqualTo(HttpStatusCode.OK)
                // Snapshot exactly at the floor is retained.
                assertThat(client.get("$path?snapshot=3").status).isEqualTo(HttpStatusCode.OK)
            }

            // Changefeed floor: from == earliest - 1 is the lowest legal value.
            expectError(
                client.get("$base/changes?from_snapshot=1"),
                HttpStatusCode.Gone,
                "expired",
            )
            assertThat(client.get("$base/changes?from_snapshot=2").status)
                .isEqualTo(HttpStatusCode.OK)
            expectError(
                client.get("$base/changes?from_snapshot=4&to_snapshot=3"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.get("$base/changes?from_snapshot=4&to_snapshot=99"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.get("$base/changes?from_snapshot=-1"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )

            // 2^63 overflows the long parser -> 400, not 422.
            expectError(
                client.get("$base?snapshot=9223372036854775808"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
            // 2^62 parses -> out of range -> 422.
            expectError(
                client.get("$base?snapshot=4611686018427387904"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }
    }

    @Test
    fun `unknown enum literals - 422 for values, 400 for the op discriminator`() {
        ensureFixture()
        api { client ->
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/tables",
                    """{"name":"badenum","columns":[{"name":"c","type":"varchar2"}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/tables/t/alter",
                    """{"ops":[{"op":"promote_column","name":"id","to":"hugeint"}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/tables/t/alter",
                    """{"ops":[{"op":"set_partition_spec","fields":[
                        {"source_field_id":1,"transform":"truncate"}]}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/tables/t/alter",
                    """{"ops":[{"op":"explode_table"}]}""",
                ),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }
    }

    @Test
    fun `offset contract - 404 catalog, 422 out of range, 409 regression, 422 oversized id`() {
        ensureFixture()
        api { client ->
            expectError(
                client.putJson("/v1/catalogs/ghost/consumers/c/offsets/$tableUuid", """{"snapshot_id":0}"""),
                HttpStatusCode.NotFound,
                "not_found",
            )
            expectError(
                client.putJson(
                    "/v1/catalogs/pedantry/consumers/c1/offsets/$tableUuid",
                    """{"snapshot_id":4611686018427387904}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.putJson(
                    "/v1/catalogs/pedantry/consumers/c1/offsets/$tableUuid",
                    """{"snapshot_id":-1}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.putJson(
                    "/v1/catalogs/pedantry/consumers/c1/offsets/$tableUuid",
                    """{"snapshot_id":1}""",
                ),
                HttpStatusCode.Conflict,
                "offset_regression",
            )
            expectError(
                client.putJson(
                    "/v1/catalogs/pedantry/consumers/${"x".repeat(129)}/offsets/$tableUuid",
                    """{"snapshot_id":0}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            // Unknown consumer id on GET is an empty list, not a 404 (the
            // resource is a keyspace, not a registered entity).
            val empty = body(client.get("/v1/catalogs/pedantry/consumers/never-seen/offsets"))
            assertThat(empty.isArray).isTrue()
            assertThat(empty).isEmpty()
        }
    }

    @Test
    fun `offsets reject a garbage table_uuid but accept a dropped table's uuid`() {
        ensureFixture()
        api { client ->
            // Regression pin for the QE-found floor pin: a garbage uuid
            // used to be accepted, and with consumer_floor on it pinned
            // the catalog's retention floor forever. The PUT now
            // validates the uuid against hog_table (ANY incarnation).
            val garbage = "00000000-0000-4000-8000-00000000dead"
            expectError(
                client.putJson(
                    "/v1/catalogs/pedantry/consumers/pinner/offsets/$garbage",
                    """{"snapshot_id":0}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            // A DROPPED table's uuid stays a valid offset key: offsets
            // deliberately survive drops so consumers SEE incarnation
            // changes rather than deducing them.
            client.postJson(
                "/v1/catalogs/pedantry/namespaces/ns/tables",
                """{"name":"doomed","columns":[{"name":"id","type":"long"}]}""",
            )
            val doomedUuid =
                body(client.get("/v1/catalogs/pedantry/namespaces/ns/tables/doomed"))["table_uuid"]
                    .asText()
            client.delete("/v1/catalogs/pedantry/namespaces/ns/tables/doomed")
            val r =
                client.putJson(
                    "/v1/catalogs/pedantry/consumers/pinner/offsets/$doomedUuid",
                    """{"snapshot_id":0}""",
                )
            assertThat(r.status)
                .describedAs("dropped-but-extant table_uuid accepted")
                .isEqualTo(HttpStatusCode.OK)
            // Clean up the pin for other tests.
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate("DELETE FROM hog_consumer_offset WHERE consumer_id = 'pinner'")
                    .execute()
            }
        }
    }

    @Test
    fun `hostile identifier names draw 422 on the wire`() {
        // POLICY CHANGE (2026-09-05): namespace/table/view/column names
        // must match ^[A-Za-z_][A-Za-z0-9_-]{0,127}$. This pins the wire
        // status for the shapes the server previously stored verbatim.
        ensureFixture()
        api { client ->
            expectError(
                client.postJson("/v1/catalogs/pedantry/namespaces", """{"name":"a/b"}"""),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/tables",
                    """{"name":"<script>","columns":[{"name":"id","type":"long"}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/tables",
                    """{"name":"ok_name","columns":[{"name":"id col","type":"long"}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/namespaces/ns/views",
                    """{"name":"v/w","sql":"SELECT 1"}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }
    }

    @Test
    fun `single-offset GET returns the row or a clean 404`() {
        ensureFixture()
        api { client ->
            val got = body(client.get("/v1/catalogs/pedantry/consumers/c1/offsets/$tableUuid"))
            assertSnakeCase(got)
            assertRequired(
                got,
                "ConsumerOffset",
                "consumer_id",
                "table_uuid",
                "committed_snapshot",
                "updated_at",
            )
            assertThat(got["table_uuid"].asText()).isEqualTo(tableUuid)
            // Garbage-but-well-formed uuid: clean 404, never a 500 or an
            // empty 200 (offsets-on-garbage-uuid is validated at PUT; the
            // GET simply has nothing stored).
            expectError(
                client.get(
                    "/v1/catalogs/pedantry/consumers/c1/offsets/00000000-0000-4000-8000-00000000dead",
                ),
                HttpStatusCode.NotFound,
                "not_found",
            )
            // Unknown consumer -> 404 too.
            expectError(
                client.get("/v1/catalogs/pedantry/consumers/never-seen/offsets/$tableUuid"),
                HttpStatusCode.NotFound,
                "not_found",
            )
            // Malformed uuid -> 400 (path param parse).
            expectError(
                client.get("/v1/catalogs/pedantry/consumers/c1/offsets/not-a-uuid"),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }
    }

    @Test
    fun `snapshots before cursor - descending page and cursor exclusivity`() {
        ensureFixture()
        api { client ->
            val head = body(client.get("/v1/catalogs/pedantry"))["head_snapshot_id"].asLong()
            val page = body(client.get("/v1/catalogs/pedantry/snapshots?before=${head + 1}&limit=3"))
            assertSnakeCase(page)
            val ids = page["snapshots"].map { it["snapshot_id"].asLong() }
            assertThat(ids).isEqualTo(ids.sortedDescending())
            assertThat(ids.first()).isEqualTo(head)
            // before=1 yields exactly snapshot 0.
            val bottom = body(client.get("/v1/catalogs/pedantry/snapshots?before=1"))
            assertThat(bottom["snapshots"].map { it["snapshot_id"].asLong() }).containsExactly(0L)
            assertThat(bottom["has_more"].asBoolean()).isFalse()
            // A non-zero after alongside before is 422; after=0 is legal.
            expectError(
                client.get("/v1/catalogs/pedantry/snapshots?after=2&before=5"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            assertThat(client.get("/v1/catalogs/pedantry/snapshots?after=0&before=5").status)
                .isEqualTo(HttpStatusCode.OK)
        }
    }

    @Test
    fun `negative read_snapshot is rejected 422`() {
        ensureFixture()
        api { client ->
            // Regression pin: read_snapshot < 0 used to slip through (only
            // "> head" was checked). It now draws the same validation line
            // as every other snapshot parameter.
            expectError(
                client.postJson(
                    "/v1/catalogs/pedantry/commit",
                    """{"read_snapshot":-5,
                    "appends":[{"namespace":"ns","table":"t","files":[
                    {"path":"s3://qe/p/neg.parquet","record_count":1,"file_size_bytes":1}]}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }
    }

    @Test
    fun `unknown request fields are rejected 400 as the spec documents`() {
        api { client ->
            val r =
                client.postJson(
                    "/v1/catalogs",
                    """{"name":"strictly","data_path":"s3://qe/s","bogus_field":1}""",
                )
            // Jackson's FAIL_ON_UNKNOWN_PROPERTIES default makes every
            // request schema effectively additionalProperties: false; the
            // spec's top-level description now states it ("unknown request
            // fields are 400 bad_request"), so this asserts documented
            // behavior.
            expectError(r, HttpStatusCode.BadRequest, "bad_request")
        }
    }

    @Test
    fun `publications endpoints answer 501 with the spec's not_implemented ApiError`() {
        ensureFixture()
        api { client ->
            // openapi/hoglake.yaml specifies /publications with "501 until
            // built"; the stub routes deliver exactly that contract
            // (regression pin for the bare-404 fall-through QE found).
            for (
            r in listOf(
                client.get("/v1/catalogs/pedantry/publications"),
                client.postJson(
                    "/v1/catalogs/pedantry/publications",
                    """{"name":"p","namespace":"ns","table":"t",
                        "sink":{"type":"kafka","bootstrap_servers":"b:9092","topic":"t"}}""",
                ),
                client.get("/v1/catalogs/pedantry/publications/p"),
                client.delete("/v1/catalogs/pedantry/publications/p"),
            )
            ) {
                assertThat(r.status)
                    .describedAs("status for %s", r.call.request.url)
                    .isEqualTo(HttpStatusCode.NotImplemented)
                val node = body(r)
                assertThat(node["error"].asText()).isEqualTo("not_implemented")
                assertThat(node["detail"].asText())
                    .isEqualTo("CDC publications are specified but not yet implemented")
            }
        }
    }

    @Test
    fun `pagination pedantry - limit coercion, floor, and after semantics`() {
        ensureFixture()
        api { client ->
            // limit above the spec maximum is coerced, not rejected.
            assertThat(client.get("/v1/catalogs/pedantry/snapshots?limit=99999").status)
                .isEqualTo(HttpStatusCode.OK)
            expectError(
                client.get("/v1/catalogs/pedantry/snapshots?limit=0"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            expectError(
                client.get("/v1/catalogs/pedantry/snapshots?limit=-3"),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
            // after beyond head: empty page, has_more false.
            val past = body(client.get("/v1/catalogs/pedantry/snapshots?after=100000"))
            assertThat(past["snapshots"]).isEmpty()
            assertThat(past["has_more"].asBoolean()).isFalse()
            // PINNED: negative after is accepted and behaves as "from the
            // beginning" — ids > -5 include 0 upward. Undocumented but sane.
            val neg = body(client.get("/v1/catalogs/pedantry/snapshots?after=-5&limit=2"))
            assertThat(neg["snapshots"][0]["snapshot_id"].asLong()).isEqualTo(0)
        }
    }

    @Test
    fun `maintenance batch validation - 0 is 422, junk is 400, unknown catalog 404`() {
        ensureFixture()
        api { client ->
            for (ep in listOf("expire", "cleanup")) {
                expectError(
                    client.postJson("/v1/catalogs/pedantry/maintenance/$ep?batch=0", ""),
                    HttpStatusCode.UnprocessableEntity,
                    "validation",
                )
                expectError(
                    client.postJson("/v1/catalogs/pedantry/maintenance/$ep?batch=lots", ""),
                    HttpStatusCode.BadRequest,
                    "bad_request",
                )
                expectError(
                    client.postJson("/v1/catalogs/ghost/maintenance/$ep", ""),
                    HttpStatusCode.NotFound,
                    "not_found",
                )
            }
        }
    }
}
