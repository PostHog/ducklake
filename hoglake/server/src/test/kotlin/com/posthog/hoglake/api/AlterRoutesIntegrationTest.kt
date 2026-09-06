package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * Wire-level tests for POST .../alter: raw JSON against the OpenAPI
 * contract (snake_case fields, spec status codes), with the same
 * serialization stack App.module installs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterRoutesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val json = ObjectMapper()
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    // ---- harness ---------------------------------------------------------

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                // Exactly App.module's serialization + error stack.
                this.install(ContentNegotiation) {
                    jackson {
                        registerKotlinModule()
                        registerModule(JavaTimeModule())
                        propertyNamingStrategy = PropertyNamingStrategies.SNAKE_CASE
                        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                        setSerializationInclusion(JsonInclude.Include.NON_NULL)
                    }
                }
                this.install(StatusPages) { installErrorMapping() }
                installAlterRoutes(alter)
            }
            block(client)
        }

    private suspend fun HttpClient.alterJson(
        path: String,
        body: String,
    ): HttpResponse =
        post(path) {
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

    /** Catalog + namespace + a two-column table; returns the alter URL. */
    private fun fixture(): String {
        val cat = "alter-wire-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "events",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("ts", ColType.TIMESTAMPTZ),
            ),
        )
        return "/v1/catalogs/$cat/namespaces/ns/tables/events/alter"
    }

    // ---- happy path ------------------------------------------------------

    @Test
    fun `alter happy path - add column and set spec, snake_case wire shape`() =
        api { client ->
            val url = fixture()
            val resp =
                client.alterJson(
                    url,
                    """
            {"ops": [
                {"op": "add_column",
                 "column": {"name": "team_id", "type": "int", "nullable": false}},
                {"op": "set_partition_spec",
                 "fields": [
                    {"source_field_id": 2, "transform": "day"},
                    {"source_field_id": 3, "transform": "bucket", "transform_param": 8}
                 ]}
            ]}
            """,
                )
            assertThat(resp.status).isEqualTo(HttpStatusCode.OK)
            val table = body(resp)
            assertThat(table["name"].asText()).isEqualTo("events")
            assertThat(table["namespace"].asText()).isEqualTo("ns")
            assertThat(table["table_uuid"].asText()).isNotBlank()
            assertThat(table["record_count"].asLong()).isEqualTo(0)
            assertThat(table["file_count"].asLong()).isEqualTo(0)
            assertThat(table["file_size_bytes"].asLong()).isEqualTo(0)

            val cols = table["columns"]
            assertThat(cols.map { it["name"].asText() }).containsExactly("id", "ts", "team_id")
            val added = cols[2]
            assertThat(added["type"].asText()).isEqualTo("int")
            assertThat(added["nullable"].asBoolean()).isFalse()
            assertThat(added["field_id"].asLong()).isEqualTo(3)
            assertThat(added["ordinal"].asInt()).isEqualTo(2)

            val spec = table["partition_spec"]
            assertThat(spec["spec_id"].asLong()).isEqualTo(1)
            val fields = spec["fields"]
            assertThat(fields).hasSize(2)
            assertThat(fields[0]["source_field_id"].asLong()).isEqualTo(2)
            assertThat(fields[0]["transform"].asText()).isEqualTo("day")
            assertThat(fields[0].has("transform_param")).isFalse()
            assertThat(fields[1]["transform"].asText()).isEqualTo("bucket")
            assertThat(fields[1]["transform_param"].asInt()).isEqualTo(8)
        }

    @Test
    fun `unpartitioned table omits partition_spec entirely`() =
        api { client ->
            val url = fixture()
            val resp =
                client.alterJson(
                    url,
                    """{"ops": [{"op": "rename_column", "from": "ts", "to": "occurred_at"}]}""",
                )
            assertThat(resp.status).isEqualTo(HttpStatusCode.OK)
            val table = body(resp)
            assertThat(table.has("partition_spec")).isFalse()
            assertThat(table["columns"].map { it["name"].asText() })
                .containsExactly("id", "occurred_at")
        }

    // ---- error shapes ----------------------------------------------------

    @Test
    fun `unknown op is 400 bad_request`() =
        api { client ->
            val url = fixture()
            val resp = client.alterJson(url, """{"ops": [{"op": "explode_table"}]}""")
            val err = assertApiError(resp, HttpStatusCode.BadRequest, "bad_request")
            assertThat(err["detail"].asText()).contains("explode_table")
        }

    @Test
    fun `missing field for the chosen op is 400 bad_request`() =
        api { client ->
            val url = fixture()
            val cases =
                listOf(
                    """{"ops": [{"op": "add_column"}]}""",
                    """{"ops": [{"op": "drop_column"}]}""",
                    """{"ops": [{"op": "rename_column", "from": "id"}]}""",
                    """{"ops": [{"op": "rename_table"}]}""",
                    """{"ops": [{"op": "set_partition_spec"}]}""",
                )
            for (case in cases) {
                assertApiError(client.alterJson(url, case), HttpStatusCode.BadRequest, "bad_request")
            }
        }

    @Test
    fun `empty ops is 400 bad_request`() =
        api { client ->
            val url = fixture()
            assertApiError(
                client.alterJson(url, """{"ops": []}"""),
                HttpStatusCode.BadRequest,
                "bad_request",
            )
        }

    @Test
    fun `semantic failure is 422 validation`() =
        api { client ->
            val url = fixture()
            val resp = client.alterJson(url, """{"ops": [{"op": "drop_column", "name": "nope"}]}""")
            val err = assertApiError(resp, HttpStatusCode.UnprocessableEntity, "validation")
            assertThat(err["detail"].asText()).contains("nope")

            // Illegal promotion.
            assertApiError(
                client.alterJson(
                    url,
                    """{"ops": [{"op": "promote_column", "name": "id", "to": "int"}]}""",
                ),
                HttpStatusCode.UnprocessableEntity,
                "validation",
            )
        }

    @Test
    fun `unknown table is 404 not_found`() =
        api { client ->
            fixture()
            val resp =
                client.alterJson(
                    "/v1/catalogs/no-such/namespaces/ns/tables/events/alter",
                    """{"ops": [{"op": "drop_column", "name": "ts"}]}""",
                )
            assertApiError(resp, HttpStatusCode.NotFound, "not_found")
        }

    @Test
    fun `rename_table collision is 409 already_exists`() =
        api { client ->
            val url = fixture()
            val cat = url.removePrefix("/v1/catalogs/").substringBefore("/")
            catalogs.createTable(cat, "ns", "taken", listOf(ColumnDef("id", ColType.LONG)))
            val resp =
                client.alterJson(
                    url,
                    """{"ops": [{"op": "rename_table", "new_name": "taken"}]}""",
                )
            assertApiError(resp, HttpStatusCode.Conflict, "already_exists")
        }
}
