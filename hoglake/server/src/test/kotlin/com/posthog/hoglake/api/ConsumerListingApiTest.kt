package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Wire-level pinning for GET /v1/catalogs/{catalog}/consumers — the
 * webui consumer listing builds against EXACTLY this JSON shape, so
 * field names are asserted verbatim and committed_snapshot is asserted
 * to be a JSON number. Semantics pinned: grouping by consumer, table
 * names resolved, dropped tables keep their last name and are flagged
 * (offsets outlive drops by design).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerListingApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)

    private val cat = seed()

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    private fun seed(): String {
        val cat = "wire-consumers"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "analytics")
        val events =
            catalogs.createTable(
                cat,
                "analytics",
                "events",
                listOf(ColumnDef("id", ColType.LONG)),
            )
        val raw =
            catalogs.createTable(cat, "analytics", "raw", listOf(ColumnDef("id", ColType.LONG)))
        val head =
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "analytics",
                                "events",
                                listOf(
                                    FileRegistration(
                                        path = "s3://bucket/$cat/e1.parquet",
                                        recordCount = 10,
                                        fileSizeBytes = 1_048_576,
                                    ),
                                ),
                            ),
                        ),
                ),
            ).snapshotId
        catalogs.commitOffset(cat, "hedgerow-a", events.tableUuid, head)
        catalogs.commitOffset(cat, "hedgerow-a", raw.tableUuid, 0)
        catalogs.commitOffset(cat, "hedgerow-b", events.tableUuid, 0)
        catalogs.dropTable(cat, "analytics", "raw")
        return cat
    }

    @Test
    fun `lists consumers grouped with resolved names and verbatim field names`() =
        api { client ->
            val res = client.get("/v1/catalogs/$cat/consumers")
            assertThat(res.status).isEqualTo(HttpStatusCode.OK)
            val root = body(res)
            assertThat(root.fieldNames().asSequence().toList()).containsExactly("consumers")

            val consumers = root["consumers"]
            assertThat(consumers.size()).isEqualTo(2)
            assertThat(consumers.map { it["consumer_id"].asText() })
                .containsExactly("hedgerow-a", "hedgerow-b")

            val a = consumers[0]["offsets"]
            assertThat(a.size()).isEqualTo(2)
            val first = a[0]
            assertThat(first.fieldNames().asSequence().toList())
                .containsExactlyInAnyOrder(
                    "table_uuid",
                    "committed_snapshot",
                    "updated_at",
                    "namespace",
                    "table_name",
                    "table_dropped",
                )
            // ordered namespace.table: events before raw
            assertThat(first["table_name"].asText()).isEqualTo("events")
            assertThat(first["namespace"].asText()).isEqualTo("analytics")
            assertThat(first["table_dropped"].asBoolean()).isFalse()
            assertThat(first["committed_snapshot"].isNumber).isTrue()
            // head after seed = 4: namespace(1), 2x createTable(2,3), append(4)
            assertThat(first["committed_snapshot"].asLong()).isEqualTo(4L)

            // the dropped table keeps its last name and is flagged
            val rawRow = a[1]
            assertThat(rawRow["table_name"].asText()).isEqualTo("raw")
            assertThat(rawRow["table_dropped"].asBoolean()).isTrue()

            assertThat(consumers[1]["offsets"].size()).isEqualTo(1)
        }

    @Test
    fun `instance info carries the configured name and omits it when unset`() {
        api { client ->
            val unnamed = client.get("/v1/info")
            assertThat(unnamed.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(unnamed).has("name")).isFalse()
        }
        val named = App.build(Config(hydratorIntervalMs = 0, instanceName = "GigaHog"), db.jdbi)
        testApplication {
            application { named.module(this) }
            val res = client.get("/v1/info")
            assertThat(body(res)["name"].asText()).isEqualTo("GigaHog")
        }
    }

    @Test
    fun `empty catalog lists no consumers and unknown catalog 404s`() =
        api { client ->
            catalogs.createCatalog("wire-consumers-empty", "s3://bucket/empty")
            val empty = client.get("/v1/catalogs/wire-consumers-empty/consumers")
            assertThat(empty.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(empty)["consumers"].size()).isEqualTo(0)

            val missing = client.get("/v1/catalogs/wire-consumers-nope/consumers")
            assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
        }
}
