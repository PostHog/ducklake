package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
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
import java.util.UUID

/**
 * Wire-level pinning for GET /v1/catalogs/{catalog}/stats/partitions —
 * the webui builds against EXACTLY this JSON shape, so field names are
 * asserted verbatim and int64 fields are asserted to be JSON numbers
 * (never strings). Routes come from App.module (production wiring:
 * threshold = Config.compactionTargetBytes, default 512 MiB — the
 * seeded sizes below straddle it).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionStatsApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    // Seeding goes through the real services against the same database.
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)

    /** 5 GiB — over the 512 MiB default target, and over int32 on purpose. */
    private val bigBytes = 5_368_709_120L

    /** 40 MiB — small. */
    private val smallBytes = 41_943_040L

    /** Seeded once for the whole class (PER_CLASS); tests only read. */
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
        val cat = "wire-pstats"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "analytics")
        catalogs.createTable(
            cat,
            "analytics",
            "events",
            listOf(ColumnDef("id", ColType.LONG), ColumnDef("team", ColType.STRING)),
        )
        alter.alterTable(
            cat,
            "analytics",
            "events",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        catalogs.createTable(cat, "analytics", "raw", listOf(ColumnDef("id", ColType.LONG)))

        fun f(
            name: String,
            bytes: Long,
            values: List<String?>? = null,
        ) = FileRegistration(
            path = "s3://bucket/$cat/$name.parquet",
            recordCount = 10,
            fileSizeBytes = bytes,
            partitionValues = values,
        )
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "analytics",
                            "events",
                            listOf(
                                // team 42: debt 2 (one big, two small).
                                f("t42-big", bigBytes, listOf("42")),
                                f("t42-s1", smallBytes, listOf("42")),
                                f("t42-s2", smallBytes, listOf("42")),
                                // team 7: debt 1.
                                f("t7-s1", smallBytes, listOf("7")),
                                // null team: debt 1, fewer small bytes than team 7.
                                f("tnull-s1", 2_097_152, listOf(null)),
                            ),
                        ),
                        // Unpartitioned table: debt 1, fewest small bytes.
                        TableAppend("analytics", "raw", listOf(f("raw-s1", 1_048_576))),
                    ),
            ),
        )
        return cat
    }

    @Test
    fun `response shape, field names, ordering and int64-as-number are exactly as pinned`() =
        api { client ->
            val r = client.get("/v1/catalogs/$cat/stats/partitions")
            assertThat(r.status).isEqualTo(HttpStatusCode.OK)
            val node = body(r)

            assertThat(node.fieldNames().asSequence().toSet())
                .isEqualTo(setOf("partitions", "truncated", "stale_spec_groups"))
            assertThat(node["truncated"].isBoolean).isTrue()
            assertThat(node["truncated"].asBoolean()).isFalse()
            assertThat(node["stale_spec_groups"].asLong()).isEqualTo(0)

            val partitions = node["partitions"]
            assertThat(partitions.isArray).isTrue()
            assertThat(partitions).hasSize(4)

            // Ordering: debt desc, ties by small_file_bytes desc.
            val first = partitions[0]
            assertThat(first["debt_score"].asLong()).isEqualTo(2)
            assertThat(partitions[1]["partition_values"][0]["value"].asText()).isEqualTo("7")
            assertThat(partitions[2]["partition_values"][0]["value"].isNull).isTrue()
            assertThat(partitions[3]["table"].asText()).isEqualTo("raw")

            // Field names of a partitioned entry, verbatim.
            assertThat(first.fieldNames().asSequence().toSet())
                .isEqualTo(
                    setOf(
                        "namespace",
                        "table",
                        "table_uuid",
                        "partition_values",
                        "spec_id",
                        "file_count",
                        "small_file_count",
                        "total_bytes",
                        "small_file_bytes",
                        "avg_file_bytes",
                        "dv_count",
                        "debt_score",
                    ),
                )
            assertThat(first["namespace"].asText()).isEqualTo("analytics")
            assertThat(first["table"].asText()).isEqualTo("events")
            assertThat(UUID.fromString(first["table_uuid"].asText()))
                .isEqualTo(catalogs.getTable(cat, "analytics", "events", null, null).tableUuid)
            assertThat(first["partition_values"][0].fieldNames().asSequence().toSet())
                .isEqualTo(setOf("field", "value"))
            assertThat(first["partition_values"][0]["field"].asText()).isEqualTo("team")
            assertThat(first["partition_values"][0]["value"].asText()).isEqualTo("42")
            assertThat(first["spec_id"].asLong()).isEqualTo(1)

            // int64 fields are JSON numbers (int64-exact), never strings.
            for (field in listOf(
                "file_count",
                "small_file_count",
                "total_bytes",
                "small_file_bytes",
                "avg_file_bytes",
                "dv_count",
                "debt_score",
            )) {
                assertThat(first[field].isIntegralNumber).describedAs(field).isTrue()
                assertThat(first[field].isTextual).describedAs(field).isFalse()
            }
            assertThat(first["file_count"].asLong()).isEqualTo(3)
            assertThat(first["small_file_count"].asLong()).isEqualTo(2)
            assertThat(first["total_bytes"].asLong()).isEqualTo(bigBytes + 2 * smallBytes)
            assertThat(first["small_file_bytes"].asLong()).isEqualTo(2 * smallBytes)
            assertThat(first["avg_file_bytes"].asLong())
                .isEqualTo((bigBytes + 2 * smallBytes) / 3)
            assertThat(first["dv_count"].asLong()).isEqualTo(0)

            // Unpartitioned entry: empty partition_values, spec_id omitted.
            val raw = partitions[3]
            assertThat(raw["partition_values"].isArray).isTrue()
            assertThat(raw["partition_values"]).isEmpty()
            assertThat(raw.path("spec_id").isMissingNode).isTrue()

            // A null partition value serializes as an explicit null, keyed.
            val nullTeam = partitions[2]
            assertThat(nullTeam["partition_values"][0]["field"].asText()).isEqualTo("team")
            assertThat(nullTeam["partition_values"][0].has("value")).isTrue()
        }

    @Test
    fun `filters, limit and error statuses over the wire`() =
        api { client ->
            val limited = body(client.get("/v1/catalogs/$cat/stats/partitions?limit=1"))
            assertThat(limited["partitions"]).hasSize(1)
            assertThat(limited["truncated"].asBoolean()).isTrue()

            val scoped =
                body(
                    client.get(
                        "/v1/catalogs/$cat/stats/partitions?namespace=analytics&table=raw",
                    ),
                )
            assertThat(scoped["partitions"]).hasSize(1)
            assertThat(scoped["partitions"][0]["table"].asText()).isEqualTo("raw")

            // table without namespace -> 422 validation.
            val half = client.get("/v1/catalogs/$cat/stats/partitions?table=events")
            assertThat(half.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(body(half)["error"].asText()).isEqualTo("validation")

            // limit=0 -> 422; non-integer limit -> 400.
            assertThat(
                client.get("/v1/catalogs/$cat/stats/partitions?limit=0").status,
            ).isEqualTo(HttpStatusCode.UnprocessableEntity)
            val badLimit = client.get("/v1/catalogs/$cat/stats/partitions?limit=lots")
            assertThat(badLimit.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(body(badLimit)["error"].asText()).isEqualTo("bad_request")

            // Unknown names -> 404.
            for (url in listOf(
                "/v1/catalogs/no-such-cat/stats/partitions",
                "/v1/catalogs/$cat/stats/partitions?namespace=nope",
                "/v1/catalogs/$cat/stats/partitions?namespace=analytics&table=nope",
            )) {
                val resp = client.get(url)
                assertThat(resp.status).describedAs(url).isEqualTo(HttpStatusCode.NotFound)
                assertThat(body(resp)["error"].asText()).isEqualTo("not_found")
            }
        }
}
