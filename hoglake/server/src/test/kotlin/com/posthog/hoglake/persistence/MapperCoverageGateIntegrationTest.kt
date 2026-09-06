package com.posthog.hoglake.persistence

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The mapper-coverage gate: for every hog_* table, the live schema's
 * column set (information_schema of the migrated Testcontainers
 * database) must EQUAL the column set the mapping layer declares in
 * [HogSchemaColumns]. Adding a column to V1__init.sql/schema.sql
 * without touching the mapping layer fails HERE with the table and the
 * missing column named — the LifecycleCatalog-vs-CatalogInfo drift
 * class (a second hog_catalog mapping quietly missing newer columns)
 * can no longer happen silently.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MapperCoverageGateIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    @AfterAll
    fun tearDown() = db.close()

    private fun liveSchema(): Map<String, Set<String>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name LIKE 'hog\_%'
                """,
            )
                .map { rs, _ -> rs.getString("table_name") to rs.getString("column_name") }
                .list()
                .groupBy({ it.first }, { it.second })
                .mapValues { (_, cols) -> cols.toSet() }
        }

    @Test
    fun `every hog_ table's live columns equal the mapping layer's declared set`() {
        val live = liveSchema()
        val declared = HogSchemaColumns.TABLES

        assertThat(declared.keys)
            .describedAs(
                "hog_* table set drifted: tables in the schema but not declared in " +
                    "HogSchemaColumns (add an entry + review its mappers), or declared " +
                    "but gone from the schema",
            )
            .isEqualTo(live.keys)

        for ((table, liveColumns) in live) {
            val declaredColumns = declared.getValue(table)
            val undeclared = liveColumns - declaredColumns
            val phantom = declaredColumns - liveColumns
            assertThat(undeclared)
                .describedAs(
                    "table '%s': column(s) %s exist in the live schema but the mapping " +
                        "layer never declared them — update the row mapper(s) named in " +
                        "HogSchemaColumns for this table, then add the column(s) there",
                    table,
                    undeclared,
                )
                .isEmpty()
            assertThat(phantom)
                .describedAs(
                    "table '%s': column(s) %s are declared in HogSchemaColumns but do " +
                        "not exist in the live schema — stale declaration",
                    table,
                    phantom,
                )
                .isEmpty()
        }
    }
}
