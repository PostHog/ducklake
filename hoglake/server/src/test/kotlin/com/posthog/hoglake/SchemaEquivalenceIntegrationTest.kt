package com.posthog.hoglake

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The migrations-vs-canonical-schema equivalence check (README.md,
 * schema/migrations mechanism, piece 3): applying every Flyway migration
 * must produce exactly the structure described by schema.sql. Drift in
 * either direction fails this test.
 *
 * Comparison is by catalog introspection with Postgres-normalized
 * definitions (pg_get_constraintdef / pg_get_indexdef), not by text
 * diffing SQL.
 */
@Tag("integration")
class SchemaEquivalenceIntegrationTest {
    @Test
    fun `folded migrations match canonical schema_sql`() {
        val canonical = Files.readString(Path.of("schema.sql"))
        PgTestSupport.freshDatabase().use { migrated ->
            PgTestSupport.freshDatabaseRaw(canonical).use { fromSchemaSql ->
                val a = describe(migrated.jdbi)
                val b = describe(fromSchemaSql.jdbi)
                assertThat(a).isEqualTo(b)
            }
        }
    }

    /** Stable structural fingerprint of the public schema. */
    private fun describe(jdbi: Jdbi): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            val out = mutableListOf<String>()
            out +=
                h.createQuery(
                    """
            SELECT 'column|' || table_name || '|' || column_name || '|' || data_type ||
                   '|' || is_nullable || '|' || coalesce(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name <> 'flyway_schema_history'
            """,
                ).mapTo(String::class.java).list()
            out +=
                h.createQuery(
                    """
            SELECT 'constraint|' || conrelid::regclass::text || '|' || conname || '|' ||
                   pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE connamespace = 'public'::regnamespace
              AND conrelid::regclass::text <> 'flyway_schema_history'
            """,
                ).mapTo(String::class.java).list()
            out +=
                h.createQuery(
                    """
            SELECT 'index|' || pg_get_indexdef(indexrelid)
            FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
            WHERE c.relnamespace = 'public'::regnamespace
              AND c.relname <> 'flyway_schema_history'
            """,
                ).mapTo(String::class.java).list()
            out.sorted()
        }
}
