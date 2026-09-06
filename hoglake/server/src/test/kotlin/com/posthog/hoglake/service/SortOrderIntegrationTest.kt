package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * Sort-order DDL lifecycle (SetSortOrder mirrors SetPartitionSpec):
 * versioned spec rows, sort_id allocation, getTable exposure at every
 * snapshot, validation against the evolving in-request column state,
 * and the drop-column guard.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SortOrderIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val alter = AlterService(db.jdbi)
    private val catalogs = CatalogService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /** Fresh catalog+ns+table: id(long,1), name(string,2), score(double,3). */
    private fun fixture(): String {
        val cat = "sort-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )
        return cat
    }

    private fun sortBy(vararg fields: SortFieldDef) = AlterOp.SetSortOrder(fields.toList())

    private val byId = SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)
    private val byScoreDesc = SortFieldDef(3, SortDirection.DESC, NullOrder.NULLS_FIRST)

    @Test
    fun `set sort order exposes the spec on the alter response and getTable`() {
        val cat = fixture()
        val info = alter.alterTable(cat, "ns", "t", listOf(sortBy(byScoreDesc, byId)))
        assertThat(info.sortSpec).isNotNull
        assertThat(info.sortSpec!!.sortId).isEqualTo(1)
        assertThat(info.sortSpec!!.fields).containsExactly(byScoreDesc, byId)

        val got = catalogs.getTable(cat, "ns", "t")
        assertThat(got.sortSpec).isEqualTo(info.sortSpec)
    }

    @Test
    fun `an unsorted table has a null sort spec everywhere`() {
        val cat = fixture()
        assertThat(catalogs.getTable(cat, "ns", "t").sortSpec).isNull()
    }

    @Test
    fun `replacing the sort order retires the old spec and mints sort_id max plus one`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(sortBy(byId)))
        val beforeReplace = catalogs.getCatalog(cat).headSnapshotId
        val info = alter.alterTable(cat, "ns", "t", listOf(sortBy(byScoreDesc)))
        assertThat(info.sortSpec!!.sortId).isEqualTo(2)
        assertThat(info.sortSpec!!.fields).containsExactly(byScoreDesc)

        // Time travel: the old spec is still visible at the old snapshot.
        val old = catalogs.getTable(cat, "ns", "t", snapshot = beforeReplace)
        assertThat(old.sortSpec!!.sortId).isEqualTo(1)
        assertThat(old.sortSpec!!.fields).containsExactly(byId)
    }

    @Test
    fun `empty sort_fields clears the sort order`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(sortBy(byId)))
        val cleared = alter.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(emptyList())))
        assertThat(cleared.sortSpec).isNull()
        assertThat(catalogs.getTable(cat, "ns", "t").sortSpec).isNull()
    }

    @Test
    fun `sort source must be a live column`() {
        val cat = fixture()
        assertThatThrownBy {
            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(sortBy(SortFieldDef(99, SortDirection.ASC, NullOrder.NULLS_LAST))),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("field_id 99")
    }

    @Test
    fun `duplicate sort source fields are rejected`() {
        val cat = fixture()
        assertThatThrownBy {
            alter.alterTable(cat, "ns", "t", listOf(sortBy(byId, byId.copy(direction = SortDirection.DESC))))
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("duplicate sort source")
    }

    @Test
    fun `dropping a column referenced by the live sort order is rejected`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(sortBy(byScoreDesc)))
        assertThatThrownBy {
            alter.alterTable(cat, "ns", "t", listOf(AlterOp.DropColumn("score")))
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("live sort order")
        // Clearing the order first unblocks the drop.
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetSortOrder(emptyList()), AlterOp.DropColumn("score")),
        )
        assertThat(catalogs.getTable(cat, "ns", "t").columns.map { it.def.name })
            .containsExactly("id", "name")
    }

    @Test
    fun `same-request churn validates against the evolving state`() {
        val cat = fixture()
        // Add a column and sort by it in one atomic DDL commit.
        val info =
            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("ts", ColType.TIMESTAMPTZ)),
                    sortBy(SortFieldDef(4, SortDirection.ASC, NullOrder.NULLS_FIRST)),
                ),
            )
        assertThat(info.sortSpec!!.fields.single().sourceFieldId).isEqualTo(4)

        // Sorting by a column an earlier op in the SAME request dropped fails.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(
                    AlterOp.SetSortOrder(emptyList()),
                    AlterOp.DropColumn("name"),
                    sortBy(SortFieldDef(2, SortDirection.ASC, NullOrder.NULLS_LAST)),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // And the failed request rolled back atomically: prior spec intact.
        assertThat(catalogs.getTable(cat, "ns", "t").sortSpec!!.fields.single().sourceFieldId)
            .isEqualTo(4)
    }

    @Test
    fun `setting a sort order mints exactly one snapshot with one table_altered change`() {
        val cat = fixture()
        val before = catalogs.getCatalog(cat).headSnapshotId
        alter.alterTable(cat, "ns", "t", listOf(sortBy(byId)))
        val after = catalogs.getCatalog(cat).headSnapshotId
        assertThat(after).isEqualTo(before + 1)
        val (page, _) = catalogs.listSnapshots(cat, after - 1, 10)
        val changes = page.single().changes
        assertThat(changes).hasSize(1)
        assertThat(changes.single().kind.wire).isEqualTo("table_altered")
    }
}
