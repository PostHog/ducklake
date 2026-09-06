package com.posthog.hoglake.service

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * View lifecycle: create/get/list/drop as DDL commits with typed
 * view_created/view_dropped change rows, verbatim SQL storage, and
 * live-name uniqueness. Each test builds its own namespace so the
 * catalog timeline stays interpretable per test.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ViewServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val views = ViewService(db.jdbi)

    private val cat = "view-cat"

    init {
        catalogs.createCatalog(cat, "s3://bucket/views") // S0
    }

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `create get list drop lifecycle with typed change rows`() {
        catalogs.createNamespace(cat, "life")
        val headBefore = catalogs.getCatalog(cat).headSnapshotId

        val sql = "SELECT  id ,\n  count(*)\nFROM t GROUP BY 1  -- stored verbatim"
        val v = views.create(cat, "life", "daily", sql)
        assertThat(v.name).isEqualTo("daily")
        assertThat(v.namespace).isEqualTo("life")
        assertThat(v.dialect).isEqualTo("trino")
        // Verbatim: whitespace, casing, comments — untouched.
        assertThat(v.sql).isEqualTo(sql)

        // The create was a DDL commit: head advanced by one.
        val createdAt = catalogs.getCatalog(cat).headSnapshotId
        assertThat(createdAt).isEqualTo(headBefore + 1)

        // get returns the same row, including the identity.
        val got = views.get(cat, "life", "daily")
        assertThat(got).isEqualTo(v)

        // list is live views ordered by name.
        views.create(cat, "life", "a-first", "SELECT 1", dialect = "duckdb")
        assertThat(views.list(cat, "life").map { it.name }).containsExactly("a-first", "daily")
        assertThat(views.list(cat, "life").first().dialect).isEqualTo("duckdb")

        // drop produces a snapshot and removes it from head reads.
        val drop = views.drop(cat, "life", "daily")
        assertThat(drop.snapshotId).isEqualTo(catalogs.getCatalog(cat).headSnapshotId)
        assertThat(views.list(cat, "life").map { it.name }).containsExactly("a-first")
        assertThatThrownBy { views.get(cat, "life", "daily") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // The snapshot timeline shows the typed view changes with the view id.
        val (snapshots, _) = catalogs.listSnapshots(cat, after = headBefore, limit = 100)
        val changes = snapshots.flatMap { it.changes }
        assertThat(changes.map { it.kind }).containsExactly(
            ChangeKind.VIEW_CREATED,
            ChangeKind.VIEW_CREATED,
            ChangeKind.VIEW_DROPPED,
        )
        val createdChange = changes.first()
        val droppedChange = changes.last()
        assertThat(createdChange.objectId).isEqualTo(v.viewId)
        assertThat(droppedChange.objectId).isEqualTo(v.viewId)
    }

    @Test
    fun `duplicate live name is AlreadyExists, recreate after drop is a new identity`() {
        catalogs.createNamespace(cat, "dup")
        val first = views.create(cat, "dup", "v", "SELECT 1")
        assertThatThrownBy { views.create(cat, "dup", "v", "SELECT 2") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        views.drop(cat, "dup", "v")
        val second = views.create(cat, "dup", "v", "SELECT 2")
        assertThat(second.viewUuid).isNotEqualTo(first.viewUuid)
        assertThat(second.viewId).isNotEqualTo(first.viewId)
        assertThat(views.get(cat, "dup", "v").sql).isEqualTo("SELECT 2")
    }

    @Test
    fun `same name is independent across namespaces`() {
        catalogs.createNamespace(cat, "ns-a")
        catalogs.createNamespace(cat, "ns-b")
        views.create(cat, "ns-a", "shared", "SELECT 'a'")
        val b = views.create(cat, "ns-b", "shared", "SELECT 'b'")
        assertThat(b.sql).isEqualTo("SELECT 'b'")
        assertThat(views.list(cat, "ns-a")).hasSize(1)
        assertThat(views.list(cat, "ns-b")).hasSize(1)
    }

    @Test
    fun `validation and resolution failures`() {
        catalogs.createNamespace(cat, "bad")
        assertThatThrownBy { views.create(cat, "bad", "", "SELECT 1") }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { views.create(cat, "bad", "v", "   ") }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { views.create(cat, "nope", "v", "SELECT 1") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { views.create("nope", "bad", "v", "SELECT 1") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { views.get(cat, "bad", "ghost") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { views.drop(cat, "bad", "ghost") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // A failed create leaves no snapshot behind (validation precedes
        // allocation inside the transaction).
        val head = catalogs.getCatalog(cat).headSnapshotId
        assertThatThrownBy { views.create(cat, "bad", "v", "") }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(head)
    }
}
