package com.posthog.hoglake.service

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/** Snapshot pagination and consumer-offset semantics. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SnapshotsAndOffsetsIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)

    @AfterAll
    fun tearDown() = db.close()

    // ---- snapshot pagination ---------------------------------------------

    @Test
    fun `listSnapshots pages in order with hasMore`() {
        svc.createCatalog("page-cat", "s3://bucket/p") // S0
        for (i in 1..5) svc.createNamespace("page-cat", "ns$i") // S1..S5

        val (p1, more1) = svc.listSnapshots("page-cat", after = 0, limit = 2)
        assertThat(p1.map { it.snapshotId }).containsExactly(1L, 2L)
        assertThat(more1).isTrue()

        val (p2, more2) = svc.listSnapshots("page-cat", after = 2, limit = 2)
        assertThat(p2.map { it.snapshotId }).containsExactly(3L, 4L)
        assertThat(more2).isTrue()

        val (p3, more3) = svc.listSnapshots("page-cat", after = 4, limit = 2)
        assertThat(p3.map { it.snapshotId }).containsExactly(5L)
        assertThat(more3).isFalse()

        val (p4, more4) = svc.listSnapshots("page-cat", after = 5, limit = 2)
        assertThat(p4).isEmpty()
        assertThat(more4).isFalse()

        // Exact-boundary page: limit == remaining -> no phantom hasMore.
        val (exact, moreExact) = svc.listSnapshots("page-cat", after = 3, limit = 2)
        assertThat(exact.map { it.snapshotId }).containsExactly(4L, 5L)
        assertThat(moreExact).isFalse()

        // Changes are populated per snapshot.
        assertThat(p1[0].changes).hasSize(1)
        assertThat(p1[0].changes[0].kind).isEqualTo(ChangeKind.NAMESPACE_CREATED)
        assertThat(p1[0].changes[0].objectId).isEqualTo(1L)
        // Snapshot times and schema versions ride along.
        assertThat(p1[0].schemaVersion).isEqualTo(1)
        assertThat(p1[0].snapshotTime).isNotNull()
    }

    @Test
    fun `listSnapshots pages descending with before`() {
        svc.createCatalog("before-cat", "s3://bucket/b") // S0
        for (i in 1..5) svc.createNamespace("before-cat", "ns$i") // S1..S5
        val head = svc.getCatalog("before-cat").headSnapshotId
        assertThat(head).isEqualTo(5)

        // Newest-first walk starting at head + 1.
        val (p1, more1) = svc.listSnapshots("before-cat", after = 0, limit = 2, before = head + 1)
        assertThat(p1.map { it.snapshotId }).containsExactly(5L, 4L)
        assertThat(more1).isTrue()
        // Changes ride along in DESC pages too.
        assertThat(p1[0].changes.single().kind).isEqualTo(ChangeKind.NAMESPACE_CREATED)

        val (p2, more2) = svc.listSnapshots("before-cat", after = 0, limit = 2, before = 4)
        assertThat(p2.map { it.snapshotId }).containsExactly(3L, 2L)
        assertThat(more2).isTrue()

        // Exact-boundary page: the remaining 1, 0 fill the page with
        // nothing below -> no phantom hasMore.
        val (p3, more3) = svc.listSnapshots("before-cat", after = 0, limit = 2, before = 2)
        assertThat(p3.map { it.snapshotId }).containsExactly(1L, 0L)
        assertThat(more3).isFalse()

        // before=1 yields exactly snapshot 0.
        val (p4, more4) = svc.listSnapshots("before-cat", after = 0, limit = 2, before = 1)
        assertThat(p4.map { it.snapshotId }).containsExactly(0L)
        assertThat(more4).isFalse()

        // before=0: nothing below the first snapshot.
        val (p5, more5) = svc.listSnapshots("before-cat", after = 0, limit = 2, before = 0)
        assertThat(p5).isEmpty()
        assertThat(more5).isFalse()

        // hasMore edge: limit exactly covers everything below before.
        val (all, moreAll) = svc.listSnapshots("before-cat", after = 0, limit = 6, before = head + 1)
        assertThat(all.map { it.snapshotId }).containsExactly(5L, 4L, 3L, 2L, 1L, 0L)
        assertThat(moreAll).isFalse()
    }

    @Test
    fun `before and a non-zero after are mutually exclusive`() {
        svc.createCatalog("cursor-cat", "s3://bucket/c")
        assertThatThrownBy { svc.listSnapshots("cursor-cat", after = 3, limit = 10, before = 9) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("mutually exclusive")
        // after = 0 (the default) alongside before is fine.
        val (page, _) = svc.listSnapshots("cursor-cat", after = 0, limit = 10, before = 1)
        assertThat(page.map { it.snapshotId }).containsExactly(0L)
    }

    @Test
    fun `listSnapshots validates limit and catalog`() {
        svc.createCatalog("page-val-cat", "s3://bucket/pv")
        assertThatThrownBy { svc.listSnapshots("page-val-cat", after = 0, limit = 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.listSnapshots("nope", after = 0, limit = 10) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    // ---- consumer offsets ------------------------------------------------

    @Test
    fun `offsets are monotonic per (consumer, table)`() {
        svc.createCatalog("off-cat", "s3://bucket/o")
        svc.createNamespace("off-cat", "ns") // S1
        val t = svc.createTable("off-cat", "ns", "t", listOf(idCol)) // S2

        val first = svc.commitOffset("off-cat", "viaduck", t.tableUuid, 1)
        assertThat(first.consumerId).isEqualTo("viaduck")
        assertThat(first.tableUuid).isEqualTo(t.tableUuid)
        assertThat(first.committedSnapshot).isEqualTo(1)

        // Advance: fine.
        assertThat(svc.commitOffset("off-cat", "viaduck", t.tableUuid, 2).committedSnapshot)
            .isEqualTo(2)
        // Re-commit the same offset: idempotent accept.
        assertThat(svc.commitOffset("off-cat", "viaduck", t.tableUuid, 2).committedSnapshot)
            .isEqualTo(2)
        // Regression: rejected, stored offset untouched.
        assertThatThrownBy { svc.commitOffset("off-cat", "viaduck", t.tableUuid, 1) }
            .isInstanceOf(HoglakeException.OffsetRegression::class.java)
        assertThat(svc.listOffsets("off-cat", "viaduck").single().committedSnapshot)
            .isEqualTo(2)
    }

    @Test
    fun `offsets outside the snapshot range are validation errors`() {
        svc.createCatalog("off-range-cat", "s3://bucket/or")
        svc.createNamespace("off-range-cat", "ns")
        val t = svc.createTable("off-range-cat", "ns", "t", listOf(idCol))
        val head = svc.getCatalog("off-range-cat").headSnapshotId

        assertThatThrownBy { svc.commitOffset("off-range-cat", "c", t.tableUuid, head + 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.commitOffset("off-range-cat", "c", t.tableUuid, -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        // Head itself is committable.
        assertThat(svc.commitOffset("off-range-cat", "c", t.tableUuid, head).committedSnapshot)
            .isEqualTo(head)
    }

    @Test
    fun `listOffsets is scoped to the consumer`() {
        svc.createCatalog("off-list-cat", "s3://bucket/ol")
        svc.createNamespace("off-list-cat", "ns")
        val t1 = svc.createTable("off-list-cat", "ns", "t1", listOf(idCol))
        val t2 = svc.createTable("off-list-cat", "ns", "t2", listOf(idCol))

        svc.commitOffset("off-list-cat", "alpha", t1.tableUuid, 2)
        svc.commitOffset("off-list-cat", "alpha", t2.tableUuid, 3)
        svc.commitOffset("off-list-cat", "beta", t1.tableUuid, 1)

        val alpha = svc.listOffsets("off-list-cat", "alpha")
        assertThat(alpha).hasSize(2)
        assertThat(alpha.map { it.tableUuid })
            .containsExactlyInAnyOrder(t1.tableUuid, t2.tableUuid)
        assertThat(svc.listOffsets("off-list-cat", "beta")).hasSize(1)
        assertThat(svc.listOffsets("off-list-cat", "nobody")).isEmpty()
    }

    @Test
    fun `offset bookkeeping survives a table drop (uuid-keyed)`() {
        svc.createCatalog("off-drop-cat", "s3://bucket/od")
        svc.createNamespace("off-drop-cat", "ns")
        val t = svc.createTable("off-drop-cat", "ns", "t", listOf(idCol))
        svc.commitOffset("off-drop-cat", "c", t.tableUuid, 2)
        svc.dropTable("off-drop-cat", "ns", "t")
        val recreated = svc.createTable("off-drop-cat", "ns", "t", listOf(idCol))

        // The old incarnation's offset row is still there, keyed to the
        // old uuid; the new incarnation starts clean.
        val offsets = svc.listOffsets("off-drop-cat", "c")
        assertThat(offsets.single().tableUuid).isEqualTo(t.tableUuid)
        assertThat(offsets.single().tableUuid).isNotEqualTo(recreated.tableUuid)
    }

    @Test
    fun `getOffset returns the single stored row or 404s`() {
        svc.createCatalog("off-get-cat", "s3://bucket/og")
        svc.createNamespace("off-get-cat", "ns")
        val t1 = svc.createTable("off-get-cat", "ns", "t1", listOf(idCol))
        val t2 = svc.createTable("off-get-cat", "ns", "t2", listOf(idCol))
        svc.commitOffset("off-get-cat", "c", t1.tableUuid, 2)

        val got = svc.getOffset("off-get-cat", "c", t1.tableUuid)
        assertThat(got.consumerId).isEqualTo("c")
        assertThat(got.tableUuid).isEqualTo(t1.tableUuid)
        assertThat(got.committedSnapshot).isEqualTo(2)

        // Known table, no offset stored for it -> NotFound.
        assertThatThrownBy { svc.getOffset("off-get-cat", "c", t2.tableUuid) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // Garbage uuid (never a table here) -> clean NotFound, same shape.
        assertThatThrownBy { svc.getOffset("off-get-cat", "c", UUID.randomUUID()) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // Unknown consumer -> NotFound.
        assertThatThrownBy { svc.getOffset("off-get-cat", "nobody", t1.tableUuid) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // Unknown catalog -> NotFound.
        assertThatThrownBy { svc.getOffset("nope", "c", t1.tableUuid) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `offset errors for unknown catalog and oversized consumer id`() {
        assertThatThrownBy { svc.commitOffset("nope", "c", UUID.randomUUID(), 0) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.listOffsets("nope", "c") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        svc.createCatalog("off-err-cat", "s3://bucket/oe")
        assertThatThrownBy {
            svc.commitOffset("off-err-cat", "x".repeat(129), UUID.randomUUID(), 0)
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }
}
