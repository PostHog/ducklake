package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Options surface semantics — especially the PATCH tri-state on
 * snapshot_retention_seconds: absent = unchanged, explicit null =
 * expiry disabled, value = set (positive only).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OptionsServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val svc = OptionsService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `new catalog defaults - no retention, consumer floor on, earliest 0`() {
        catalogs.createCatalog("opt-defaults", "s3://b/p")
        val opts = svc.get("opt-defaults")
        assertThat(opts.snapshotRetentionSeconds).isNull()
        assertThat(opts.consumerFloor).isTrue()
        assertThat(opts.earliestSnapshotId).isEqualTo(0)
    }

    @Test
    fun `patch value sets retention and leaves the absent field untouched`() {
        catalogs.createCatalog("opt-set", "s3://b/p")
        val patched = svc.patch("opt-set", PatchField.Set(3_600L), consumerFloor = null)
        assertThat(patched.snapshotRetentionSeconds).isEqualTo(3_600L)
        assertThat(patched.consumerFloor).isTrue() // absent -> unchanged

        // Now the reverse: retention absent, floor set.
        val patched2 = svc.patch("opt-set", PatchField.Absent, consumerFloor = false)
        assertThat(patched2.snapshotRetentionSeconds).isEqualTo(3_600L) // absent -> unchanged
        assertThat(patched2.consumerFloor).isFalse()

        // Persisted, not just echoed.
        val readBack = svc.get("opt-set")
        assertThat(readBack.snapshotRetentionSeconds).isEqualTo(3_600L)
        assertThat(readBack.consumerFloor).isFalse()
    }

    @Test
    fun `patch explicit null disables expiry`() {
        catalogs.createCatalog("opt-null", "s3://b/p")
        svc.patch("opt-null", PatchField.Set(60L), consumerFloor = null)
        assertThat(svc.get("opt-null").snapshotRetentionSeconds).isEqualTo(60L)

        val disabled = svc.patch("opt-null", PatchField.Set(null), consumerFloor = null)
        assertThat(disabled.snapshotRetentionSeconds).isNull()
        assertThat(svc.get("opt-null").snapshotRetentionSeconds).isNull()
    }

    @Test
    fun `patch with both fields absent is a no-op read`() {
        catalogs.createCatalog("opt-noop", "s3://b/p")
        svc.patch("opt-noop", PatchField.Set(120L), consumerFloor = false)
        val opts = svc.patch("opt-noop", PatchField.Absent, consumerFloor = null)
        assertThat(opts.snapshotRetentionSeconds).isEqualTo(120L)
        assertThat(opts.consumerFloor).isFalse()
    }

    @Test
    fun `non-positive retention is rejected without touching the row`() {
        catalogs.createCatalog("opt-invalid", "s3://b/p")
        svc.patch("opt-invalid", PatchField.Set(60L), consumerFloor = null)

        for (bad in listOf(0L, -1L)) {
            assertThatThrownBy { svc.patch("opt-invalid", PatchField.Set(bad), consumerFloor = null) }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(svc.get("opt-invalid").snapshotRetentionSeconds).isEqualTo(60L)
    }

    @Test
    fun `unknown catalog is NotFound for both get and patch`() {
        assertThatThrownBy { svc.get("opt-nope") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.patch("opt-nope", PatchField.Set(60L), consumerFloor = null) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }
}
