package com.posthog.hoglake.persistence

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/** Pure-logic unit tests for the persistence helpers (no database). */
class PgHelpersTest {
    @Test
    fun `type params round-trip through jsonb serialization`() {
        val params: Map<String, Any?> = mapOf("precision" to 38, "scale" to 9, "note" to null)
        val json = Pg.toJson(params)!!
        val back = Pg.fromJson(json)!!
        assertThat(back["precision"]).isEqualTo(38)
        assertThat(back["scale"]).isEqualTo(9)
        assertThat(back).containsKey("note")
        assertThat(back["note"]).isNull()
    }

    @Test
    fun `null type params stay null in both directions`() {
        assertThat(Pg.toJson(null)).isNull()
        assertThat(Pg.fromJson(null)).isNull()
    }

    @Test
    fun `nested type params survive`() {
        val params: Map<String, Any?> = mapOf("element" to mapOf("type" to "long"))
        val back = Pg.fromJson(Pg.toJson(params))!!
        assertThat(back["element"]).isEqualTo(mapOf("type" to "long"))
    }

    @Test
    fun `catalog commit lock uses the agreed classid`() {
        // Wave-2 code hardcodes SQL against this discriminator; a silent
        // change here would split the lock space.
        assertThat(Locks.CATALOG_COMMIT_LOCK_CLASS).isEqualTo(4740871)
    }
}
