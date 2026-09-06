package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.AlterTableRequestDto
import com.posthog.hoglake.api.CommitRequestDto
import com.posthog.hoglake.model.HoglakeException
import io.ktor.server.plugins.BadRequestException

/**
 * Fuzz target (fuzzing.md layer 4, target f): the wire parse path for the
 * two most structured request bodies — CommitRequestDto and the
 * polymorphic AlterTableRequestDto (`op`-discriminated) — over arbitrary
 * bytes, through a Jackson mapper configured exactly like App.module's
 * ContentNegotiation (kotlin module, JavaTimeModule, SNAKE_CASE).
 *
 * The two phases have different contracts, and conflating them produces
 * false findings:
 *
 *  - **readValue** runs inside ktor's ContentNegotiation, which wraps
 *    *any* converter throw into JsonConvertException/BadRequestException
 *    -> 400. Jackson's encoding detector can raise plain IOExceptions
 *    (e.g. CharConversionException on a byte run it reads as UTF-32);
 *    those are still 400s on the wire, so a parse throw is never a
 *    finding here.
 *  - **toModel** runs in the route handler, *outside* that wrapping. An
 *    exception escaping it surfaces as a 500 on hostile input unless it
 *    is one ErrorMapping turns into 4xx: [JacksonException],
 *    [BadRequestException] (AlterOpDto's unknown-op / missing-field), or
 *    [HoglakeException] (Validation -> 422, e.g. unknown enum literals).
 *
 * The end-to-end complement — hostile bytes really do get a 400 through
 * the whole ktor stack — is pinned in `WireParseErrorMappingTest`.
 */
class WireDtoParseFuzzTest {
    @FuzzTest(maxDuration = "120s")
    fun wireParseFailsOnlyWithMappedExceptions(data: ByteArray) {
        if (data.size > MAX_INPUT_BYTES) return

        parseOrNull { mapper.readValue<CommitRequestDto>(data) }?.let { dto ->
            try {
                dto.toModel()
            } catch (e: Exception) {
                checkAllowed("CommitRequestDto", e)
            }
        }

        parseOrNull { mapper.readValue<AlterTableRequestDto>(data) }?.let { dto ->
            try {
                dto.ops.forEach { it.toModel() }
            } catch (e: Exception) {
                checkAllowed("AlterTableRequestDto", e)
            }
        }
    }

    /** ContentNegotiation turns every parse failure into a 400; not a finding. */
    private fun <T> parseOrNull(parse: () -> T): T? =
        try {
            parse()
        } catch (_: Exception) {
            null
        }

    private fun checkAllowed(
        dto: String,
        e: Exception,
    ) {
        check(e is JacksonException || e is BadRequestException || e is HoglakeException) {
            "$dto parse escaped with unmapped ${e.javaClass.name}: ${e.message?.take(200)}"
        }
    }

    private companion object {
        const val MAX_INPUT_BYTES = 1 shl 20

        /** Mirror of App.module's ContentNegotiation jackson block (request side). */
        val mapper: JsonMapper =
            JsonMapper.builder()
                .addModule(kotlinModule())
                .addModule(JavaTimeModule())
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .build()
    }
}
