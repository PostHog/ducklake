package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.Identifiers

/**
 * Fuzz target (fuzzing.md layer 4, target e): the identifier allowlists —
 * [Identifiers.validate] (namespace/table/view/column names, 422 policy)
 * and the RequestId header shape (observability/RequestId.kt).
 *
 * Contract under test:
 *  - validate never throws anything but [HoglakeException.Validation],
 *    whatever the string (astral-plane unicode, NULs, 10KB blobs);
 *  - its refusal message stays bounded (hostile names must not blow up
 *    log/response lines);
 *  - both regex decisions are STABLE (same input, same answer) and agree
 *    with a straight character-walk reference of the documented policy —
 *    a divergence means the regex does something the doc does not say.
 *
 * The RequestId shape regex is private to the plugin by design; the
 * harness reads it reflectively rather than widening main-source
 * visibility for a test.
 */
class IdentifiersFuzzTest {
    @FuzzTest(maxDuration = "60s")
    fun identifierDecisionsAreStableAndTyped(data: FuzzedDataProvider) {
        val name = data.consumeRemainingAsString()

        val first = validationError(name)
        val second = validationError(name)
        check((first == null) == (second == null)) { "unstable Identifiers.validate decision" }

        check((first == null) == referenceIdentifierPolicy(name)) {
            "Identifiers.validate disagrees with the documented policy for ${name.toDebug()}"
        }
        if (first != null) {
            check(first.length <= 256) { "unbounded validation error message (${first.length} chars)" }
        }

        val requestIdAccepted = REQUEST_ID_SHAPE.matches(name)
        check(requestIdAccepted == REQUEST_ID_SHAPE.matches(name)) { "unstable RequestId decision" }
        check(requestIdAccepted == referenceRequestIdPolicy(name)) {
            "RequestId shape disagrees with the documented policy for ${name.toDebug()}"
        }
    }

    /** null = accepted; message = the typed refusal. Any other throwable propagates as a finding. */
    private fun validationError(name: String): String? =
        try {
            Identifiers.validate("table", name)
            null
        } catch (e: HoglakeException.Validation) {
            e.message ?: ""
        }

    /** `^[A-Za-z_][A-Za-z0-9_-]{0,127}$` as a plain character walk. */
    private fun referenceIdentifierPolicy(name: String): Boolean {
        if (name.isEmpty() || name.length > 128) return false
        val head = name[0]
        if (!(head in 'A'..'Z' || head in 'a'..'z' || head == '_')) return false
        return name.drop(1).all {
            it in 'A'..'Z' || it in 'a'..'z' || it in '0'..'9' || it == '_' || it == '-'
        }
    }

    /** `^[A-Za-z0-9._-]{1,128}$` as a plain character walk. */
    private fun referenceRequestIdPolicy(value: String): Boolean {
        if (value.isEmpty() || value.length > 128) return false
        return value.all {
            it in 'A'..'Z' || it in 'a'..'z' || it in '0'..'9' || it == '.' || it == '_' || it == '-'
        }
    }

    private fun String.toDebug(): String =
        take(64).map { if (it.code in 32..126) it else "\\u%04x".format(it.code) }.joinToString("")

    private companion object {
        /** The plugin's private REQUEST_ID_SHAPE, read reflectively (see class doc). */
        val REQUEST_ID_SHAPE: Regex =
            Class.forName("com.posthog.hoglake.observability.RequestIdKt")
                .getDeclaredField("REQUEST_ID_SHAPE")
                .apply { isAccessible = true }
                .get(null) as Regex
    }
}
