package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

/**
 * Identifier policy for namespace, table, view, and column names
 * (decision 2026-09-05, after two reviews flagged that everything but
 * catalog names accepted slashes, markup, and 10KB blobs verbatim):
 * `^[A-Za-z_][A-Za-z0-9_-]{0,127}$`, enforced here (Validation -> 422)
 * and by CHECK constraints on hog_namespace.name, hog_table_version.name,
 * hog_view.name, and hog_column.name (V1__init.sql / schema.sql).
 *
 * Catalog names keep their own stricter CHECK (lowercase, 63 chars);
 * author/message/view SQL stay free-text by design.
 */
object Identifiers {
    const val PATTERN = "^[A-Za-z_][A-Za-z0-9_-]{0,127}$"

    private val regex = Regex(PATTERN)

    /** Validate [name] as a [kind] identifier; violation -> Validation (422). */
    fun validate(
        kind: String,
        name: String,
    ) {
        if (!regex.matches(name)) {
            // Hostile names can be huge; keep the error line bounded.
            val shown = if (name.length > 64) name.take(61) + "..." else name
            throw HoglakeException.Validation(
                "invalid $kind name '$shown' (must match $PATTERN)",
            )
        }
    }
}
