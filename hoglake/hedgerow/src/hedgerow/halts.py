"""Halt conditions: fatal, non-retryable states where hedgerow must stop
LOUDLY (exit nonzero, precise message) instead of limping onward.

These encode viaduck lessons #3 (incarnation guard), #4 (never skip an
expired gap silently — the retention-clamp lesson) and #8 (append-only
mode cannot represent deletes). A halt is never caught-and-continued by
the daemon loop; it propagates out of ``run_forever`` and the CLI exits
nonzero.
"""

from __future__ import annotations


class HaltError(Exception):
    """Base for fatal daemon-stopping conditions. exit_code is what the
    CLI process exits with."""

    exit_code = 2


class IncarnationChangedError(HaltError):
    """Lesson #3: the table resolved by name no longer has the table_uuid
    pinned at startup (drop+recreate, or the table is gone). Continuing
    would replicate against a different table's history. HALT."""

    exit_code = 3


class FeedExpiredError(HaltError):
    """Lesson #4: changes() returned 410 — part of the requested snapshot
    range has been expired out from under the consumer. Skipping the gap
    silently is data loss (the retention-clamp lesson). HALT and carry
    the server's reconcile instructions."""

    exit_code = 4


class DeletesPresentError(HaltError):
    """Lesson #8: the change plan contains delete files (deletion
    vectors). hedgerow v1 is append-only and cannot represent deletions
    in the destination. HALT rather than replicate a wrong view."""

    exit_code = 5


class SchemaMismatchError(HaltError):
    """Lesson #6: the destination's columns do not project from the
    source (missing column or name/type mismatch). Refuse to start; the
    message carries the precise diff."""

    exit_code = 6


class SplitBrainError(HaltError):
    """The source catalog rejected our offset commit as a regression
    (409): some OTHER writer sharing this consumer_id has advanced the
    offset past our window. Adopting the foreign offset would silently
    skip rows never replicated to THIS destination — evidence of a
    consumer_id collision. HALT; never adopt a foreign offset."""

    exit_code = 7


class DataIntegrityError(HaltError):
    """A data file delivered a different number of rows than the change
    plan's server-side record_count. Committing the offset would cover
    rows that were never read or appended (short read: truncated or
    stale object-store response, reader bug, wrong file content). HALT
    before any offset movement."""

    exit_code = 8


class PersistentFailureError(HaltError):
    """Retrying cannot succeed (a permanent client error: validation,
    not-found, already-exists), or the transient-retry budget
    (``max_window_replays``) is exhausted. Each retry replays the whole
    window — duplicates per replay — so the amplification is capped by
    halting instead of retrying forever."""

    exit_code = 9
