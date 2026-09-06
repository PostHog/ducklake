"""Window math: which snapshot range does the next cycle cover?

Pure function, unit-tested in isolation. The window is
``(committed, min(head, committed + max_snapshot_window)]`` — bounded so
a lagging consumer catches up in bounded bites (bounded memory,
lesson #5) and the offset only ever advances to the end of a fully
applied window (commit-through-complete-windows, lesson #2).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Window:
    from_snapshot: int  # exclusive
    to_snapshot: int  # inclusive
    head_snapshot: int

    @property
    def backlog_remains(self) -> bool:
        """True when the window was clamped short of head — more work is
        immediately available after this window commits."""
        return self.to_snapshot < self.head_snapshot


def plan_window(committed: int, head: int, max_snapshot_window: int) -> Window | None:
    """The next window after ``committed`` given catalog ``head``, or
    None when fully caught up (nothing to do this cycle)."""
    if max_snapshot_window < 1:
        raise ValueError(f"max_snapshot_window must be >= 1, got {max_snapshot_window}")
    if committed < 0:
        raise ValueError(f"committed offset must be >= 0, got {committed}")
    if head <= committed:
        return None
    to = min(head, committed + max_snapshot_window)
    return Window(from_snapshot=committed, to_snapshot=to, head_snapshot=head)
