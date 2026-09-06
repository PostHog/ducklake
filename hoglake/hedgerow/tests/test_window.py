"""Window math (bounded windows; commit-through-complete-windows)."""

import pytest

from hedgerow.window import Window, plan_window


def test_caught_up_returns_none():
    assert plan_window(committed=10, head=10, max_snapshot_window=100) is None


def test_ahead_of_head_returns_none():
    # e.g. start_snapshot configured past head: nothing to do, no negative window
    assert plan_window(committed=15, head=10, max_snapshot_window=100) is None


def test_small_backlog_goes_to_head():
    w = plan_window(committed=10, head=13, max_snapshot_window=100)
    assert w == Window(from_snapshot=10, to_snapshot=13, head_snapshot=13)
    assert not w.backlog_remains


def test_large_backlog_clamped_to_max_window():
    w = plan_window(committed=0, head=5000, max_snapshot_window=1000)
    assert w.from_snapshot == 0
    assert w.to_snapshot == 1000
    assert w.backlog_remains


def test_exact_window_boundary():
    w = plan_window(committed=0, head=1000, max_snapshot_window=1000)
    assert w.to_snapshot == 1000
    assert not w.backlog_remains


def test_window_of_one():
    w = plan_window(committed=7, head=100, max_snapshot_window=1)
    assert (w.from_snapshot, w.to_snapshot) == (7, 8)
    assert w.backlog_remains


def test_invalid_inputs():
    with pytest.raises(ValueError):
        plan_window(committed=0, head=1, max_snapshot_window=0)
    with pytest.raises(ValueError):
        plan_window(committed=-1, head=1, max_snapshot_window=10)
