"""Live-server smoke: `hoglake-bench all --quick` must run green.

Skips cleanly when no server is reachable (same convention as the
pyhoglake integration suite). Exit code 2 (regression flags) is treated
as a failure on purpose: a smoke run on a healthy dev stack should not
trip the 1.5x ratio flags.
"""

import json

import pytest

from hoglake_bench.cli import main

pytestmark = pytest.mark.integration


def test_all_quick_smoke(live_server_url, tmp_path):
    results = tmp_path / "bench-results.jsonl"
    rc = main(
        [
            "all",
            "--quick",
            "--url",
            live_server_url,
            "--results",
            str(results),
        ]
    )
    assert rc == 0, f"hoglake-bench all --quick exited {rc}"

    lines = results.read_text().strip().splitlines()
    scenarios = {json.loads(line)["scenario"] for line in lines}
    assert scenarios == {
        "commit-throughput",
        "commit-contention",
        "delete-contention",
        "changefeed-scan",
        "expiry-throughput",
        "ddl-churn",
        "end-to-end-writer",
    }
    for line in lines:
        rec = json.loads(line)
        assert rec["metrics"], f"{rec['scenario']} recorded no metrics"
