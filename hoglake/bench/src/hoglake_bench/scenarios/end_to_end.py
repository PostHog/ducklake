"""Scenario 7: the realistic writer — real parquet through Table.append.

pyarrow tables go through the full client path: schema alignment,
parquet encode, S3 upload to MinIO, footer-stats extraction, commit.
Reports rows/s end to end and per-append latency.
"""

from __future__ import annotations

import argparse

import pyarrow as pa

from ..context import Bench
from ..runner import FailureGuard, run_loop
from ..stats import Metric
from .common import ScenarioReport, assert_row_tiling, check

E2E_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
    ]
)


def _batch(start: int, rows: int) -> pa.Table:
    ids = list(range(start, start + rows))
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([f"event-{i % 97}" for i in ids], pa.string()),
            "value": pa.array([(i % 1000) / 7.0 for i in ids], pa.float64()),
            "ts": pa.array(
                [1_700_000_000_000_000 + i for i in ids],
                pa.timestamp("us", tz="UTC"),
            ),
        }
    )


def run(bench: Bench, args: argparse.Namespace) -> ScenarioReport:
    batches = max(1, args.rows // args.batch_rows)
    warmup = 1 if batches > 1 else 0
    report = ScenarioReport(
        scenario="end-to-end-writer",
        params={
            "rows": args.rows,
            "batch_rows": args.batch_rows,
            "batches": batches,  # measured appends; warmup appends on top
            "warmup_batches": warmup,
            "warmup": warmup,
            "duration": args.duration,
            "url": args.url,
        },
    )
    bench.ensure_bucket()
    catalog = bench.new_catalog("e2e")
    ns = catalog.create_namespace("bench")
    table = ns.create_table("events", E2E_SCHEMA)
    guard = FailureGuard()

    payload_bytes = [0]

    def op(i: int) -> None:
        data = _batch(i * args.batch_rows, args.batch_rows)
        table.append(data, author="hoglake-bench", message=f"batch {i}")
        if i >= warmup:
            # arrow_mb_s: numerator and denominator must cover the SAME
            # measured window — warmup bytes stay out of both
            payload_bytes[0] += data.nbytes

    loop = run_loop(
        op, ops=batches, warmup=warmup, duration_s=args.duration, guard=guard
    )
    check(loop.errors == 0, f"{loop.errors} failed appends")
    appended = loop.recorder.count + loop.warmup.count
    measured_rows = loop.recorder.count * args.batch_rows
    report.add(
        Metric.from_recorder(
            "append.real_parquet",
            loop.recorder,
            loop.wall_s,
            rows_s=measured_rows / loop.wall_s if loop.wall_s else 0.0,
            rows=measured_rows,
            arrow_mb_s=(payload_bytes[0] / 1e6 / loop.wall_s) if loop.wall_s else 0.0,
        )
    )

    # correctness: server-side accounting matches what we wrote
    info = table.info()
    check(
        info.record_count == appended * args.batch_rows,
        f"table record_count={info.record_count}, wrote "
        f"{appended * args.batch_rows} rows",
    )
    check(
        info.file_count == appended,
        f"file_count={info.file_count}, appended {appended} files",
    )
    files = table.files()
    check(
        all(f.stats_state == "provided" for f in files),
        "footer-shipped stats did not register as provided",
    )
    assert_row_tiling(table, expected_rows=appended * args.batch_rows)
    return report


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--rows", type=int, default=100_000, help="total rows to write")
    p.add_argument("--batch-rows", type=int, default=10_000, help="rows per append")
