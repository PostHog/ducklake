"""hoglake-bench: the stress/benchmark CLI for the hoglake control plane.

Every scenario prints one line per metric (name, ops, wall_s, rate_s,
p50/p95/p99/max) and appends a JSON line to bench-results.jsonl so runs
compare over time. Regression ratios above 1.5x are flagged loudly on
stderr and make the exit code nonzero.

Exit codes (severity-resolved across `all`; a later scenario can never
demote an earlier one — precedence 4 > 2 > 5 > 3 > 0):

- 0: everything ran, no flags
- 2: at least one regression flag
- 3: abort — server unresponsive, or a guarded stage had insufficient
  samples for a trustworthy ratio
- 4: invariant violation (the numbers are not trustworthy)
- 5: unexpected exception (harness bug)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from typing import Any

import httpx

from . import seed as seed_task
from .context import Bench, BenchConfig
from .runner import BenchAbort, InvariantViolation
from .scenarios import (
    changefeed_scan,
    commit_contention,
    commit_throughput,
    ddl_churn,
    delete_contention,
    end_to_end,
    expiry_throughput,
)
from .scenarios.common import ScenarioReport

SCENARIOS: dict[str, Any] = {
    "commit-throughput": commit_throughput,
    "commit-contention": commit_contention,
    "delete-contention": delete_contention,
    "changefeed-scan": changefeed_scan,
    "expiry-throughput": expiry_throughput,
    "ddl-churn": ddl_churn,
    "end-to-end-writer": end_to_end,
}

# Per-scenario argument overrides for `all --quick` (a ~2-3 minute smoke
# profile) and `all --full` (the real numbers).
QUICK_PROFILE: dict[str, dict[str, Any]] = {
    "commit-throughput": {
        "preseed_snapshots": [0, 1000],
        "files_per_commit": [1, 10],
        "ops": 40,
        "tables": 150,
    },
    "commit-contention": {"writers": [1, 4, 8], "ops": 25},
    "delete-contention": {
        "writers": 4,
        "files_per_writer": 3,
        "rounds": 8,
        "hotfile_ops": 32,
    },
    "changefeed-scan": {
        "stages": [500, 2000],
        "windows": [10, 100, 1000],
        "reps": 5,
        "offset_commits": 100,
    },
    "expiry-throughput": {"snapshots": 1500, "batch": 500, "objects": 50},
    "ddl-churn": {"tables": 100, "ops": 30},
    "end-to-end-writer": {"rows": 20_000, "batch_rows": 5_000},
}
FULL_PROFILE: dict[str, dict[str, Any]] = {name: {} for name in SCENARIOS}


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--url",
        default=os.environ.get("HOGLAKE_URL", "http://localhost:8080"),
        help="hoglake server (env HOGLAKE_URL)",
    )
    p.add_argument(
        "--s3-endpoint",
        default=os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:19000"),
    )
    p.add_argument(
        "--s3-access-key",
        default=os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake"),
    )
    p.add_argument(
        "--s3-secret-key",
        default=os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123"),
    )
    p.add_argument("--bucket", default="hoglake-bench")
    p.add_argument(
        "--results",
        default="bench-results.jsonl",
        help="JSONL file to append run records to",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=None,
        help="cap (seconds) on each measured phase",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=3,
        help="unmeasured warmup iterations per loop",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hoglake-bench",
        description="stress-test/benchmark harness for the hoglake control plane",
    )
    sub = parser.add_subparsers(dest="scenario", required=True)
    for name, mod in SCENARIOS.items():
        p = sub.add_parser(name, help=(mod.__doc__ or "").strip().splitlines()[0])
        _add_common(p)
        mod.add_args(p)
    # `seed` is a task, not a scenario: it is deliberately NOT in
    # SCENARIOS (so `all` never runs it) and journals no metrics.
    p_seed = sub.add_parser(
        "seed",
        help="populate a catalog with a realistic fake data warehouse",
    )
    _add_common(p_seed)
    seed_task.add_args(p_seed)
    p_all = sub.add_parser("all", help="run every scenario")
    _add_common(p_all)
    profile = p_all.add_mutually_exclusive_group()
    profile.add_argument(
        "--quick",
        action="store_true",
        help="fast smoke profile (~2-3 min total; the default)",
    )
    profile.add_argument(
        "--full", action="store_true", help="full-size runs (the real numbers)"
    )
    return parser


def check_server(url: str) -> None:
    try:
        r = httpx.get(url.rstrip("/") + "/healthz", timeout=5.0)
        r.raise_for_status()
    except Exception as exc:
        raise BenchAbort(
            f"no healthy hoglake server at {url} ({exc}); start one with "
            "`docker compose up -d && flox activate -- gradle run` in "
            "../server, or point --url/HOGLAKE_URL elsewhere"
        ) from exc


# Exit codes, in severity-precedence order (first present wins across
# an `all` run): invariant violation > regression > unexpected error >
# abort/insufficient-samples > ok. A later scenario's failure can never
# demote an earlier scenario's regression flag.
EXIT_OK = 0
EXIT_REGRESSION = 2
EXIT_ABORT = 3
EXIT_INVARIANT = 4
EXIT_ERROR = 5
EXIT_PRECEDENCE = (EXIT_INVARIANT, EXIT_REGRESSION, EXIT_ERROR, EXIT_ABORT)


def _resolve_exit(codes: list[int]) -> int:
    for code in EXIT_PRECEDENCE:
        if code in codes:
            return code
    return EXIT_OK


def _journal_params(args: argparse.Namespace) -> dict[str, Any]:
    """Effective config for a scenario that died before building its
    report — everything except credentials."""
    return {
        k: v
        for k, v in vars(args).items()
        if k not in ("s3_access_key", "s3_secret_key", "scenario", "quick", "full")
    }


def _run_scenario(
    bench: Bench,
    name: str,
    args: argparse.Namespace,
    config: dict[str, Any],
) -> tuple[int, list[str]]:
    """Run one scenario; journal a JSONL line whether it completed or
    failed (always with a ``status`` field); return (exit code, flags)."""
    print(f"=== {name} (run {bench.cfg.run_id}) ===", flush=True)
    t0 = time.monotonic()
    try:
        report: ScenarioReport = SCENARIOS[name].run(bench, args)
    except InvariantViolation as exc:
        bench.append_result(
            name,
            _journal_params(args),
            [],
            status="invariant_violation",
            error=str(exc),
            config=config,
        )
        print(
            f"\nINVARIANT VIOLATION in {name}: {exc}\n"
            "The benchmark corrupted or mis-modeled catalog state; the "
            "numbers above are not trustworthy.",
            file=sys.stderr,
        )
        return EXIT_INVARIANT, []
    except BenchAbort as exc:
        bench.append_result(
            name,
            _journal_params(args),
            [],
            status="aborted",
            error=str(exc),
            config=config,
        )
        print(f"\nABORT in {name}: {exc}", file=sys.stderr)
        return EXIT_ABORT, []
    except Exception as exc:  # noqa: BLE001 - harness bug, own exit code
        bench.append_result(
            name,
            _journal_params(args),
            [],
            status="error",
            error=f"{type(exc).__name__}: {exc}",
            config=config,
        )
        traceback.print_exc()
        print(
            f"\nERROR in {name}: unexpected {type(exc).__name__}: {exc} "
            f"(harness bug — exit {EXIT_ERROR})",
            file=sys.stderr,
        )
        return EXIT_ERROR, []
    status = "regression" if report.flags else "ok"
    bench.append_result(
        name,
        report.params,
        report.metrics,
        status=status,
        flags=report.flags,
        config=config,
    )
    print(f"=== {name} done in {time.monotonic() - t0:.1f}s ===\n", flush=True)
    return (EXIT_REGRESSION if report.flags else EXIT_OK), report.flags


def _namespace_for(
    name: str, base: argparse.Namespace, overrides: dict[str, Any]
) -> argparse.Namespace:
    """Build the scenario's namespace: its own parser defaults, the shared
    options from ``base``, then the profile overrides."""
    p = argparse.ArgumentParser()
    _add_common(p)
    SCENARIOS[name].add_args(p)
    ns = p.parse_args([])
    for k in vars(ns):
        if (
            hasattr(base, k)
            and getattr(base, k) is not None
            and k
            in (
                "url",
                "s3_endpoint",
                "s3_access_key",
                "s3_secret_key",
                "bucket",
                "results",
                "duration",
                "warmup",
            )
        ):
            setattr(ns, k, getattr(base, k))
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = BenchConfig(
        url=args.url,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        bucket=args.bucket,
        results_path=args.results,
    )
    try:
        check_server(cfg.url)
    except BenchAbort as exc:
        print(f"\nABORT: {exc}", file=sys.stderr)
        return EXIT_ABORT
    bench = Bench(cfg)
    codes: list[int] = []
    flags: list[str] = []
    try:
        if args.scenario == "seed":
            try:
                seed_task.run(bench, args)
            except BenchAbort as exc:
                print(f"\nABORT: {exc}", file=sys.stderr)
                return EXIT_ABORT
            return EXIT_OK
        if args.scenario == "all":
            profile = FULL_PROFILE if args.full else QUICK_PROFILE
            label = "full" if args.full else "quick"
            print(f"hoglake-bench all --{label}\n", flush=True)
            for name in SCENARIOS:
                ns = _namespace_for(name, args, profile[name])
                config = {"url": cfg.url, "profile": label}
                code, scenario_flags = _run_scenario(bench, name, ns, config)
                codes.append(code)
                flags.extend(scenario_flags)
                if code == EXIT_ABORT:
                    # keep going only if the server still answers — an
                    # insufficient-samples abort shouldn't kill the run,
                    # but a dead server must not be hammered further
                    try:
                        check_server(cfg.url)
                    except BenchAbort:
                        print(
                            "server unreachable — skipping remaining scenarios",
                            file=sys.stderr,
                        )
                        break
        else:
            config = {"url": cfg.url, "profile": None}
            code, scenario_flags = _run_scenario(bench, args.scenario, args, config)
            codes.append(code)
            flags.extend(scenario_flags)
    finally:
        bench.close()
    if flags:
        print(
            f"\n{len(flags)} regression flag(s) raised — see !!! lines",
            file=sys.stderr,
        )
    return _resolve_exit(codes)


if __name__ == "__main__":
    sys.exit(main())
