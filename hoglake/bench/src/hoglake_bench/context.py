"""Bench run context: server/S3 wiring, per-run unique naming, results sink."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from pyhoglake import Catalog, HoglakeClient, S3Config


@dataclass
class BenchConfig:
    url: str = "http://localhost:8080"
    s3_endpoint: str = "http://localhost:19000"
    s3_access_key: str = "hoglake"
    s3_secret_key: str = "hoglake123"
    s3_region: str = "us-east-1"
    bucket: str = "hoglake-bench"
    results_path: str = "bench-results.jsonl"
    timeout: float = 60.0
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])


class Bench:
    """Owns the client, mints unique bench-* catalogs, appends results.

    Every catalog (and its data_path) embeds the run id, so runs never
    collide and no cleanup is required (there is no catalog-delete API;
    bench catalogs are cheap rows the next expiry sweep keeps small).
    """

    def __init__(self, cfg: BenchConfig) -> None:
        self.cfg = cfg
        self.client = HoglakeClient(
            cfg.url,
            s3=S3Config(
                access_key=cfg.s3_access_key,
                secret_key=cfg.s3_secret_key,
                endpoint_override=cfg.s3_endpoint,
                region=cfg.s3_region,
                allow_bucket_creation=True,
            ),
            timeout=cfg.timeout,
        )
        self._seq = 0
        self._bucket_ready = False

    # -- naming ------------------------------------------------------------

    def new_catalog(self, slug: str) -> Catalog:
        self._seq += 1
        name = f"bench-{slug}-{self.cfg.run_id}-{self._seq}"
        data_path = f"s3://{self.cfg.bucket}/{self.cfg.run_id}/{name}/"
        return self.client.create_catalog(name, data_path)

    # -- object store (only for scenarios that touch real bytes) -----------

    def ensure_bucket(self) -> None:
        if self._bucket_ready:
            return
        fs = self.client._filesystem()
        fs.create_dir(self.cfg.bucket)  # idempotent
        self._bucket_ready = True

    def put_object(self, uri: str, payload: bytes) -> None:
        assert uri.startswith("s3://")
        fs = self.client._filesystem()
        with fs.open_output_stream(uri[len("s3://") :]) as out:
            out.write(payload)

    # -- results -----------------------------------------------------------

    def append_result(
        self,
        scenario: str,
        params: dict[str, Any],
        metrics: list[Any],
        *,
        status: str = "ok",
        flags: list[str] | None = None,
        error: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        """One JSONL line per completed-OR-failed scenario. Stable
        schema: status is always present ('ok', 'regression', 'aborted',
        'invariant_violation', 'error'); flags/error/config are always
        present (empty list / null / {})."""
        record = {
            "ts": time.time(),
            "run_id": self.cfg.run_id,
            "scenario": scenario,
            "status": status,
            "params": params,
            "config": config or {},
            "flags": flags or [],
            "error": error,
            "metrics": [m.to_json() for m in metrics],
        }
        with open(self.cfg.results_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")

    def close(self) -> None:
        self.client.close()
