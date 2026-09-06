import os

import pytest

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080")


def _server_reachable() -> bool:
    import httpx

    try:
        r = httpx.get(HOGLAKE_URL.rstrip("/") + "/healthz", timeout=3.0)
        return r.status_code == 200
    except Exception:  # noqa: BLE001 - any failure means "not reachable"
        return False


@pytest.fixture(scope="session")
def live_server_url() -> str:
    if not _server_reachable():
        pytest.skip(f"no live hoglake server at {HOGLAKE_URL} (set HOGLAKE_URL)")
    return HOGLAKE_URL
