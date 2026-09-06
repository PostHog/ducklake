"""Config parsing and shape validation (fail-fast, lesson #6)."""

import pytest
import yaml

from hedgerow.config import (
    ConfigError,
    HedgerowConfig,
    apply_env_overrides,
    load_config,
)

VALID = {
    "source": {
        "url": "http://src:8080",
        "catalog": "cat",
        "namespace": "ns",
        "table": "t",
        "consumer_id": "hedge-1",
        "start_snapshot": 5,
        "s3": {
            "endpoint": "http://minio:19000",
            "access_key": "k",
            "secret_key": "s",
            "path_style": True,
        },
    },
    "destination": {
        "url": "http://dst:8080",
        "catalog": "cat2",
        "namespace": "ns2",
        "table": "t2",
        "s3": {"endpoint": "http://minio:19000", "access_key": "k", "secret_key": "s"},
    },
    "filter": {"column": "team_id", "equals": 42},
    "replication": {
        "poll_interval_s": 1,
        "max_snapshot_window": 100,
        "max_rows_per_append": 1000,
    },
    "metrics": {"port": 9000},
}


def _valid() -> dict:
    import copy

    return copy.deepcopy(VALID)


def test_parses_full_config():
    cfg = HedgerowConfig.parse(_valid())
    assert cfg.source.url == "http://src:8080"
    assert cfg.source.consumer_id == "hedge-1"
    assert cfg.source.start_snapshot == 5
    assert cfg.source.s3.endpoint == "http://minio:19000"
    assert cfg.destination.catalog == "cat2"
    assert cfg.filter.column == "team_id"
    assert cfg.filter.equals == 42
    assert cfg.replication.poll_interval_s == 1.0
    assert cfg.replication.max_snapshot_window == 100
    assert cfg.replication.max_rows_per_append == 1000
    assert cfg.metrics.port == 9000


def test_defaults():
    raw = _valid()
    del raw["filter"], raw["replication"], raw["metrics"]
    del raw["source"]["start_snapshot"]
    cfg = HedgerowConfig.parse(raw)
    assert cfg.filter is None
    assert cfg.source.start_snapshot == 0
    assert cfg.replication.poll_interval_s == 5.0
    assert cfg.replication.max_snapshot_window == 1000
    assert cfg.replication.max_rows_per_append == 100_000
    assert cfg.metrics.port == 0


@pytest.mark.parametrize("key", ["source", "destination"])
def test_missing_top_level_section(key):
    raw = _valid()
    del raw[key]
    with pytest.raises(ConfigError, match=f"config.{key}"):
        HedgerowConfig.parse(raw)


@pytest.mark.parametrize("key", ["url", "catalog", "namespace", "table", "consumer_id"])
def test_missing_source_key(key):
    raw = _valid()
    del raw["source"][key]
    with pytest.raises(ConfigError, match=f"source.{key}"):
        HedgerowConfig.parse(raw)


def test_unknown_key_rejected():
    raw = _valid()
    raw["source"]["routing_value"] = "nope"
    with pytest.raises(ConfigError, match="unknown key.*routing_value"):
        HedgerowConfig.parse(raw)


def test_unknown_top_level_key_rejected():
    raw = _valid()
    raw["scheduler"] = {"central": True}  # never again
    with pytest.raises(ConfigError, match="unknown key.*scheduler"):
        HedgerowConfig.parse(raw)


def test_destination_has_no_consumer_id():
    raw = _valid()
    raw["destination"]["consumer_id"] = "x"
    with pytest.raises(ConfigError, match="unknown key.*consumer_id"):
        HedgerowConfig.parse(raw)


def test_filter_requires_both_keys():
    raw = _valid()
    raw["filter"] = {"column": "team_id"}
    with pytest.raises(ConfigError, match="filter.equals"):
        HedgerowConfig.parse(raw)
    raw["filter"] = {"equals": 42}
    with pytest.raises(ConfigError, match="filter.column"):
        HedgerowConfig.parse(raw)


def test_filter_equals_may_be_falsy():
    raw = _valid()
    raw["filter"] = {"column": "flag", "equals": 0}
    assert HedgerowConfig.parse(raw).filter.equals == 0


def test_filter_equals_null_refused():
    # BUG-5 regression: `equals:` / `equals: null` matches nothing —
    # silent 100% drop. Refused at parse time.
    raw = _valid()
    raw["filter"] = {"column": "team_id", "equals": None}
    with pytest.raises(ConfigError, match="non-null"):
        HedgerowConfig.parse(raw)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_filter_equals_nan_inf_refused(value):
    # bugs.md #4 regression: NaN == x is False for EVERY row (IEEE 754),
    # so a NaN filter is vacuous — 100% of rows dropped while the offset
    # advances. Same silent-data-loss class as the null filter.
    raw = _valid()
    raw["filter"] = {"column": "team_id", "equals": value}
    with pytest.raises(ConfigError, match="finite"):
        HedgerowConfig.parse(raw)


def test_filter_equals_yaml_nan_refused(tmp_path):
    # the real-world shape: YAML `.nan` safe_loads to float('nan')
    raw = _valid()
    del raw["filter"]
    text = yaml.safe_dump(raw) + "filter:\n  column: team_id\n  equals: .nan\n"
    p = tmp_path / "nan.yaml"
    p.write_text(text)
    with pytest.raises(ConfigError, match="finite"):
        load_config(str(p), environ={})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_poll_interval_nan_inf_refused_at_startup(value):
    # bugs.md #21 regression: isinstance(nan, float) passes and
    # `nan < 0.1` is False, so without the finite check the daemon would
    # start and explode at the first time.sleep(nan) mid-loop.
    raw = _valid()
    raw["replication"]["poll_interval_s"] = value
    with pytest.raises(ConfigError, match="poll_interval_s must be a finite"):
        HedgerowConfig.parse(raw)


def test_poll_interval_yaml_nan_refused_via_env(tmp_path):
    # env overrides are YAML-parsed scalars: ".nan" -> float('nan')
    p = tmp_path / "ok.yaml"
    p.write_text(yaml.safe_dump(_valid()))
    with pytest.raises(ConfigError, match="poll_interval_s must be a finite"):
        load_config(str(p), environ={"HEDGEROW__REPLICATION__POLL_INTERVAL_S": ".nan"})


@pytest.mark.parametrize(
    "field,value",
    [
        ("poll_interval_s", -1),
        ("poll_interval_s", 0),  # would busy-spin the loop; min 0.1
        ("poll_interval_s", 0.05),
        ("poll_interval_s", "fast"),
        ("max_snapshot_window", 0),
        ("max_rows_per_append", 0),
        ("max_rows_per_append", 1.5),
        ("max_window_replays", -1),
        ("max_window_replays", 1.5),
        ("max_append_retries", -1),
        ("max_append_retries", 1.5),
    ],
)
def test_replication_bounds(field, value):
    raw = _valid()
    raw["replication"][field] = value
    with pytest.raises(ConfigError, match=f"replication.{field}"):
        HedgerowConfig.parse(raw)


def test_max_window_replays_default_and_parse():
    raw = _valid()
    assert HedgerowConfig.parse(raw).replication.max_window_replays == 3
    raw["replication"]["max_window_replays"] = 0  # halt on first failure
    assert HedgerowConfig.parse(raw).replication.max_window_replays == 0


def test_max_append_retries_default_and_parse():
    raw = _valid()
    assert HedgerowConfig.parse(raw).replication.max_append_retries == 3
    raw["replication"]["max_append_retries"] = 0  # escalate on first conflict
    assert HedgerowConfig.parse(raw).replication.max_append_retries == 0


def test_type_errors_are_precise():
    raw = _valid()
    raw["source"]["url"] = 12
    with pytest.raises(ConfigError, match="source.url must be a non-empty string"):
        HedgerowConfig.parse(raw)


@pytest.mark.parametrize("key", ["access_key", "secret_key"])
def test_credential_values_never_appear_in_errors(key):
    # a mistyped credential (e.g. YAML parsed it as an int) must not leak
    # the value into logs via the error message — type name only.
    raw = _valid()
    raw["source"]["s3"][key] = 981276345
    with pytest.raises(ConfigError) as ei:
        HedgerowConfig.parse(raw)
    msg = str(ei.value)
    assert "981276345" not in msg
    assert f"source.s3.{key}" in msg
    assert "int" in msg


def test_s3_path_style_must_be_bool():
    raw = _valid()
    raw["source"]["s3"]["path_style"] = "yes"
    with pytest.raises(ConfigError, match="path_style"):
        HedgerowConfig.parse(raw)


def test_load_config_missing_file():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/hedgerow.yaml")


def test_load_config_invalid_yaml(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("source: [unclosed")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(str(p))


def test_load_config_roundtrip(tmp_path):
    p = tmp_path / "ok.yaml"
    p.write_text(yaml.safe_dump(_valid()))
    cfg = load_config(str(p), environ={})
    assert cfg.source.table == "t"


def test_env_overrides(tmp_path):
    p = tmp_path / "ok.yaml"
    p.write_text(yaml.safe_dump(_valid()))
    cfg = load_config(
        str(p),
        environ={
            "HEDGEROW__SOURCE__S3__SECRET_KEY": "overridden",
            "HEDGEROW__REPLICATION__POLL_INTERVAL_S": "0.5",
            "HEDGEROW__METRICS__PORT": "9100",
            "UNRELATED": "ignored",
        },
    )
    assert cfg.source.s3.secret_key == "overridden"
    assert cfg.replication.poll_interval_s == 0.5
    assert cfg.metrics.port == 9100


def test_env_override_creates_missing_section():
    raw = {"a": {}}
    out = apply_env_overrides(raw, {"HEDGEROW__A__B__C": "1"})
    assert out["a"]["b"]["c"] == 1


def test_s3_settings_to_pyhoglake():
    cfg = HedgerowConfig.parse(_valid())
    s3 = cfg.source.s3.to_pyhoglake()
    assert s3.endpoint_override == "http://minio:19000"
    assert s3.access_key == "k"
    assert s3.secret_key == "s"
