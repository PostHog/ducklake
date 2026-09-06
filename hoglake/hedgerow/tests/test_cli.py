"""CLI entry point: exit codes for the no-server-needed paths."""

import yaml

from hedgerow.cli import main


def test_missing_config_file_exits_1():
    assert main(["--config", "/nonexistent/hedgerow.yaml"]) == 1


def test_invalid_config_shape_exits_1(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text(yaml.safe_dump({"source": {"url": "http://x"}}))  # missing keys
    assert main(["--config", str(p)]) == 1
