from pathlib import Path

import pytest

from cadence_config import ConfigError
from planetr.legacy_runtime import _load


def test_legacy_inheritance_is_declaration_relative(tmp_path, monkeypatch):
    base = tmp_path / "profiles"
    base.mkdir()
    (base / "base.yaml").write_text(
        "version: 1\nonboard: {bind_host: 127.0.0.1, port: 50570}\n"
        "planner: {endpoint: '@record-test'}\n"
        "recording: {directory: recordings, flush_interval_s: 0.5}\n"
    )
    entry = tmp_path / "entry.yaml"
    entry.write_text("extends: profiles/base.yaml\nrecording: {flush_interval_s: 0.25}\n")
    monkeypatch.chdir(tmp_path.parent)
    config = _load(entry)
    assert config["onboard"]["port"] == 50570
    assert config["recording"] == {"directory": "recordings", "flush_interval_s": 0.25}
    assert config["_source"] == entry


@pytest.mark.parametrize("body", [
    "version: 1\nversion: 2\n",
    "extends: entry.yaml\n",
    "version: 1\nonbord: {}\n",
])
def test_legacy_invalid_layers_fail_before_opening_sockets(tmp_path, body):
    entry = tmp_path / "entry.yaml"
    entry.write_text(body)
    with pytest.raises(ConfigError):
        _load(entry)
