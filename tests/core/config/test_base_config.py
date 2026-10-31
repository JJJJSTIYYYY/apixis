"""Tests for shared configuration loading and core runtime settings."""

import importlib
import os

import pytest

from apixis.core.config import base, core_config


@pytest.mark.parametrize("value", [0, -1, True, "8192", 1.5])
def test_chain_cache_size_rejects_invalid_configuration(monkeypatch, value):
    try:
        with monkeypatch.context() as patch:
            patch.setattr(base, "_config", {"EVENT": {"handler_chain_cache_size": value}})
            with pytest.raises(ValueError if type(value) is int else TypeError, match="handler_chain_cache_size"):
                importlib.reload(core_config)
    finally:
        importlib.reload(core_config)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("nested.value", 42),
        ("nested.zero", 0),
        ("nested.disabled", False),
        ("nested.empty", ""),
        ("nested.null", "fallback"),
        ("nested.absent", "fallback"),
        ("nested.value.child", "fallback"),
        ("missing", "fallback"),
    ],
)
def test_get_config_uses_shared_mapping_and_preserves_falsey_values(
    monkeypatch, path, expected
):
    """Lookup works in base.py without an undefined or duplicated _config."""
    monkeypatch.setattr(base, "_config", {
        "nested": {"value": 42, "zero": 0, "disabled": False, "empty": "", "null": None}
    })

    assert base._get_config(path, "fallback") == expected
    assert core_config._get_config(path, "fallback") == expected


def test_yaml_missing_or_empty_file_uses_defaults(tmp_path):
    path = tmp_path / "config.yaml"
    assert base._load_from_yaml(str(path)) == {}
    path.write_text("", encoding="utf-8")
    assert base._load_from_yaml(str(path)) == {}


def test_yaml_rejects_non_mapping(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("- item\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must contain a YAML mapping"):
        base._load_from_yaml(str(path))


def test_yaml_reads_only_apixis_namespace(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "APIXIS:\n  EVENT:\n    show_event_dispatch: false\n"
        "EVENT:\n  show_event_dispatch: true\nSERVER:\n  base_dir: /app\n",
        encoding="utf-8",
    )
    assert base._load_from_yaml(str(path)) == {"EVENT": {"show_event_dispatch": False}}


@pytest.mark.parametrize("content", [
    "APIXIS: null\n",
    "APIXIS: {}\n",
    "LOG:\n  show_event_dispatch: false\nPIPELINE:\n  event_pipe_max_len: 1\n"
    "GRAPH:\n  max_snapshots: 3\n",
])
def test_yaml_missing_or_null_namespace_ignores_other_settings(tmp_path, content):
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    assert base._load_from_yaml(str(path)) == {}


@pytest.mark.parametrize("value", ["[]", "false", "0", "settings"])
def test_yaml_rejects_non_mapping_namespace(tmp_path, value):
    path = tmp_path / "config.yaml"
    path.write_text(f"APIXIS: {value}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="APIXIS.*must contain a YAML mapping"):
        base._load_from_yaml(str(path))


def test_legacy_categories_are_not_read(monkeypatch):
    try:
        with monkeypatch.context() as patch:
            patch.setattr(base, "_config", {
                "LOG": {"show_event_dispatch": False},
                "PIPELINE": {"event_pipe_max_len": 1, "event_loop_backpressure": 128,
                             "handler_chain_cache_size": 1},
            })
            importlib.reload(core_config)
            assert core_config.SHOW_EVENT_DISPATCH is True
            assert core_config.EVENT_PIPE_MAX_LEN == 65536
            assert core_config.EVENT_LOOP_BACKPRESSURE == 1024
            assert core_config.HANDLER_CHAIN_CACHE_SIZE == 8192
    finally:
        importlib.reload(core_config)


@pytest.mark.parametrize("section, key", [
    ("EVENT", "event_pipe_max_len"),
    ("EVENT", "event_loop_backpressure"),
    ("EVENT", "handler_chain_cache_size"),
    ("GRAPH", "max_snapshots"),
])
@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "2", float("nan"), float("inf")])
def test_all_numeric_configuration_rejects_invalid_capacities(monkeypatch, section, key, value):
    try:
        with monkeypatch.context() as patch:
            patch.setattr(base, "_config", {section: {key: value}})
            with pytest.raises(ValueError if type(value) is int else TypeError, match=key):
                importlib.reload(core_config)
    finally:
        importlib.reload(core_config)


def test_yaml_import_contract_and_snapshot_default_in_fresh_process(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    (tmp_path / "config.yaml").write_text(
        "APIXIS:\n"
        "  GRAPH:\n    max_snapshots: 3\n"
        "  EVENT:\n    event_pipe_max_len: 2\n    event_loop_backpressure: 1\n"
        "    handler_chain_cache_size: 7\n    show_event_dispatch: false\n"
        "GRAPH:\n  max_snapshots: 99\nEVENT:\n  event_pipe_max_len: 99\n",
        encoding="utf-8",
    )
    script = '''
from pathlib import Path
from apixis.core.config import core_config
from apixis import GraphManager
assert core_config.GRAPH_MAX_SNAPSHOTS == 3
assert core_config.EVENT_PIPE_MAX_LEN == 2
assert core_config.EVENT_LOOP_BACKPRESSURE == 128
assert core_config.HANDLER_CHAIN_CACHE_SIZE == 7
assert core_config.SHOW_EVENT_DISPATCH is False
graph = GraphManager().compile_graph(None)
assert graph.create_context({}).max_snapshots == 3
Path("config.yaml").write_text("APIXIS:\\n  GRAPH:\\n    max_snapshots: 5\\n")
assert core_config.GRAPH_MAX_SNAPSHOTS == 3
graph.decompose()
'''
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(Path(base.__file__).resolve().parents[3])},
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
