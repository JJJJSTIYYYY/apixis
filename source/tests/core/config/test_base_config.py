"""Tests for shared configuration loading and core runtime settings."""

import importlib
import os

import pytest

from apixis.core.config import base, core_config


def test_apixis_data_path_is_within_shared_root():
    assert core_config.APIXIS_BASE_DIR == os.path.join(core_config.BASE_DIR, "apixis")


@pytest.mark.parametrize("value", [0, -1, True, "8192", 1.5])
def test_chain_cache_size_rejects_invalid_configuration(monkeypatch, value):
    try:
        with monkeypatch.context() as patch:
            patch.setattr(base, "_config", {"PIPELINE": {"handler_chain_cache_size": value}})
            with pytest.raises(ValueError, match="handler_chain_cache_size must be a positive integer"):
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
