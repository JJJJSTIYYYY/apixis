import os
from collections.abc import Mapping
from typing import Any

import yaml


def _load_from_yaml(path: str) -> dict[str, Any]:
    """Load only the APIXIS namespace from a local YAML file."""
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    if data is None:
        return {}

    if not isinstance(data, dict):
        raise ValueError(
            f"Config file must contain a YAML mapping, got {type(data).__name__}."
        )

    config = data.get("APIXIS")
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise ValueError(
            f"APIXIS configuration must contain a YAML mapping, got {type(config).__name__}."
        )
    return config


def _get_config(path: str, default=None):
    """Read a dotted path, falling back for missing, non-mapping or null values."""
    value = _config

    for key in path.split("."):
        if not isinstance(value, Mapping):
            return default

        if key not in value:
            return default

        value = value[key]

    return default if value is None else value


# Load once in the module that owns configuration lookup.
_config = _load_from_yaml("./config.yaml")
