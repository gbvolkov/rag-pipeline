from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Callable

from app.core.config import get_settings


def _load_registry_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items()}


def load_plugin_registry() -> dict[str, str]:
    settings = get_settings()
    return _load_registry_file(settings.plugin_registry_file)


def resolve_plugin_ref(ref: str) -> Callable[..., Any]:
    registry = load_plugin_registry()
    target = registry.get(ref, ref)
    if ":" not in target:
        raise ValueError(f"Invalid plugin_ref '{ref}'. Expected 'module:function' or registry alias.")
    module_name, _, symbol = target.partition(":")
    module = importlib.import_module(module_name)
    fn = getattr(module, symbol)
    if not callable(fn):
        raise ValueError(f"Resolved plugin_ref '{ref}' is not callable.")
    return fn


def materialize_plugin_refs(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value.keys()) == {"plugin_ref"} and isinstance(value["plugin_ref"], str):
            return resolve_plugin_ref(value["plugin_ref"])
        return {k: materialize_plugin_refs(v) for k, v in value.items()}
    if isinstance(value, list):
        return [materialize_plugin_refs(v) for v in value]
    return value

