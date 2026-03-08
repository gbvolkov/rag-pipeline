from __future__ import annotations

import importlib
import inspect
from typing import Any


LEGACY_RUNTIME_SPEC_KEYS = frozenset({"factory", "__runtime_factory__", "plugin_ref"})

_RUNTIME_OBJECT_IMPORTS: dict[str, tuple[str, str]] = {
    "create_llm": ("rag_lib.llm.factory", "create_llm"),
    "create_embeddings_model": ("rag_lib.embeddings.factory", "create_embeddings_model"),
    "create_graph_store": ("rag_lib.graph.store", "create_graph_store"),
    "NetworkXGraphStore": ("rag_lib.graph.store", "NetworkXGraphStore"),
    "GraphQueryConfig": ("rag_lib.retrieval.graph_retriever", "GraphQueryConfig"),
    "LLMTableSummarizer": ("rag_lib.summarizers.table_llm", "LLMTableSummarizer"),
    "WebCleanupConfig": ("rag_lib.loaders.web_playwright_extractors", "WebCleanupConfig"),
    "PlaywrightNavigationConfig": (
        "rag_lib.loaders.web_playwright_extractors",
        "PlaywrightNavigationConfig",
    ),
    "PlaywrightExtractionConfig": (
        "rag_lib.loaders.web_playwright_extractors",
        "PlaywrightExtractionConfig",
    ),
    "PlaywrightProfileConfig": (
        "rag_lib.loaders.web_playwright_extractors",
        "PlaywrightProfileConfig",
    ),
}


class RuntimeObjectError(ValueError):
    pass


def _resolve_runtime_object(object_type: str):
    target_ref = _RUNTIME_OBJECT_IMPORTS.get(object_type)
    if target_ref is None:
        raise RuntimeObjectError(
            f"Unknown runtime object_type '{object_type}'",
        )
    module_name, symbol_name = target_ref
    module = importlib.import_module(module_name)
    try:
        return getattr(module, symbol_name)
    except AttributeError as exc:
        raise RuntimeObjectError(
            f"Runtime object_type '{object_type}' is unavailable in installed rag-lib",
        ) from exc


def _is_runtime_object_spec(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("object_type"), str)


def validate_runtime_object_specs(value: Any, *, path: str = "params") -> None:
    if isinstance(value, dict):
        legacy = LEGACY_RUNTIME_SPEC_KEYS.intersection(value.keys())
        if legacy:
            joined = ", ".join(sorted(legacy))
            raise RuntimeObjectError(
                f"Legacy runtime spec key(s) {joined} are unsupported at {path}; use explicit object_type specs",
            )
        if _is_runtime_object_spec(value):
            object_type = value["object_type"]
            _resolve_runtime_object(object_type)
            for key, nested in value.items():
                if key == "object_type":
                    continue
                validate_runtime_object_specs(nested, path=f"{path}.{key}")
            return
        for key, nested in value.items():
            validate_runtime_object_specs(nested, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for idx, nested in enumerate(value):
            validate_runtime_object_specs(nested, path=f"{path}[{idx}]")


def materialize_runtime_object_value(value: Any) -> Any:
    if isinstance(value, dict):
        legacy = LEGACY_RUNTIME_SPEC_KEYS.intersection(value.keys())
        if legacy:
            joined = ", ".join(sorted(legacy))
            raise RuntimeObjectError(
                f"Legacy runtime spec key(s) {joined} are unsupported; use explicit object_type specs",
            )
        if _is_runtime_object_spec(value):
            object_type = value["object_type"]
            target = _resolve_runtime_object(object_type)
            params = {
                key: materialize_runtime_object_value(nested)
                for key, nested in value.items()
                if key != "object_type"
            }
            if inspect.isclass(target):
                return target(**params)
            return target(**params)
        return {key: materialize_runtime_object_value(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [materialize_runtime_object_value(nested) for nested in value]
    return value
