from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import importlib.util
import inspect
import json
import pkgutil
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.errors import UnprocessableError


CALLABLE_HINTS = ("callback", "extractor", "processor", "function", "callable")
RETRIEVER_FACTORY_MODULES = (
    "rag_lib.retrieval.retrievers",
    "rag_lib.retrieval.composition",
)
RETRIEVER_CLASS_MODULES = (
    "rag_lib.retrieval.retrievers",
    "rag_lib.retrieval.graph_retriever",
)
_PROVIDER_PATTERN = re.compile(
    r"""(?:^|\s)(?:if|elif)\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*==\s*["'](?P<value>[^"']+)["']"""
)
_TUPLE_LITERAL_PATTERN = re.compile(
    r"""(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*\((?P<body>[^)]*)\)""",
    re.DOTALL,
)
_STRING_LITERAL_PATTERN = re.compile(r"""["']([^"']+)["']""")


def _resolve_runtime_rag_lib_dir() -> Path:
    spec = importlib.util.find_spec("rag_lib")
    if spec is None or not spec.submodule_search_locations:
        raise ModuleNotFoundError("Installed package 'rag_lib' was not found")
    candidates = [Path(path) for path in spec.submodule_search_locations]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Could not resolve installed rag_lib package directory from spec locations: {candidates}"
    )


def _digest_files(root: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        hasher.update(path.as_posix().encode("utf-8"))
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def _module_members(package_name: str) -> tuple[list[tuple[str, Any]], list[str]]:
    package = importlib.import_module(package_name)
    out: list[tuple[str, Any]] = [(package_name.rsplit(".", 1)[-1], package)]
    warnings: list[str] = []

    package_paths = getattr(package, "__path__", None)
    if not package_paths:
        return out, warnings

    for module_info in sorted(pkgutil.iter_modules(package_paths), key=lambda item: item.name):
        module_name = f"{package_name}.{module_info.name}"
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            warnings.append(f"{module_name}: {type(exc).__name__}: {exc}")
            continue
        out.append((module_info.name, module))
    return out, warnings


def _modules_by_name(module_names: tuple[str, ...]) -> tuple[list[tuple[str, Any]], list[str]]:
    out: list[tuple[str, Any]] = []
    warnings: list[str] = []
    for module_name in module_names:
        try:
            out.append((module_name.rsplit(".", 1)[-1], importlib.import_module(module_name)))
        except Exception as exc:
            warnings.append(f"{module_name}: {type(exc).__name__}: {exc}")
    return out, warnings


def _pydantic_model_params(target: type[Any]) -> list[dict[str, Any]] | None:
    model_fields = getattr(target, "model_fields", None)
    if not isinstance(model_fields, dict) or not model_fields:
        return None

    params: list[dict[str, Any]] = []
    for name, field in model_fields.items():
        required = bool(getattr(field, "is_required", lambda: False)())
        params.append(
            {
                "name": name,
                "required": required,
                "default": None if required else repr(getattr(field, "default", None)),
                "callable_like": any(hint in name.lower() for hint in CALLABLE_HINTS),
            }
        )
    return params


def _signature_params(target: Any) -> list[dict[str, Any]]:
    if inspect.isclass(target):
        pydantic_params = _pydantic_model_params(target)
        if pydantic_params is not None:
            return pydantic_params
        signature_target = target.__init__
    else:
        signature_target = target

    signature = inspect.signature(signature_target)
    params: list[dict[str, Any]] = []
    for name, param in signature.parameters.items():
        if name == "self" or param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue
        required = param.default is inspect.Signature.empty
        params.append(
            {
                "name": name,
                "required": required,
                "default": None if required else repr(param.default),
                "callable_like": any(hint in name.lower() for hint in CALLABLE_HINTS),
            }
        )
    return params


def _discover_component_classes(
    package_name: str,
    suffixes: tuple[str, ...],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    out: dict[str, dict[str, Any]] = {}
    modules, warnings = _module_members(package_name)
    for module_name, module in modules:
        for name, value in vars(module).items():
            if not inspect.isclass(value):
                continue
            if value.__module__ != module.__name__:
                continue
            if not name.endswith(suffixes):
                continue
            out[name] = {
                "module": module_name,
                "params": _signature_params(value),
            }
    return dict(sorted(out.items())), warnings


def _discover_retriever_classes() -> tuple[dict[str, dict[str, Any]], list[str]]:
    out: dict[str, dict[str, Any]] = {}
    modules, warnings = _modules_by_name(RETRIEVER_CLASS_MODULES)
    for module_name, module in modules:
        for name, value in vars(module).items():
            if not inspect.isclass(value):
                continue
            if value.__module__ != module.__name__:
                continue
            if not name.endswith("Retriever"):
                continue
            out[name] = {
                "module": module_name,
                "params": _signature_params(value),
            }
    return dict(sorted(out.items())), warnings


def _discover_retriever_factories() -> tuple[dict[str, dict[str, Any]], list[str]]:
    out: dict[str, dict[str, Any]] = {}
    modules, warnings = _modules_by_name(RETRIEVER_FACTORY_MODULES)
    for module_name, module in modules:
        for name, value in vars(module).items():
            if not inspect.isfunction(value):
                continue
            if value.__module__ != module.__name__:
                continue
            if not name.startswith("create_") or "retriever" not in name:
                continue
            out[name] = {
                "module": module_name,
                "params": _signature_params(value),
            }
    return dict(sorted(out.items())), warnings


def _extract_provider_literals(fn: Any, variable_name: str) -> list[str]:
    source = inspect.getsource(fn)
    providers = {
        match.group("value")
        for match in _PROVIDER_PATTERN.finditer(source)
        if match.group("name") == variable_name
    }
    return sorted(providers)


def _extract_literal_values(module: Any) -> dict[str, list[str]]:
    source = inspect.getsource(module)
    found: dict[str, list[str]] = {}
    for match in _TUPLE_LITERAL_PATTERN.finditer(source):
        values = _STRING_LITERAL_PATTERN.findall(match.group("body"))
        if values:
            found[match.group("name")] = sorted(set(values))
    return dict(sorted(found.items()))


def _best_effort_call(warnings: list[str], label: str, fn) -> Any:
    try:
        return fn()
    except Exception as exc:
        warnings.append(f"{label}: {type(exc).__name__}: {exc}")
        return None


def _resolve_symbol_from_package(package_name: str, symbol_name: str, *, kind: str):
    modules, warnings = _module_members(package_name)
    for _, module in modules:
        candidate = getattr(module, symbol_name, None)
        if inspect.isclass(candidate):
            return candidate
    details = {"package": package_name}
    if warnings:
        details["import_errors"] = warnings
    raise UnprocessableError(f"Unknown {kind} type '{symbol_name}'", details=details)


def resolve_loader_class(loader_type: str):
    return _resolve_symbol_from_package("rag_lib.loaders", loader_type, kind="loader")


def resolve_splitter_class(splitter_type: str):
    return _resolve_symbol_from_package("rag_lib.chunkers", splitter_type, kind="splitter")


def resolve_processor_class(processor_type: str):
    return _resolve_symbol_from_package("rag_lib.processors", processor_type, kind="processor")


def discover_capabilities() -> dict[str, Any]:
    rag_lib_dir = _resolve_runtime_rag_lib_dir()
    discovery_warnings: list[str] = []

    loaders, loader_warnings = _discover_component_classes("rag_lib.loaders", ("Loader",))
    splitters, splitter_warnings = _discover_component_classes("rag_lib.chunkers", ("Splitter", "Chunker"))
    processors, processor_warnings = _discover_component_classes(
        "rag_lib.processors",
        ("Processor", "Extractor", "Enricher", "Summarizer"),
    )
    retriever_classes, retriever_class_warnings = _discover_retriever_classes()
    retriever_factories, retriever_factory_warnings = _discover_retriever_factories()
    discovery_warnings.extend(loader_warnings)
    discovery_warnings.extend(splitter_warnings)
    discovery_warnings.extend(processor_warnings)
    discovery_warnings.extend(retriever_class_warnings)
    discovery_warnings.extend(retriever_factory_warnings)

    vectors_factory = _best_effort_call(
        discovery_warnings,
        "rag_lib.vectors.factory",
        lambda: importlib.import_module("rag_lib.vectors.factory"),
    )
    llm_factory = _best_effort_call(
        discovery_warnings,
        "rag_lib.llm.factory",
        lambda: importlib.import_module("rag_lib.llm.factory"),
    )
    embeddings_factory = _best_effort_call(
        discovery_warnings,
        "rag_lib.embeddings.factory",
        lambda: importlib.import_module("rag_lib.embeddings.factory"),
    )
    graph_store = _best_effort_call(
        discovery_warnings,
        "rag_lib.graph.store",
        lambda: importlib.import_module("rag_lib.graph.store"),
    )
    web_file = _best_effort_call(
        discovery_warnings,
        "rag_lib.loaders.web",
        lambda: importlib.import_module("rag_lib.loaders.web"),
    )
    web_async_file = _best_effort_call(
        discovery_warnings,
        "rag_lib.loaders.web_async",
        lambda: importlib.import_module("rag_lib.loaders.web_async"),
    )
    playwright_file = _best_effort_call(
        discovery_warnings,
        "rag_lib.loaders.web_playwright_extractors",
        lambda: importlib.import_module("rag_lib.loaders.web_playwright_extractors"),
    )

    literals: dict[str, list[str]] = {}
    if web_file is not None:
        extracted = _best_effort_call(
            discovery_warnings,
            "rag_lib.loaders.web literals",
            lambda: _extract_literal_values(web_file),
        )
        if isinstance(extracted, dict):
            literals.update(extracted)
    if web_async_file is not None:
        extracted = _best_effort_call(
            discovery_warnings,
            "rag_lib.loaders.web_async literals",
            lambda: _extract_literal_values(web_async_file),
        )
        if isinstance(extracted, dict):
            literals.update(extracted)
    if playwright_file is not None:
        extracted = _best_effort_call(
            discovery_warnings,
            "rag_lib.loaders.web_playwright_extractors literals",
            lambda: _extract_literal_values(playwright_file),
        )
        if isinstance(extracted, dict):
            literals.update(extracted)

    strict = {
        "loaders": loaders,
        "splitters": splitters,
        "processors": processors,
        "retrievers": {
            "classes": retriever_classes,
            "factories": retriever_factories,
        },
    }
    advisory = {
        "indexes": {
            "vector_store_providers": _best_effort_call(
                discovery_warnings,
                "rag_lib.vectors.factory.create_vector_store providers",
                lambda: _extract_provider_literals(vectors_factory.create_vector_store, "provider"),
            )
            if vectors_factory is not None
            else [],
            "index_types": _best_effort_call(
                discovery_warnings,
                "rag_lib.vectors.factory.create_vector_store index_types",
                lambda: _extract_provider_literals(vectors_factory.create_vector_store, "provider"),
            )
            if vectors_factory is not None
            else [],
        },
        "llm_providers": _best_effort_call(
            discovery_warnings,
            "rag_lib.llm.factory.create_llm providers",
            lambda: _extract_provider_literals(llm_factory.create_llm, "provider"),
        )
        if llm_factory is not None
        else [],
        "embedding_providers": _best_effort_call(
            discovery_warnings,
            "rag_lib.embeddings.factory.create_embeddings_model providers",
            lambda: _extract_provider_literals(embeddings_factory.create_embeddings_model, "provider"),
        )
        if embeddings_factory is not None
        else [],
        "graph_backends": _best_effort_call(
            discovery_warnings,
            "rag_lib.graph.store.create_graph_store providers",
            lambda: _extract_provider_literals(graph_store.create_graph_store, "resolved_provider"),
        )
        if graph_store is not None
        else [],
        "literals": dict(sorted(literals.items())),
        "discovery_warnings": sorted(set(discovery_warnings)),
    }

    return {
        "source_hash": _digest_files(rag_lib_dir),
        "strict": strict,
        "advisory": advisory,
    }


def get_rag_lib_version() -> str:
    return importlib.metadata.version("rag-lib")


def get_capabilities_response() -> dict[str, Any]:
    matrix = discover_capabilities()
    return {
        "rag_lib_version": get_rag_lib_version(),
        "source_hash": matrix["source_hash"],
        "generated_at": datetime.now(tz=UTC),
        "matrix": {
            "strict": matrix["strict"],
            "advisory": matrix["advisory"],
        },
    }


def dump_capabilities_json() -> str:
    matrix = discover_capabilities()
    return json.dumps(matrix, ensure_ascii=False, sort_keys=True, indent=2)
