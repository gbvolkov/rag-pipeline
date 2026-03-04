from __future__ import annotations

import ast
import hashlib
import importlib.metadata
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.entities import CapabilitySnapshot


CALLABLE_HINTS = ("callback", "extractor", "processor", "function", "callable")


def _safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _parse_file(path: Path) -> ast.Module | None:
    content = _safe_read(path)
    if not content:
        return None
    try:
        return ast.parse(content)
    except SyntaxError:
        return None


def _list_python_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(p for p in path.rglob("*.py") if p.name != "__init__.py")


def _digest_files(paths: list[Path]) -> str:
    hasher = hashlib.sha256()
    for path in paths:
        hasher.update(path.as_posix().encode("utf-8"))
        try:
            hasher.update(path.read_bytes())
        except Exception:
            hasher.update(b"")
    return hasher.hexdigest()


def _constructor_params(class_node: ast.ClassDef) -> list[dict[str, Any]]:
    for stmt in class_node.body:
        if isinstance(stmt, ast.FunctionDef) and stmt.name == "__init__":
            params: list[dict[str, Any]] = []
            args = stmt.args.args[1:]  # skip self
            defaults = stmt.args.defaults
            default_offset = len(args) - len(defaults)
            for idx, arg in enumerate(args):
                has_default = idx >= default_offset
                default_value = None
                if has_default:
                    default_node = defaults[idx - default_offset]
                    default_value = ast.unparse(default_node) if hasattr(ast, "unparse") else "<default>"
                params.append(
                    {
                        "name": arg.arg,
                        "required": not has_default,
                        "default": default_value,
                        "callable_like": any(h in arg.arg.lower() for h in CALLABLE_HINTS),
                    }
                )
            return params
    return []


def _discover_classes(files: list[Path], suffix: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in files:
        tree = _parse_file(path)
        if tree is None:
            continue
        module_name = path.stem
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name.endswith(suffix):
                out[node.name] = {
                    "module": module_name,
                    "params": _constructor_params(node),
                }
    return dict(sorted(out.items()))


def _extract_provider_literals(path: Path, variable_name: str = "provider") -> list[str]:
    tree = _parse_file(path)
    if tree is None:
        return []
    providers: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Name) and node.left.id == variable_name:
            for comparator in node.comparators:
                if isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
                    providers.add(comparator.value)
        if isinstance(node, ast.Match):
            for case in node.cases:
                pattern = case.pattern
                if isinstance(pattern, ast.MatchValue) and isinstance(pattern.value, ast.Constant):
                    if isinstance(pattern.value.value, str):
                        providers.add(pattern.value.value)
    return sorted(providers)


def _extract_literal_values(path: Path) -> dict[str, list[str]]:
    tree = _parse_file(path)
    if tree is None:
        return {}
    found: dict[str, set[str]] = defaultdict(set)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            name = node.targets[0].id
            if isinstance(node.value, ast.Call) and getattr(node.value.func, "id", None) == "Literal":
                for arg in node.value.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        found[name].add(arg.value)
            if isinstance(node.value, ast.Tuple):
                str_values = [elt.value for elt in node.value.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)]
                if str_values:
                    for value in str_values:
                        found[name].add(value)
    return {k: sorted(v) for k, v in found.items() if v}


def _extract_retrievers(retrieval_dir: Path) -> dict[str, Any]:
    files = _list_python_files(retrieval_dir)
    retriever_classes = _discover_classes(files, "Retriever")
    retriever_factories: dict[str, dict[str, Any]] = {}
    for path in files:
        tree = _parse_file(path)
        if tree is None:
            continue
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith("create_") and "retriever" in node.name:
                args = [arg.arg for arg in node.args.args]
                retriever_factories[node.name] = {"module": path.stem, "args": args}
    return {"classes": retriever_classes, "factories": dict(sorted(retriever_factories.items()))}


def _extract_index_types(vectors_factory: Path, indexer_file: Path) -> dict[str, Any]:
    vector_providers = _extract_provider_literals(vectors_factory)
    index_modes: set[str] = set(vector_providers)
    index_tree = _parse_file(indexer_file)
    if index_tree is not None:
        for node in ast.walk(index_tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value.lower()
                if "dual" in value and "index" in value:
                    index_modes.add(node.value)
    index_modes.add("dual_storage")
    return {"vector_store_providers": vector_providers, "index_types": sorted(index_modes)}


def discover_capabilities() -> dict[str, Any]:
    settings = get_settings()
    rag_lib_dir = settings.rag_lib_source_dir

    loaders_dir = rag_lib_dir / "loaders"
    chunkers_dir = rag_lib_dir / "chunkers"
    retrieval_dir = rag_lib_dir / "retrieval"
    vectors_factory = rag_lib_dir / "vectors" / "factory.py"
    llm_factory = rag_lib_dir / "llm" / "factory.py"
    embeddings_factory = rag_lib_dir / "embeddings" / "factory.py"
    graph_store = rag_lib_dir / "graph" / "store.py"
    web_file = loaders_dir / "web.py"
    web_async_file = loaders_dir / "web_async.py"
    playwright_file = loaders_dir / "web_playwright_extractors.py"
    indexer_file = rag_lib_dir / "core" / "indexer.py"

    all_files = _list_python_files(rag_lib_dir)
    source_hash = _digest_files(all_files)

    loaders = _discover_classes(_list_python_files(loaders_dir), "Loader")
    splitters = _discover_classes(_list_python_files(chunkers_dir), "Splitter")
    processors = _discover_classes(_list_python_files(rag_lib_dir / "processors"), "Processor")
    processors.update(_discover_classes(_list_python_files(rag_lib_dir / "processors"), "Extractor"))
    processors.update(_discover_classes(_list_python_files(rag_lib_dir / "processors"), "Enricher"))
    processors.update(_discover_classes(_list_python_files(rag_lib_dir / "processors"), "Summarizer"))

    capabilities: dict[str, Any] = {
        "source_root": rag_lib_dir.as_posix(),
        "source_hash": source_hash,
        "loaders": loaders,
        "splitters": splitters,
        "processors": dict(sorted(processors.items())),
        "retrievers": _extract_retrievers(retrieval_dir),
        "indexes": _extract_index_types(vectors_factory, indexer_file),
        "llm_providers": _extract_provider_literals(llm_factory),
        "embedding_providers": _extract_provider_literals(embeddings_factory),
        "graph_backends": _extract_provider_literals(graph_store),
        "literals": {},
    }

    literals = {}
    literals.update(_extract_literal_values(web_file))
    literals.update(_extract_literal_values(web_async_file))
    literals.update(_extract_literal_values(playwright_file))
    capabilities["literals"] = dict(sorted(literals.items()))

    return capabilities


def get_rag_lib_version() -> str:
    try:
        return importlib.metadata.version("rag-lib")
    except Exception:
        pyproject = get_settings().rag_lib_source_dir.parent.parent / "pyproject.toml"
        text = _safe_read(pyproject)
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("version"):
                _, _, value = line.partition("=")
                return value.strip().strip('"').strip("'")
        return "unknown"


def get_or_create_capability_snapshot(db: Session) -> CapabilitySnapshot:
    matrix = discover_capabilities()
    source_hash = matrix["source_hash"]
    version = get_rag_lib_version()

    existing = db.execute(
        select(CapabilitySnapshot).where(
            CapabilitySnapshot.source_hash == source_hash,
            CapabilitySnapshot.rag_lib_version == version,
        )
    ).scalar_one_or_none()
    if existing:
        return existing

    snapshot = CapabilitySnapshot(
        rag_lib_version=version,
        source_hash=source_hash,
        capability_matrix=matrix,
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)
    return snapshot


def capability_snapshot_to_response(snapshot: CapabilitySnapshot) -> dict[str, Any]:
    return {
        "rag_lib_version": snapshot.rag_lib_version,
        "source_hash": snapshot.source_hash,
        "generated_at": snapshot.created_at.astimezone(UTC),
        "matrix": snapshot.capability_matrix,
    }


def dump_capabilities_json() -> str:
    matrix = discover_capabilities()
    return json.dumps(matrix, ensure_ascii=False, sort_keys=True, indent=2)

