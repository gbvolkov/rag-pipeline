from __future__ import annotations

from collections import defaultdict
from typing import Any

from app.core.errors import UnprocessableError
from app.schemas.pipelines import PipelineCreate


def _allowed_params(kind: str, type_name: str, capabilities: dict[str, Any]) -> dict[str, Any] | None:
    if kind == "loader":
        return capabilities.get("loaders", {}).get(type_name)
    if kind == "splitter":
        return capabilities.get("splitters", {}).get(type_name)
    if kind == "retriever":
        return capabilities.get("retrievers", {}).get("classes", {}).get(type_name)
    return None


def _validate_params(
    kind: str,
    type_name: str,
    params: dict[str, Any],
    capabilities: dict[str, Any],
) -> None:
    capability = _allowed_params(kind, type_name, capabilities)
    if capability is None:
        raise UnprocessableError(f"Unknown {kind} type '{type_name}'")
    allowed = {p["name"]: p for p in capability.get("params", [])}
    unknown = sorted([key for key in params if key not in allowed])
    if unknown:
        raise UnprocessableError(
            f"Unknown parameter(s) for {kind} '{type_name}'",
            details={"unknown_keys": unknown, "allowed_keys": sorted(allowed.keys())},
        )
    for key, value in params.items():
        meta = allowed[key]
        if isinstance(value, dict) and set(value.keys()) == {"plugin_ref"} and not meta.get("callable_like", False):
            raise UnprocessableError(
                f"Parameter '{key}' on {kind} '{type_name}' is not callable-like and cannot use plugin_ref"
            )


def _validate_input_refs(pipeline: PipelineCreate) -> None:
    if not pipeline.inputs:
        return
    aliases = [x.alias for x in pipeline.inputs]
    if len(set(aliases)) != len(aliases):
        raise UnprocessableError("Input aliases must be unique")

    kinds = {x.artifact_kind for x in pipeline.inputs}
    if len(kinds) > 1:
        raise UnprocessableError("Input artifact kinds must be homogeneous")

    merged = [x.alias for x in pipeline.inputs if x.alias == "__merged__"]
    if merged and len(pipeline.inputs) > 1:
        raise UnprocessableError("'__merged__' alias must be the only pipeline input alias")

    pinned_aliases: dict[str, set[int]] = defaultdict(set)
    for ref in pipeline.inputs:
        if ref.pinned_version is not None:
            pinned_aliases[ref.alias].add(ref.pinned_version)
    inconsistent = [alias for alias, versions in pinned_aliases.items() if len(versions) > 1]
    if inconsistent:
        raise UnprocessableError(
            "Pinned version mismatch for aliases",
            details={"aliases": inconsistent},
        )


def _validate_stages(pipeline: PipelineCreate) -> None:
    names = [stage.stage_name for stage in pipeline.segmentation_stages]
    if len(set(names)) != len(names):
        raise UnprocessableError("Segmentation stage names must be unique")

    known_refs = {x.alias for x in pipeline.inputs}
    if pipeline.loader is not None:
        known_refs.add("LOADING")
    for stage in sorted(pipeline.segmentation_stages, key=lambda x: x.position):
        if stage.stage_name in known_refs:
            raise UnprocessableError(f"Stage name '{stage.stage_name}' conflicts with existing input alias")
        for alias in stage.input_aliases:
            if alias not in known_refs:
                raise UnprocessableError(
                    f"Unknown stage input alias '{alias}' in stage '{stage.stage_name}'",
                    details={"known_aliases": sorted(known_refs)},
                )
        known_refs.add(stage.stage_name)


def _validate_no_cycles(pipeline: PipelineCreate) -> None:
    graph: dict[str, set[str]] = defaultdict(set)
    for stage in pipeline.segmentation_stages:
        for alias in stage.input_aliases:
            graph[alias].add(stage.stage_name)

    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(node: str) -> None:
        if node in visiting:
            raise UnprocessableError("Circular dependency detected in pipeline stage graph")
        if node in visited:
            return
        visiting.add(node)
        for nxt in graph.get(node, set()):
            dfs(nxt)
        visiting.remove(node)
        visited.add(node)

    for node in list(graph.keys()):
        dfs(node)


def classify_pipeline_shape(pipeline: PipelineCreate) -> str:
    has_loader = pipeline.loader is not None
    has_segmentation = bool(pipeline.segmentation_stages)
    has_indexing = pipeline.indexing is not None

    if has_loader and has_segmentation and has_indexing:
        return "full"
    if has_loader and has_segmentation and not has_indexing:
        return "loading_segmentation"
    if has_loader and not has_segmentation and not has_indexing:
        return "loading_only"
    if not has_loader and has_segmentation and has_indexing:
        return "input_segmentation_indexing"
    if not has_loader and has_segmentation and not has_indexing:
        return "input_segmentation_only"
    if not has_loader and not has_segmentation and has_indexing:
        return "indexing_only"
    return "custom"


def validate_pipeline(pipeline: PipelineCreate, capabilities: dict[str, Any]) -> str:
    _validate_input_refs(pipeline)
    _validate_stages(pipeline)
    _validate_no_cycles(pipeline)

    if pipeline.loader is not None:
        _validate_params("loader", pipeline.loader.type, pipeline.loader.params, capabilities)

    for stage in pipeline.segmentation_stages:
        _validate_params("splitter", stage.splitter_type, stage.params, capabilities)

    if pipeline.indexing is not None:
        supported = set(capabilities.get("indexes", {}).get("index_types", []))
        if pipeline.indexing.index_type not in supported:
            raise UnprocessableError(
                f"Unknown index_type '{pipeline.indexing.index_type}'",
                details={"supported": sorted(supported)},
            )

    return classify_pipeline_shape(pipeline)
