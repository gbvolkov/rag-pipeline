from __future__ import annotations

from collections import defaultdict

from app.core.errors import UnprocessableError
from app.schemas.pipelines import PipelineCreate
from app.services.runtime_objects import RuntimeObjectError, validate_runtime_object_specs


def _validate_component_params(kind: str, type_name: str, params: dict[str, object]) -> None:
    try:
        validate_runtime_object_specs(params, path=f"{kind}.{type_name}.params")
    except RuntimeObjectError as exc:
        raise UnprocessableError(str(exc)) from exc


def _validate_input_refs(pipeline: PipelineCreate) -> None:
    if pipeline.runtime_input is not None and pipeline.inputs:
        raise UnprocessableError("runtime_input and inputs are mutually exclusive")
    if not pipeline.inputs:
        return

    aliases = [item.alias for item in pipeline.inputs]
    if len(set(aliases)) != len(aliases):
        raise UnprocessableError("Input aliases must be unique")

    kinds = {item.artifact_kind for item in pipeline.inputs}
    if len(kinds) > 1:
        raise UnprocessableError("Input artifact kinds must be homogeneous")

    merged = [item.alias for item in pipeline.inputs if item.alias == "__merged__"]
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
    stages = pipeline.ordered_stages()
    names = [stage.stage_name for stage in stages]
    if len(set(names)) != len(names):
        raise UnprocessableError("Pipeline stage names must be unique")

    known_refs = {item.alias for item in pipeline.inputs}
    if pipeline.loader is not None:
        known_refs.add("LOADING")
    if pipeline.runtime_input is not None:
        known_refs.add(pipeline.runtime_input.alias)

    for stage in stages:
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
    for stage in pipeline.ordered_stages():
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
    has_runtime_input = pipeline.runtime_input is not None
    has_inputs = bool(pipeline.inputs)
    has_stages = bool(pipeline.stages)
    has_indexing = pipeline.indexing is not None

    if has_loader and has_stages and has_indexing:
        return "full"
    if has_loader and has_stages and not has_indexing:
        return "loading_stages"
    if has_loader and not has_stages and not has_indexing:
        return "loading_only"
    if has_runtime_input and has_stages and has_indexing:
        return "runtime_input_stages_indexing"
    if has_runtime_input and has_stages and not has_indexing:
        return "runtime_input_stages_only"
    if has_inputs and has_stages and has_indexing:
        return "input_stages_indexing"
    if has_inputs and has_stages and not has_indexing:
        return "input_stages_only"
    if (has_inputs or has_runtime_input) and not has_stages and has_indexing:
        return "indexing_only"
    return "custom"


def validate_pipeline(pipeline: PipelineCreate) -> str:
    _validate_input_refs(pipeline)
    _validate_stages(pipeline)
    _validate_no_cycles(pipeline)

    if pipeline.loader is not None:
        _validate_component_params("loader", pipeline.loader.type, pipeline.loader.params)

    for stage in pipeline.ordered_stages():
        _validate_component_params(stage.stage_kind, stage.component_type, stage.params)

    if pipeline.indexing is not None:
        try:
            validate_runtime_object_specs(
                pipeline.indexing.params,
                path=f"indexing.{pipeline.indexing.index_type}.params",
            )
        except RuntimeObjectError as exc:
            raise UnprocessableError(str(exc)) from exc

    return classify_pipeline_shape(pipeline)
