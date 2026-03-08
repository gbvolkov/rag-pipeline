from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class RetrievalQueryPlan:
    name: str
    query: str
    top_k: int
    strict_match: bool


@dataclass(slots=True)
class RetrievalPlan:
    name: str
    source_kind: str
    source_stage_name: str | None
    retriever_type: str
    retriever_params: dict[str, Any]
    requires_session: bool
    queries: list[RetrievalQueryPlan]


@dataclass(slots=True)
class PipelineRunSpec:
    run_name: str
    pipeline_create_payload: dict[str, Any]
    run_payload_template: dict[str, Any] = field(default_factory=dict)
    retrievals: list[RetrievalPlan] = field(default_factory=list)


@dataclass(slots=True)
class PipelineExampleSpec:
    example_id: str
    source_example_file: str
    input_mode: str
    input_spec: dict[str, Any]
    runs: list[PipelineRunSpec]
    expected_outcome: str
    notes: str


@dataclass(slots=True)
class PipelineExampleManifest:
    version: str
    examples: list[PipelineExampleSpec]


def _load_structured_file(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        payload = yaml.safe_load(raw)
    elif suffix == ".json":
        payload = json.loads(raw)
    else:
        raise ValueError(f"Unsupported manifest extension '{path.suffix}'")
    if not isinstance(payload, dict):
        raise ValueError("Manifest root must be an object")
    return payload


def _require(mapping: dict[str, Any], key: str) -> Any:
    if key not in mapping:
        raise ValueError(f"Missing required key '{key}' in manifest entry")
    return mapping[key]


def _as_dict(value: Any, key: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected '{key}' to be an object")
    return value


def _as_list(value: Any, key: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"Expected '{key}' to be a list")
    return value


def _as_list_of_dicts(value: Any, key: str) -> list[dict[str, Any]]:
    items = _as_list(value, key)
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"Expected '{key}[{idx}]' to be an object")
        out.append(item)
    return out


def _as_str(value: Any, key: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Expected '{key}' to be a non-empty string")
    return value


def _as_bool(value: Any, key: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected '{key}' to be a boolean")
    return value


def _as_int(value: Any, key: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Expected '{key}' to be an integer")
    return value


def _parse_query_plan(raw: dict[str, Any]) -> RetrievalQueryPlan:
    return RetrievalQueryPlan(
        name=_as_str(raw.get("name") or raw.get("query") or "query", "queries[].name"),
        query=_as_str(_require(raw, "query"), "query"),
        top_k=_as_int(_require(raw, "top_k"), "top_k"),
        strict_match=_as_bool(_require(raw, "strict_match"), "strict_match"),
    )


def _parse_retrieval_plan(raw: dict[str, Any]) -> RetrievalPlan:
    source = _as_dict(_require(raw, "source"), "source")
    source_kind = _as_str(_require(source, "kind"), "source.kind")
    if source_kind not in {"index", "stage"}:
        raise ValueError(f"Unsupported retrieval source.kind '{source_kind}'")
    source_stage_name = source.get("stage_name")
    if source_kind == "stage":
        source_stage_name = _as_str(_require(source, "stage_name"), "source.stage_name")
    elif source_stage_name is not None:
        raise ValueError("source.stage_name is only valid for source.kind='stage'")

    create = _as_dict(_require(raw, "create"), "create")
    raw_queries = _as_list_of_dicts(_require(raw, "queries"), "queries")
    if not raw_queries:
        raise ValueError("retrieval plan must define at least one query")

    return RetrievalPlan(
        name=_as_str(_require(raw, "name"), "name"),
        source_kind=source_kind,
        source_stage_name=source_stage_name,
        retriever_type=_as_str(_require(create, "retriever_type"), "create.retriever_type"),
        retriever_params=_as_dict(create.get("params", {}), "create.params"),
        requires_session=_as_bool(raw.get("requires_session", False), "requires_session"),
        queries=[_parse_query_plan(item) for item in raw_queries],
    )


def _parse_run_spec(raw: dict[str, Any], *, fallback_name: str) -> PipelineRunSpec:
    if "run_name" not in raw:
        raise ValueError("Missing required key 'run_name' in run spec")
    retrievals = raw.get("retrievals", [])
    return PipelineRunSpec(
        run_name=_as_str(_require(raw, "run_name"), "run_name"),
        pipeline_create_payload=_as_dict(_require(raw, "pipeline_create_payload"), "pipeline_create_payload"),
        run_payload_template=_as_dict(raw.get("run_payload_template", {}), "run_payload_template"),
        retrievals=[_parse_retrieval_plan(item) for item in _as_list_of_dicts(retrievals, "retrievals")],
    )


def _parse_runs(raw: dict[str, Any], example_id: str) -> list[PipelineRunSpec]:
    raw_runs = _require(raw, "runs")
    runs = [_parse_run_spec(item, fallback_name=example_id) for item in _as_list_of_dicts(raw_runs, "runs")]
    if not runs:
        raise ValueError(f"Example '{example_id}' must define at least one run")
    return runs


def load_manifest(manifest_path: Path) -> PipelineExampleManifest:
    payload = _load_structured_file(manifest_path)
    version = _as_str(_require(payload, "version"), "version")
    raw_examples = _require(payload, "examples")
    if not isinstance(raw_examples, list):
        raise ValueError("Manifest 'examples' must be a list")

    examples: list[PipelineExampleSpec] = []
    ids: set[str] = set()
    for idx, raw in enumerate(raw_examples):
        if not isinstance(raw, dict):
            raise ValueError(f"Manifest examples[{idx}] must be an object")
        example_id = _as_str(_require(raw, "example_id"), "example_id")
        if example_id in ids:
            raise ValueError(f"Duplicate example_id '{example_id}' in manifest")
        ids.add(example_id)

        input_mode = _as_str(_require(raw, "input_mode"), "input_mode")
        if input_mode not in {"file", "url", "documents", "segments"}:
            raise ValueError(f"Unsupported input_mode '{input_mode}' for '{example_id}'")
        expected_outcome = _as_str(_require(raw, "expected_outcome"), "expected_outcome")
        if expected_outcome not in {"success", "error"}:
            raise ValueError(f"Unsupported expected_outcome '{expected_outcome}' for '{example_id}'")

        examples.append(
            PipelineExampleSpec(
                example_id=example_id,
                source_example_file=_as_str(_require(raw, "source_example_file"), "source_example_file"),
                input_mode=input_mode,
                input_spec=_as_dict(_require(raw, "input_spec"), "input_spec"),
                runs=_parse_runs(raw, example_id),
                expected_outcome=expected_outcome,
                notes=_as_str(_require(raw, "notes"), "notes"),
            )
        )
    return PipelineExampleManifest(version=version, examples=examples)


def _resolve_file_path(example_docs_root: Path, relative_file: str) -> Path:
    candidate = (example_docs_root / relative_file).resolve()
    root = example_docs_root.resolve()
    if root not in candidate.parents and candidate != root:
        raise ValueError("input_spec.file escapes example_docs root")
    if not candidate.exists():
        raise FileNotFoundError(f"Input file does not exist: {candidate}")
    if not candidate.is_file():
        raise ValueError(f"Input path is not a file: {candidate}")
    return candidate


def _build_inline_payload_records(input_spec: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = _require(input_spec, key)
    records = _as_list_of_dicts(value, f"input_spec.{key}")
    return copy.deepcopy(records)


def build_run_payload(
    spec: PipelineExampleSpec,
    run_spec: PipelineRunSpec,
    example_docs_root: Path,
) -> dict[str, Any]:
    payload = copy.deepcopy(run_spec.run_payload_template)
    if spec.input_mode == "url":
        payload["url"] = _as_str(_require(spec.input_spec, "url"), "input_spec.url")
        return payload

    if spec.input_mode == "file":
        file_value = _as_str(_require(spec.input_spec, "file"), "input_spec.file")
        full_path = _resolve_file_path(example_docs_root, file_value)
        payload["file_name"] = full_path.name
        payload["upload_file_path"] = str(full_path)
        return payload

    if spec.input_mode == "documents":
        payload["documents"] = _build_inline_payload_records(spec.input_spec, "documents")
        return payload

    if spec.input_mode == "segments":
        payload["segments"] = _build_inline_payload_records(spec.input_spec, "segments")
        return payload

    raise ValueError(f"Unsupported input_mode '{spec.input_mode}'")


def select_examples(
    manifest: PipelineExampleManifest,
    example_ids: list[str] | None,
) -> list[PipelineExampleSpec]:
    if not example_ids:
        return list(manifest.examples)
    lookup = {spec.example_id: spec for spec in manifest.examples}
    missing = [example_id for example_id in example_ids if example_id not in lookup]
    if missing:
        raise ValueError(f"Unknown example_id(s): {', '.join(missing)}")
    return [lookup[example_id] for example_id in example_ids]
