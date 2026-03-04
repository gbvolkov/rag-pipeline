from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


@dataclass(slots=True)
class RetrievalPlan:
    retriever_type: str
    retriever_params: dict[str, Any]
    requires_session: bool
    query: str
    top_k: int
    strict_match: bool


@dataclass(slots=True)
class PipelineExampleSpec:
    example_id: str
    source_example_file: str
    input_mode: str
    input_spec: dict[str, Any]
    pipeline_create_payload: dict[str, Any]
    run_payload_template: dict[str, Any]
    retrieval_plan: RetrievalPlan
    expected_outcome: str
    notes: str


@dataclass(slots=True)
class PipelineExampleManifest:
    version: str
    examples: list[PipelineExampleSpec]


def _load_structured_file(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if yaml is not None:
        payload = yaml.safe_load(raw)
        if isinstance(payload, dict):
            return payload
    payload = json.loads(raw)
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


def _parse_retrieval_plan(raw: dict[str, Any]) -> RetrievalPlan:
    return RetrievalPlan(
        retriever_type=_as_str(_require(raw, "retriever_type"), "retriever_type"),
        retriever_params=_as_dict(_require(raw, "retriever_params"), "retriever_params"),
        requires_session=_as_bool(_require(raw, "requires_session"), "requires_session"),
        query=_as_str(_require(raw, "query"), "query"),
        top_k=_as_int(_require(raw, "top_k"), "top_k"),
        strict_match=_as_bool(_require(raw, "strict_match"), "strict_match"),
    )


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
        if input_mode not in {"text", "file", "url"}:
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
                pipeline_create_payload=_as_dict(_require(raw, "pipeline_create_payload"), "pipeline_create_payload"),
                run_payload_template=_as_dict(_require(raw, "run_payload_template"), "run_payload_template"),
                retrieval_plan=_parse_retrieval_plan(
                    _as_dict(_require(raw, "retrieval_plan"), "retrieval_plan")
                ),
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


def build_run_payload(spec: PipelineExampleSpec, example_docs_root: Path) -> dict[str, Any]:
    payload = dict(spec.run_payload_template)
    if spec.input_mode == "text":
        text_value = _as_str(_require(spec.input_spec, "text"), "input_spec.text")
        payload["text"] = text_value
        return payload

    if spec.input_mode == "url":
        url_value = _as_str(_require(spec.input_spec, "url"), "input_spec.url")
        payload["url"] = url_value
        return payload

    if spec.input_mode == "file":
        file_value = _as_str(_require(spec.input_spec, "file"), "input_spec.file")
        full_path = _resolve_file_path(example_docs_root, file_value)
        payload["file_name"] = full_path.name
        payload["file_content_b64"] = base64.b64encode(full_path.read_bytes()).decode("ascii")
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

