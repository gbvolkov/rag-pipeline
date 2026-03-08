from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.errors import UnprocessableError
from app.schemas.pipelines import PipelineCreate
from app.services.capabilities import discover_capabilities
from app.services.pipeline_advisory_validator import validate_pipeline_advisory
from app.services.pipeline_validator import validate_pipeline
from app.services.runtime_objects import RuntimeObjectError, validate_runtime_object_specs
from scripts.lib.pipeline_example_manifest import PipelineExampleSpec, load_manifest


MANIFEST_PATH = Path("examples/pipeline_examples/manifest.v1.yaml")
CATALOG_PATH = Path("docs/example-profiles/catalog.v1.json")
SUPPORTED_STATUSES = frozenset({"implemented", "expected_error"})


@dataclass(frozen=True)
class DiscoveredExample:
    example_path: str
    profile_id: str
    family: str
    support_status: str
    implemented: bool
    notes: str | None = None


def profile_id_for(example_id: str) -> str:
    return f"profile::{example_id}"


def _manifest_examples() -> list[PipelineExampleSpec]:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Pipeline example manifest does not exist: {MANIFEST_PATH}")
    return load_manifest(MANIFEST_PATH).examples


def _validate_retrieval_plan(
    example: PipelineExampleSpec,
    run_name: str,
    pipeline_payload: dict[str, Any],
    retrieval: Any,
    capabilities: dict[str, Any],
) -> None:
    strict = capabilities.get("strict", {})
    available = set((strict.get("retrievers", {}).get("classes") or {}).keys())
    available.update((strict.get("retrievers", {}).get("factories") or {}).keys())
    if retrieval.retriever_type not in available:
        raise UnprocessableError(
            f"Retriever '{retrieval.retriever_type}' is unavailable for example '{example.example_id}'",
            details={"available": sorted(available), "run_name": run_name},
        )
    try:
        validate_runtime_object_specs(
            retrieval.retriever_params,
            path=f"retriever.{retrieval.retriever_type}.params",
        )
    except RuntimeObjectError as exc:
        raise UnprocessableError(str(exc)) from exc

    if retrieval.source_kind == "index" and "indexing" not in pipeline_payload:
        raise UnprocessableError(
            f"Example '{example.example_id}' run '{run_name}' uses index retrieval without indexing",
        )

    if retrieval.source_kind == "stage":
        stage_names = {
            stage.get("stage_name")
            for stage in pipeline_payload.get("stages", [])
            if isinstance(stage, dict)
        }
        if retrieval.source_stage_name not in stage_names:
            raise UnprocessableError(
                f"Example '{example.example_id}' run '{run_name}' references unknown source stage '{retrieval.source_stage_name}'",
                details={"stage_names": sorted(name for name in stage_names if isinstance(name, str))},
            )


def _evaluate_example(spec: PipelineExampleSpec, capabilities: dict[str, Any]) -> tuple[str, str]:
    try:
        for run in spec.runs:
            pipeline = PipelineCreate.model_validate(run.pipeline_create_payload)
            validate_pipeline(pipeline)
            warnings = validate_pipeline_advisory(pipeline)
            if warnings:
                raise UnprocessableError(
                    "Example requires capabilities that are unavailable in the installed rag-lib",
                    details={"warnings": [warning.model_dump() for warning in warnings]},
                )
            for retrieval in run.retrievals:
                _validate_retrieval_plan(spec, run.run_name, run.pipeline_create_payload, retrieval, capabilities)
    except Exception as exc:
        if spec.expected_outcome == "error":
            return "expected_error", f"Expected strict rejection: {exc}"
        return "unsupported", str(exc)

    if spec.expected_outcome == "error":
        return "unsupported", "Manifest expects an error, but the current API validates the example successfully"
    return "implemented", "Manifest example validates against installed rag-lib capabilities"


def discover_examples() -> list[DiscoveredExample]:
    capabilities = discover_capabilities()
    discovered: list[DiscoveredExample] = []
    for spec in _manifest_examples():
        support_status, notes = _evaluate_example(spec, capabilities)
        discovered.append(
            DiscoveredExample(
                example_path=spec.source_example_file,
                profile_id=profile_id_for(spec.example_id),
                family=spec.example_id,
                support_status=support_status,
                implemented=support_status in SUPPORTED_STATUSES,
                notes=notes,
            )
        )
    return discovered


def get_example_capability_matrix() -> dict[str, Any]:
    items = []
    covered = 0
    for item in discover_examples():
        if item.implemented:
            covered += 1
        items.append(
            {
                "example_path": item.example_path,
                "profile_id": item.profile_id,
                "family": item.family,
                "support_status": item.support_status,
                "implemented": item.implemented,
                "notes": item.notes,
            }
        )
    return {
        "generated_at": datetime.now(tz=UTC),
        "total_examples": len(items),
        "covered_examples": covered,
        "items": items,
    }


def write_catalog_file() -> None:
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    catalog = {
        "version": "v2",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "profiles": [
            {
                "profile_id": item.profile_id,
                "family": item.family,
                "examples": [item.example_path],
                "support_status": item.support_status,
                "implemented": item.implemented,
                "notes": item.notes,
            }
            for item in discover_examples()
        ],
    }
    CATALOG_PATH.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
