from __future__ import annotations

from typing import Any

from app.schemas.pipelines import PipelineCreate, PipelineValidationWarning
from app.services.capabilities import discover_capabilities


def _warning(
    code: str,
    message: str,
    *,
    path: str | None = None,
    details: dict[str, Any] | None = None,
) -> PipelineValidationWarning:
    return PipelineValidationWarning(code=code, message=message, path=path, details=details)


def _warn_on_component(
    warnings: list[PipelineValidationWarning],
    *,
    kind: str,
    type_name: str,
    params: dict[str, Any],
    path: str,
    inventory: dict[str, Any],
) -> None:
    capability = inventory.get(type_name)
    if capability is None:
        warnings.append(
            _warning(
                f"unknown_{kind}_type",
                f"{kind.title()} '{type_name}' was not discovered in the installed rag-lib",
                path=path,
                details={"available": sorted(inventory.keys())},
            )
        )
        return

    allowed_params = {item["name"]: item for item in capability.get("params", [])}
    if not allowed_params:
        return

    unknown = sorted(key for key in params if key not in allowed_params)
    if unknown:
        warnings.append(
            _warning(
                f"unknown_{kind}_params",
                f"{kind.title()} '{type_name}' has parameter(s) that were not discovered in the installed rag-lib signature",
                path=f"{path}.params",
                details={"unknown_keys": unknown, "allowed_keys": sorted(allowed_params.keys())},
            )
        )

    missing_required = sorted(
        item["name"]
        for item in capability.get("params", [])
        if item.get("required") and item["name"] not in params
    )
    if missing_required:
        warnings.append(
            _warning(
                f"missing_{kind}_params",
                f"{kind.title()} '{type_name}' is missing parameter(s) that appear required by the installed rag-lib signature",
                path=f"{path}.params",
                details={"missing_keys": missing_required},
            )
        )


def validate_pipeline_advisory(pipeline: PipelineCreate) -> list[PipelineValidationWarning]:
    try:
        capabilities = discover_capabilities()
    except Exception as exc:
        return [
            _warning(
                "capability_discovery_failed",
                "Capability discovery failed; execution may still succeed because rag-lib remains the runtime authority",
                details={"error": f"{type(exc).__name__}: {exc}"},
            )
        ]

    warnings: list[PipelineValidationWarning] = []
    strict = capabilities.get("strict", {})
    advisory = capabilities.get("advisory", {})

    for item in advisory.get("discovery_warnings", []):
        warnings.append(
            _warning(
                "capability_discovery_warning",
                str(item),
            )
        )

    if pipeline.loader is not None:
        _warn_on_component(
            warnings,
            kind="loader",
            type_name=pipeline.loader.type,
            params=pipeline.loader.params,
            path="loader",
            inventory=strict.get("loaders", {}),
        )

    for index, stage in enumerate(pipeline.ordered_stages()):
        _warn_on_component(
            warnings,
            kind=stage.stage_kind,
            type_name=stage.component_type,
            params=stage.params,
            path=f"stages[{index}]",
            inventory=strict.get(f"{stage.stage_kind}s", {}),
        )

    if pipeline.indexing is not None:
        supported = set((advisory.get("indexes") or {}).get("index_types") or [])
        if supported and pipeline.indexing.index_type not in supported:
            warnings.append(
                _warning(
                    "unknown_index_type",
                    f"Index type '{pipeline.indexing.index_type}' was not discovered in the installed rag-lib capability metadata",
                    path="indexing.index_type",
                    details={"available": sorted(supported)},
                )
            )

    return warnings
