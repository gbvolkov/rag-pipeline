from __future__ import annotations

from app.models.entities import Artifact
from app.services.jobs import (
    StageOutput,
    _artifact_to_document_payload,
    _resolve_parent_segments_from_stage_outputs,
)


def test_artifact_to_document_payload_preserves_full_content_json():
    artifact = Artifact(
        project_id="p1",
        pipeline_id="pl1",
        artifact_kind="document",
        artifact_key="doc_0",
        version=1,
        content_text="unused",
        content_json={
            "id": "doc-id-1",
            "content": "hello",
            "metadata": {"source": "loader"},
            "extra": {"page": 2},
        },
        metadata_json={"source": "loader"},
    )
    payload = _artifact_to_document_payload(artifact)
    assert payload["id"] == "doc-id-1"
    assert payload["content"] == "hello"
    assert payload["metadata"] == {"source": "loader"}
    assert payload["extra"] == {"page": 2}


def test_resolve_parent_segments_from_stage_outputs_uses_parent_ids_and_artifact_ids():
    raw_segments = [
        {"segment_id": "child-1", "parent_id": "parent-a", "content": "child one", "metadata": {}},
        {"segment_id": "child-2", "parent_id": "parent-b", "content": "child two", "metadata": {}},
    ]
    stage_outputs = [
        StageOutput(
            kind="segment",
            payload=[
                {"segment_id": "parent-a", "content": "parent A", "metadata": {"segment_id": "parent-a"}},
                {"segment_id": "parent-b", "content": "parent B", "metadata": {"segment_id": "parent-b"}},
            ],
            artifact_ids=["artifact-parent-a", "artifact-parent-b"],
        )
    ]

    parents, parent_artifact_ids = _resolve_parent_segments_from_stage_outputs(raw_segments, stage_outputs)

    assert [item["segment_id"] for item in parents] == ["parent-a", "parent-b"]
    assert parent_artifact_ids == ["artifact-parent-a", "artifact-parent-b"]
