from __future__ import annotations

from sqlalchemy import select

from app.core.database import SessionLocal
from app.models.entities import Artifact
from app.models.entities import ArtifactInput, Pipeline, PipelineIndexingConfig, Project
from app.services import jobs
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


def test_run_pipeline_job_persists_graph_segments_without_switching_index_source(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        jobs,
        "run_loader",
        lambda **_: {
            "kind": "document",
            "payload": [{"content": "doc body", "metadata": {"source": "loader"}}],
            "runtime_extras": {},
            "diagnostics": {},
        },
    )
    monkeypatch.setattr(
        jobs,
        "run_splitter",
        lambda **_: {
            "kind": "segment",
            "payload": [
                {
                    "content": "chunk body",
                    "metadata": {"segment_id": "chunk-1", "title": "Chunk One"},
                    "segment_id": "chunk-1",
                    "parent_id": None,
                    "level": 0,
                    "path": [],
                    "type": "text",
                    "original_format": "text",
                }
            ],
            "runtime_extras": {},
            "diagnostics": {},
        },
    )
    monkeypatch.setattr(
        jobs,
        "run_processor",
        lambda **_: {
            "kind": "none",
            "payload": [],
            "persisted_artifacts": [
                {
                    "artifact_kind": "graph_entity",
                    "payload": [
                        {
                            "content": "Entity: Probability\nType: Concept\nDescription: Likelihood of an event",
                            "metadata": {
                                "entity_id": "Probability",
                                "entity_label": "Probability",
                                "entity_type": "Concept",
                                "graph_artifact_type": "entity",
                                "source_segment_id": "chunk-1",
                            },
                            "segment_id": "graph_entity_probability",
                            "parent_id": "chunk-1",
                            "level": 0,
                            "path": [],
                            "type": "text",
                            "original_format": "text",
                        }
                    ],
                }
            ],
            "runtime_extras": {},
            "diagnostics": {"graph_entities_persisted": 1, "graph_edges_seen": 2},
        },
    )
    monkeypatch.setattr(jobs, "validate_indexing_params", lambda *args, **kwargs: None)

    def _fake_build_index(
        *,
        index_artifact_id,
        index_type,
        params,
        raw_segments,
        raw_parent_segments=None,
        runtime_extras=None,
        logical_collection_name=None,
        logical_docstore_name=None,
    ):
        captured["raw_segments"] = list(raw_segments)
        captured["raw_parent_segments"] = list(raw_parent_segments or [])
        captured["runtime_extras"] = dict(runtime_extras or {})
        return {
            "index_type": index_type,
            "params": dict(params),
            "logical_collection_name": logical_collection_name,
            "logical_docstore_name": logical_docstore_name,
            "dual_storage": False,
            "segments_indexed": len(raw_segments),
            "storage": {
                "version": 1,
                "backend": "filesystem",
                "vector_store": {
                    "provider": "chroma",
                    "persist_path": "data/indexes/chroma",
                    "collection_name": f"idx_{index_artifact_id.replace('-', '')}",
                },
                "doc_store": None,
            },
        }

    monkeypatch.setattr(jobs, "build_index", _fake_build_index)

    with SessionLocal() as db:
        project = Project(name="graph-persist")
        db.add(project)
        db.flush()

        pipeline = Pipeline(
            project_id=project.id,
            name="graph-pipeline",
            shape="full",
            definition={
                "loader": {"type": "TextLoader", "params": {}},
                "inputs": [],
                "stages": [
                    {
                        "stage_name": "docx_structure",
                        "stage_kind": "splitter",
                        "component_type": "RegexSplitter",
                        "params": {},
                        "input_aliases": ["LOADING"],
                        "position": 0,
                    },
                    {
                        "stage_name": "graph_entities",
                        "stage_kind": "processor",
                        "component_type": "EntityExtractor",
                        "params": {},
                        "input_aliases": ["docx_structure"],
                        "position": 1,
                    },
                ],
            },
        )
        db.add(pipeline)
        db.flush()

        db.add(
            PipelineIndexingConfig(
                pipeline_id=pipeline.id,
                index_type="chroma",
                params={},
                collection_name="graph-logical-index",
                docstore_name=None,
            )
        )
        db.commit()

        job = jobs.create_job(
            db,
            project_id=project.id,
            pipeline_id=pipeline.id,
            kind="run_pipeline",
            payload={},
        )
        project_id = project.id
        pipeline_id = pipeline.id
        job_id = job.id

    with SessionLocal() as db:
        finished = jobs.run_pipeline_job(db, job_id)

        assert finished.status == "succeeded"
        assert finished.result["artifacts_produced"]["document"] == 1
        assert finished.result["artifacts_produced"]["segment"] == 1
        assert finished.result["artifacts_produced"]["graph_entity"] == 1
        assert finished.result["stage_diagnostics"]["graph_entities"]["graph_entities_persisted"] == 1

        docx_structure_rows = db.execute(
            select(Artifact).where(
                Artifact.project_id == project_id,
                Artifact.pipeline_id == pipeline_id,
                Artifact.artifact_kind == "segment",
                Artifact.stage_name == "docx_structure",
            )
        ).scalars().all()
        graph_entity_rows = db.execute(
            select(Artifact).where(
                Artifact.project_id == project_id,
                Artifact.pipeline_id == pipeline_id,
                Artifact.artifact_kind == "graph_entity",
                Artifact.stage_name == "graph_entities",
            )
        ).scalars().all()
        index_row = db.execute(
            select(Artifact).where(
                Artifact.project_id == project_id,
                Artifact.pipeline_id == pipeline_id,
                Artifact.artifact_kind == "index",
            )
        ).scalar_one()

        assert len(docx_structure_rows) == 1
        assert len(graph_entity_rows) == 1
        assert graph_entity_rows[0].content_json["metadata"]["entity_id"] == "Probability"

        index_input_ids = db.execute(
            select(ArtifactInput.input_artifact_id).where(ArtifactInput.artifact_id == index_row.id)
        ).scalars().all()
        assert index_input_ids == [docx_structure_rows[0].id]

    assert captured["raw_segments"] == [
        {
            "content": "chunk body",
            "metadata": {"segment_id": "chunk-1", "title": "Chunk One"},
            "segment_id": "chunk-1",
            "parent_id": None,
            "level": 0,
            "path": [],
            "type": "text",
            "original_format": "text",
        }
    ]
