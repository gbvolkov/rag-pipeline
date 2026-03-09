from __future__ import annotations

import json
from pathlib import Path


def _storage_descriptor(index_id: str, *, dual_storage: bool = False) -> dict[str, object]:
    storage: dict[str, object] = {
        "version": 1,
        "backend": "filesystem",
        "vector_store": {
            "provider": "chroma",
            "persist_path": str(Path("data/indexes/chroma")),
            "collection_name": f"idx_{index_id.replace('-', '')}",
        },
        "doc_store": None,
    }
    if dual_storage:
        storage["doc_store"] = {
            "provider": "local_pickle",
            "file_path": str(Path("data/indexes/docstore") / f"{index_id}.pkl"),
        }
    return storage


def _index_payload(
    index_id: str,
    *,
    params: dict[str, object],
    logical_collection_name: str | None,
    logical_docstore_name: str | None = None,
    dual_storage: bool = False,
) -> dict[str, object]:
    return {
        "index_type": "chroma",
        "params": dict(params),
        "logical_collection_name": logical_collection_name,
        "logical_docstore_name": logical_docstore_name,
        "dual_storage": dual_storage,
        "segments_indexed": 1,
        "storage": _storage_descriptor(index_id, dual_storage=dual_storage),
    }


def test_capabilities_endpoints(client):
    resp = client.get("/api/v1/capabilities")
    assert resp.status_code == 200
    data = resp.json()
    assert "matrix" in data
    assert "strict" in data["matrix"]
    assert "advisory" in data["matrix"]
    assert "loaders" in data["matrix"]["strict"]
    assert "splitters" in data["matrix"]["strict"]
    assert "SemanticChunker" in data["matrix"]["strict"]["splitters"]
    assert "source_root" not in data["matrix"]
    assert "discovery_warnings" in data["matrix"]["advisory"]

    ex = client.get("/api/v1/capabilities/examples")
    assert ex.status_code == 404


def test_run_submit_rejects_example_profile_id(client):
    pid = client.post("/api/v1/projects", json={"name": "prod-api"}).json()["id"]
    pipeline_payload = {
        "name": "text-flow",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "stages": [],
    }
    pl_resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=pipeline_payload)
    assert pl_resp.status_code == 201
    plid = pl_resp.json()["id"]

    run_resp = client.post(
        f"/api/v1/projects/{pid}/pipelines/{plid}/runs",
        json={"example_profile_id": "profile::01_text_basic"},
    )
    assert run_resp.status_code == 422
    assert "example_profile_id is unsupported" in run_resp.json()["message"]


def test_pipeline_creation_returns_advisory_warnings_for_unknown_loader_params(client):
    pid = client.post("/api/v1/projects", json={"name": "p1"}).json()["id"]
    payload = {
        "name": "bad-pipeline",
        "loader": {"type": "TextLoader", "params": {"unknown_key": 1}},
        "inputs": [],
        "stages": [],
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["shape"] == "loading_only"
    assert any(warning["code"] == "unknown_loader_params" for warning in body["validation_warnings"])


def test_pipeline_validate_endpoint_returns_shape_and_warnings(client):
    pid = client.post("/api/v1/projects", json={"name": "p-validate"}).json()["id"]
    payload = {
        "name": "warn-pipeline",
        "loader": {"type": "TextLoader", "params": {"unknown_key": 1}},
        "inputs": [],
        "stages": [],
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines/validate", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["shape"] == "loading_only"
    assert any(warning["code"] == "unknown_loader_params" for warning in body["validation_warnings"])


def test_pipeline_creation_allows_unknown_loader_type_but_run_fails(client):
    pid = client.post("/api/v1/projects", json={"name": "p-unknown-loader"}).json()["id"]
    payload = {
        "name": "unknown-loader",
        "loader": {"type": "DefinitelyMissingLoader", "params": {}},
        "inputs": [],
        "stages": [],
    }
    create_resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert create_resp.status_code == 201
    pipeline = create_resp.json()
    assert any(warning["code"] == "unknown_loader_type" for warning in pipeline["validation_warnings"])

    run_resp = client.post(
        f"/api/v1/projects/{pid}/pipelines/{pipeline['id']}/runs",
        data={"form_payload": json.dumps({})},
        files={"file": ("input.txt", b"alpha beta gamma", "text/plain")},
    )
    assert run_resp.status_code == 202
    job_id = run_resp.json()["job_id"]

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "failed"
    assert "Unknown loader type 'DefinitelyMissingLoader'" in job["error"]["message"]


def test_pipeline_validation_rejects_legacy_segmentation_stages_field(client):
    pid = client.post("/api/v1/projects", json={"name": "p-legacy-stages"}).json()["id"]
    payload = {
        "name": "legacy-pipeline",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "segmentation_stages": [],
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert resp.status_code == 422
    body = resp.json()
    assert body["message"] == "Validation failed"
    assert any(error["loc"][-1] == "segmentation_stages" for error in body["details"]["errors"])


def test_pipeline_validation_rejects_legacy_factory_specs(client):
    pid = client.post("/api/v1/projects", json={"name": "p-legacy-factory"}).json()["id"]
    payload = {
        "name": "legacy-factory",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "stages": [
            {
                "stage_name": "enriched",
                "stage_kind": "processor",
                "component_type": "SegmentEnricher",
                "params": {
                    "llm": {
                        "factory": "create_llm",
                        "params": {"provider": "openai", "model_name": "gpt-4.1-nano"},
                    }
                },
                "input_aliases": ["LOADING"],
                "position": 0,
            }
        ],
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert resp.status_code == 422
    assert "legacy runtime spec key" in resp.json()["message"].lower()


def test_pipeline_validation_rejects_managed_chroma_physical_storage_params(client):
    pid = client.post("/api/v1/projects", json={"name": "p-physical-keys"}).json()["id"]
    payload = {
        "name": "bad-chroma-storage",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "stages": [],
        "indexing": {
            "index_type": "chroma",
            "params": {
                "cleanup": True,
                "collection_name": "physical_collection",
                "doc_store_path": "./data/docstore/physical.pkl",
            },
            "collection_name": "logical_collection",
            "docstore_name": "logical_docstore",
        },
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert resp.status_code == 422
    assert "physical storage params" in resp.json()["message"].lower()


def test_pipeline_validation_rejects_list_pair_regex_hierarchy_patterns(client):
    pid = client.post("/api/v1/projects", json={"name": "p-regex-hierarchy-patterns"}).json()["id"]
    payload = {
        "name": "bad-regex-hierarchy",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "stages": [
            {
                "stage_name": "structured",
                "stage_kind": "splitter",
                "component_type": "RegexHierarchySplitter",
                "params": {
                    "patterns": [
                        [1, r"^\s*#\s+(.+)$"],
                        [2, r"^\s*##\s+(.+)$"],
                    ]
                },
                "input_aliases": ["LOADING"],
                "position": 0,
            }
        ],
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert resp.status_code == 422
    body = resp.json()
    assert "regexhierarchysplitter params.patterns must use object entries" in body["message"].lower()
    assert body["details"]["list_pair_indexes"] == [0, 1]


def test_loader_segmentation_run_flow(client):
    from app.core.config import get_settings

    pid = client.post("/api/v1/projects", json={"name": "p2"}).json()["id"]
    pipeline_payload = {
        "name": "text-flow",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "stages": [
            {
                "stage_name": "s1",
                "stage_kind": "splitter",
                "component_type": "RecursiveCharacterTextSplitter",
                "params": {"chunk_size": 16, "chunk_overlap": 0},
                "input_aliases": ["LOADING"],
                "position": 0,
            }
        ],
    }
    pl_resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=pipeline_payload)
    assert pl_resp.status_code == 201
    plid = pl_resp.json()["id"]

    run_resp = client.post(
        f"/api/v1/projects/{pid}/pipelines/{plid}/runs",
        data={"form_payload": json.dumps({})},
        files={"file": ("input.txt", b"alpha beta gamma delta epsilon zeta eta theta iota", "text/plain")},
    )
    assert run_resp.status_code == 202
    job_id = run_resp.json()["job_id"]

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "succeeded"
    assert "_uploads" in job["payload"]["uploaded_file_path"]
    assert str(get_settings().local_blob_root).replace("\\", "/") in job["payload"]["uploaded_file_path"].replace("\\", "/")
    assert job["result"]["artifacts_produced"]["document"] == 1
    assert job["result"]["artifacts_produced"]["segment"] > 0

    docs = client.get(f"/api/v1/projects/{pid}/pipelines/{plid}/documents").json()
    segs = client.get(f"/api/v1/projects/{pid}/pipelines/{plid}/segments/s1").json()
    assert docs["total"] == 1
    assert segs["total"] > 0


def test_silent_empty_loader_result_fails_job(client, monkeypatch):
    from app.services import jobs

    pid = client.post("/api/v1/projects", json={"name": "p-empty-loader"}).json()["id"]
    pipeline_payload = {
        "name": "text-flow",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "stages": [],
    }
    pl_resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=pipeline_payload)
    assert pl_resp.status_code == 201
    plid = pl_resp.json()["id"]

    monkeypatch.setattr(
        jobs,
        "run_loader",
        lambda **kwargs: {"kind": "document", "payload": [], "runtime_extras": {}, "diagnostics": {}},
    )

    run_resp = client.post(
        f"/api/v1/projects/{pid}/pipelines/{plid}/runs",
        data={"form_payload": json.dumps({})},
        files={"file": ("input.txt", b"alpha beta gamma", "text/plain")},
    )
    assert run_resp.status_code == 202
    job_id = run_resp.json()["job_id"]

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "failed"
    assert "returned no documents" in job["error"]["message"]
    assert job["error"]["rag_lib_exception_type"] == "SilentEmptyResult"


def test_retriever_creation_rehydrates_missing_index_runtime(monkeypatch):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, ArtifactInput, Pipeline, PipelineIndexingConfig, Project
    from app.services import jobs
    from app.services.jobs import create_retriever
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

    INDEX_RUNTIME_REGISTRY.clear()
    restored: dict[str, object] = {}

    def _fake_restore_index_runtime(**kwargs):
        restored.update(kwargs)
        INDEX_RUNTIME_REGISTRY[kwargs["index_artifact_id"]] = {
            "vector_store": object(),
            "doc_store": None,
            "embeddings": object(),
            "segments": list(kwargs["raw_segments"]),
            "parent_segments": list(kwargs.get("raw_parent_segments") or []),
            "params": kwargs["params"],
            "runtime_extras": {},
        }

    monkeypatch.setattr(jobs, "restore_index_runtime", _fake_restore_index_runtime)
    monkeypatch.setattr(jobs, "create_retriever_runtime", lambda *args, **kwargs: None)

    with SessionLocal() as db:
        project = Project(name="p3")
        db.add(project)
        db.flush()

        pipeline = Pipeline(
            project_id=project.id,
            name="index-flow",
            description=None,
            shape="loader_indexing",
            immutable=True,
            deleted=False,
            definition={"loader": {"type": "TextLoader", "params": {}}},
        )
        db.add(pipeline)
        db.flush()

        db.add(
            PipelineIndexingConfig(
                pipeline_id=pipeline.id,
                index_type="chroma",
                params={"cleanup": True},
                collection_name="rehydration_test",
                docstore_name=None,
            )
        )
        db.flush()

        seg = Artifact(
            project_id=project.id,
            pipeline_id=pipeline.id,
            job_id=None,
            artifact_kind="segment",
            stage_name="chunks",
            artifact_key="seg_0",
            version=1,
            content_text="alpha",
            content_json={"content": "alpha", "metadata": {"segment_id": "seg-1"}, "segment_id": "seg-1"},
            metadata_json={"segment_id": "seg-1"},
        )
        db.add(seg)
        db.flush()

        index_artifact_id = "00000000-0000-0000-0000-000000000111"
        index_metadata = _index_payload(
            index_artifact_id,
            params={"cleanup": True},
            logical_collection_name="rehydration_test",
        )
        index_artifact = Artifact(
            id=index_artifact_id,
            project_id=project.id,
            pipeline_id=pipeline.id,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="rehydration_test",
            version=1,
            content_text=None,
            content_json=index_metadata,
            metadata_json=index_metadata,
        )
        db.add(index_artifact)
        db.flush()
        db.add(ArtifactInput(artifact_id=index_artifact.id, input_artifact_id=seg.id))
        db.commit()

        retriever = create_retriever(
            db,
            project_id=project.id,
            index_artifact_id=index_artifact.id,
            source_artifact_ids=None,
            retriever_type="create_vector_retriever",
            params={"top_k": 4},
        )

        assert retriever.index_artifact_id == index_artifact.id
        assert restored["index_artifact_id"] == index_artifact.id
        assert restored["index_type"] == "chroma"
        assert restored["params"] == {"cleanup": True}
        assert restored["logical_collection_name"] == "rehydration_test"
        assert restored["storage"] == index_metadata["storage"]
        assert restored["raw_segments"] == [
            {"content": "alpha", "metadata": {"segment_id": "seg-1"}, "segment_id": "seg-1"}
        ]


def test_retriever_creation_rejects_index_restore_without_lineage_segments():
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Pipeline, PipelineIndexingConfig, Project
    from app.services.jobs import create_retriever
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

    INDEX_RUNTIME_REGISTRY.clear()

    with SessionLocal() as db:
        project = Project(name="p3-no-lineage")
        db.add(project)
        db.flush()

        pipeline = Pipeline(
            project_id=project.id,
            name="index-flow",
            description=None,
            shape="loader_indexing",
            immutable=True,
            deleted=False,
            definition={"loader": {"type": "TextLoader", "params": {}}},
        )
        db.add(pipeline)
        db.flush()

        db.add(
            PipelineIndexingConfig(
                pipeline_id=pipeline.id,
                index_type="chroma",
                params={"cleanup": True},
                collection_name="restore_without_lineage",
                docstore_name=None,
            )
        )
        db.flush()

        index_artifact_id = "00000000-0000-0000-0000-000000000222"
        index_metadata = _index_payload(
            index_artifact_id,
            params={"cleanup": True},
            logical_collection_name="restore_without_lineage",
        )
        index_artifact = Artifact(
            id=index_artifact_id,
            project_id=project.id,
            pipeline_id=pipeline.id,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="restore_without_lineage",
            version=1,
            content_text=None,
            content_json=index_metadata,
            metadata_json=index_metadata,
        )
        db.add(index_artifact)
        db.commit()

        try:
            create_retriever(
                db,
                project_id=project.id,
                index_artifact_id=index_artifact.id,
                source_artifact_ids=None,
                retriever_type="create_vector_retriever",
                params={"top_k": 4},
            )
        except Exception as exc:
            assert "no source segments were recovered" in getattr(exc, "message", str(exc)).lower()
        else:
            raise AssertionError("Expected missing lineage segments to be rejected")


def test_query_retriever_rehydrates_missing_runtime(client, monkeypatch):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, ArtifactInput, Pipeline, PipelineIndexingConfig, Project, Retriever
    from app.services import jobs
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY, RETRIEVER_RUNTIME_REGISTRY

    INDEX_RUNTIME_REGISTRY.clear()
    RETRIEVER_RUNTIME_REGISTRY.clear()
    captured: dict[str, object] = {}

    def _fake_restore_index_runtime(**kwargs):
        captured["restore"] = kwargs
        INDEX_RUNTIME_REGISTRY[kwargs["index_artifact_id"]] = {
            "vector_store": object(),
            "doc_store": None,
            "embeddings": object(),
            "segments": list(kwargs["raw_segments"]),
            "parent_segments": list(kwargs.get("raw_parent_segments") or []),
            "params": kwargs["params"],
            "runtime_extras": {},
        }

    def _fake_create_retriever_runtime(**kwargs):
        captured["create"] = kwargs
        RETRIEVER_RUNTIME_REGISTRY[kwargs["retriever_id"]] = {"retriever": object()}

    monkeypatch.setattr(jobs, "restore_index_runtime", _fake_restore_index_runtime)
    monkeypatch.setattr(jobs, "create_retriever_runtime", _fake_create_retriever_runtime)
    monkeypatch.setattr(
        jobs,
        "execute_retriever_query",
        lambda **_: [
            {
                "score": 0.91,
                "segment": {
                    "content": "alpha",
                    "metadata": {"segment_id": "seg-1"},
                    "segment_id": "seg-1",
                },
            }
        ],
    )

    with SessionLocal() as db:
        project = Project(name="p3-query")
        db.add(project)
        db.flush()

        pipeline = Pipeline(
            project_id=project.id,
            name="index-flow-query",
            description=None,
            shape="loader_indexing",
            immutable=True,
            deleted=False,
            definition={"loader": {"type": "TextLoader", "params": {}}},
        )
        db.add(pipeline)
        db.flush()

        db.add(
            PipelineIndexingConfig(
                pipeline_id=pipeline.id,
                index_type="chroma",
                params={"cleanup": True},
                collection_name="rehydration_query",
                docstore_name=None,
            )
        )
        db.flush()

        seg = Artifact(
            project_id=project.id,
            pipeline_id=pipeline.id,
            job_id=None,
            artifact_kind="segment",
            stage_name="chunks",
            artifact_key="seg_0",
            version=1,
            content_text="alpha",
            content_json={"content": "alpha", "metadata": {"segment_id": "seg-1"}, "segment_id": "seg-1"},
            metadata_json={"segment_id": "seg-1"},
        )
        db.add(seg)
        db.flush()

        index_artifact_id = "00000000-0000-0000-0000-000000000333"
        index_payload = _index_payload(
            index_artifact_id,
            params={"cleanup": True},
            logical_collection_name="rehydration_query",
        )
        index_artifact = Artifact(
            id=index_artifact_id,
            project_id=project.id,
            pipeline_id=pipeline.id,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="rehydration_query",
            version=1,
            content_text=None,
            content_json=index_payload,
            metadata_json=index_payload,
        )
        db.add(index_artifact)
        db.flush()
        db.add(ArtifactInput(artifact_id=index_artifact.id, input_artifact_id=seg.id))
        db.flush()

        retriever = Retriever(
            project_id=project.id,
            index_artifact_id=index_artifact.id,
            retriever_type="create_vector_retriever",
            params={"top_k": 2},
        )
        db.add(retriever)
        db.commit()

        project_id = project.id
        retriever_id = retriever.id
        index_id = index_artifact.id
        seg_id = seg.id

    query_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers/{retriever_id}/query",
        json={"query": "alpha"},
    )
    assert query_resp.status_code == 200
    body = query_resp.json()
    assert body["items"][0]["segment"]["segment_id"] == "seg-1"

    result_id = body["retrieval_result_id"]
    result_resp = client.get(f"/api/v1/projects/{project_id}/retrieval-results/{result_id}")
    assert result_resp.status_code == 200
    assert result_resp.json()["items"][0]["segment_artifact_id"] == seg_id
    assert captured["restore"]["index_artifact_id"] == index_id
    assert captured["restore"]["storage"] == index_payload["storage"]
    assert captured["create"]["retriever_id"] == retriever_id


def test_list_indices_exposes_persisted_storage_columns(client):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Project

    with SessionLocal() as db:
        project = Project(name="p-indices")
        db.add(project)
        db.flush()

        index_id = "00000000-0000-0000-0000-000000000444"
        vector_collection_name = f"idx_{index_id.replace('-', '')}"
        index_artifact = Artifact(
            id=index_id,
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="logical_index",
            version=1,
            content_text=None,
            content_json=_index_payload(
                index_id,
                params={"cleanup": True},
                logical_collection_name="logical_index",
            ),
            metadata_json=_index_payload(
                index_id,
                params={"cleanup": True},
                logical_collection_name="logical_index",
            ),
            storage_backend="filesystem",
            vector_collection_name=vector_collection_name,
            vector_persist_path=str(Path("data/indexes/chroma")),
            docstore_persist_path=None,
        )
        db.add(index_artifact)
        db.commit()
        project_id = project.id

    resp = client.get(f"/api/v1/projects/{project_id}/indices")
    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["storage_backend"] == "filesystem"
    assert item["vector_collection_name"] == vector_collection_name
    assert item["vector_persist_path"] == str(Path("data/indexes/chroma"))
    assert item["docstore_persist_path"] is None


def test_retrieval_result_endpoints_return_scored_segments(client):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Project, RetrievalResult, RetrievalResultItem, Retriever

    with SessionLocal() as db:
        project = Project(name="p4")
        db.add(project)
        db.flush()

        index_artifact = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="idx_0",
            version=1,
            content_text=None,
            content_json={"index_type": "bm25"},
            metadata_json={"index_type": "bm25"},
        )
        db.add(index_artifact)
        db.flush()

        retriever = Retriever(
            project_id=project.id,
            index_artifact_id=index_artifact.id,
            retriever_type="create_bm25_retriever",
            params={},
        )
        db.add(retriever)
        db.flush()

        result = RetrievalResult(
            project_id=project.id,
            retriever_id=retriever.id,
            query_text="alpha",
            top_k=2,
        )
        db.add(result)
        db.flush()

        seg_a = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="RETRIEVAL_RESULT",
            artifact_key="seg_a",
            version=1,
            content_text="alpha content",
            content_json={"content": "alpha content", "metadata": {"source": "a"}},
            metadata_json={"source": "a"},
        )
        seg_b = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="RETRIEVAL_RESULT",
            artifact_key="seg_b",
            version=1,
            content_text="beta content",
            content_json={"content": "beta content", "metadata": {"source": "b"}},
            metadata_json={"source": "b"},
        )
        db.add_all([seg_a, seg_b])
        db.flush()

        db.add_all(
            [
                RetrievalResultItem(
                    retrieval_result_id=result.id,
                    rank=1,
                    score=0.9,
                    segment_artifact_id=seg_a.id,
                    segment_payload={"content": "alpha content", "metadata": {"source": "a"}},
                ),
                RetrievalResultItem(
                    retrieval_result_id=result.id,
                    rank=2,
                    score=0.8,
                    segment_artifact_id=seg_b.id,
                    segment_payload={"content": "beta content", "metadata": {"source": "b"}},
                ),
            ]
        )
        db.commit()

        project_id = project.id
        retriever_id = retriever.id
        result_id = result.id
        seg_a_id = seg_a.id
        seg_b_id = seg_b.id

    single_resp = client.get(f"/api/v1/projects/{project_id}/retrieval-results/{result_id}")
    assert single_resp.status_code == 200
    single = single_resp.json()
    assert single["id"] == result_id
    assert [item["segment_artifact_id"] for item in single["items"]] == [seg_a_id, seg_b_id]
    assert single["items"][0]["segment"]["content"] == "alpha content"
    assert single["items"][0]["score"] == 0.9
    assert single["items"][0]["similarity_score"] is None
    assert single["items"][0]["max_similarity_score"] is None
    assert single["items"][0]["score_details"] == {}

    list_resp = client.get(f"/api/v1/projects/{project_id}/retrievers/{retriever_id}/results")
    assert list_resp.status_code == 200
    listing = list_resp.json()
    assert listing["total"] == 1
    assert len(listing["items"]) == 1
    assert listing["items"][0]["id"] == result_id
    assert [item["segment_artifact_id"] for item in listing["items"][0]["items"]] == [seg_a_id, seg_b_id]
    assert listing["items"][0]["items"][0]["score"] == 0.9
    assert listing["items"][0]["items"][0]["score_details"] == {}


def test_retrieval_result_endpoints_derive_similarity_scores_from_metadata(client):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Project, RetrievalResult, RetrievalResultItem, Retriever

    with SessionLocal() as db:
        project = Project(name="p5")
        db.add(project)
        db.flush()

        index_artifact = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="idx_scores",
            version=1,
            content_text=None,
            content_json={"index_type": "chroma"},
            metadata_json={"index_type": "chroma"},
        )
        db.add(index_artifact)
        db.flush()

        retriever = Retriever(
            project_id=project.id,
            index_artifact_id=index_artifact.id,
            retriever_type="create_scored_dual_storage_retriever",
            params={},
        )
        db.add(retriever)
        db.flush()

        result = RetrievalResult(
            project_id=project.id,
            retriever_id=retriever.id,
            query_text="crm",
            top_k=1,
        )
        db.add(result)
        db.flush()

        seg = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="RETRIEVAL_RESULT",
            artifact_key="seg_scores",
            version=1,
            content_text="crm content",
            content_json={"content": "crm content", "metadata": {"source": "scores"}},
            metadata_json={"source": "scores"},
        )
        db.add(seg)
        db.flush()

        db.add(
            RetrievalResultItem(
                retrieval_result_id=result.id,
                rank=1,
                score=None,
                segment_artifact_id=seg.id,
                segment_payload={
                    "content": "crm content",
                    "metadata": {
                        "similarity_score": 0.77,
                        "max_similarity_score": 0.88,
                        "relevance_score": 0.66,
                    },
                    "score_details": {"custom_score": 0.55},
                },
            )
        )
        db.commit()

        project_id = project.id
        retriever_id = retriever.id
        result_id = result.id

    single_resp = client.get(f"/api/v1/projects/{project_id}/retrieval-results/{result_id}")
    assert single_resp.status_code == 200
    single_item = single_resp.json()["items"][0]
    assert single_item["score"] == 0.77
    assert single_item["similarity_score"] == 0.77
    assert single_item["max_similarity_score"] == 0.88
    assert single_item["score_details"]["custom_score"] == 0.55
    assert single_item["score_details"]["relevance_score"] == 0.66

    list_resp = client.get(f"/api/v1/projects/{project_id}/retrievers/{retriever_id}/results")
    assert list_resp.status_code == 200
    list_item = list_resp.json()["items"][0]["items"][0]
    assert list_item["score"] == 0.77
    assert list_item["similarity_score"] == 0.77
    assert list_item["max_similarity_score"] == 0.88


def test_stage_based_retriever_creation_and_query(client):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Project

    with SessionLocal() as db:
        project = Project(name="p-stage-retriever")
        db.add(project)
        db.flush()

        seg_a = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="enriched",
            artifact_key="seg_0",
            version=1,
            content_text="Title: Einstein Quotes\nSummary: Famous Einstein quotes",
            content_json={
                "content": "Title: Einstein Quotes\nSummary: Famous Einstein quotes",
                "metadata": {"source": "quotes"},
                "segment_id": "seg-enriched-1",
            },
            metadata_json={"source": "quotes"},
        )
        seg_b = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="enriched",
            artifact_key="seg_1",
            version=1,
            content_text="Title: Newton Notes\nSummary: Notes about Newton",
            content_json={
                "content": "Title: Newton Notes\nSummary: Notes about Newton",
                "metadata": {"source": "quotes"},
                "segment_id": "seg-enriched-2",
            },
            metadata_json={"source": "quotes"},
        )
        db.add_all([seg_a, seg_b])
        db.commit()

        project_id = project.id
        seg_a_id = seg_a.id
        seg_b_id = seg_b.id

    create_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers",
        json={
            "source_artifact_ids": [seg_a_id, seg_b_id],
            "retriever_type": "FuzzyRetriever",
            "params": {"threshold": 40, "mode": "wratio"},
        },
    )
    assert create_resp.status_code == 201
    retriever = create_resp.json()
    assert retriever["index_artifact_id"] is None
    assert retriever["source_artifact_ids"] == [seg_a_id, seg_b_id]

    query_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers/{retriever['id']}/query",
        json={"query": "einstein"},
    )
    assert query_resp.status_code == 200
    items = query_resp.json()["items"]
    assert items
    assert items[0]["fuzzy_score"] >= 40
    assert items[0]["segment"]["metadata"]["fuzzy_score"] >= 40


def test_graph_entity_artifacts_are_listed_and_supported_as_retriever_sources(client):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Project

    with SessionLocal() as db:
        project = Project(name="p-graph-entities")
        db.add(project)
        db.flush()

        graph_entity = Artifact(
            project_id=project.id,
            pipeline_id="pipeline-1",
            job_id=None,
            artifact_kind="graph_entity",
            stage_name="graph_entities",
            artifact_key="graph_entity_0",
            version=1,
            content_text="Entity: Probability\nType: Concept\nDescription: Likelihood of an event",
            content_json={
                "content": "Entity: Probability\nType: Concept\nDescription: Likelihood of an event",
                "metadata": {
                    "entity_id": "Probability",
                    "entity_label": "Probability",
                    "entity_type": "Concept",
                    "graph_artifact_type": "entity",
                    "source_segment_id": "chunk-1",
                },
                "segment_id": "graph-entity-probability",
                "parent_id": "chunk-1",
                "level": 0,
                "path": [],
                "type": "text",
                "original_format": "text",
            },
            metadata_json={
                "entity_id": "Probability",
                "entity_label": "Probability",
                "entity_type": "Concept",
                "graph_artifact_type": "entity",
                "source_segment_id": "chunk-1",
            },
        )
        db.add(graph_entity)
        db.commit()

        project_id = project.id
        graph_entity_id = graph_entity.id

    list_resp = client.get(f"/api/v1/projects/{project_id}/pipelines/pipeline-1/graph-entities/graph_entities")
    assert list_resp.status_code == 200
    listing = list_resp.json()
    assert listing["total"] == 1
    assert listing["items"][0]["artifact_kind"] == "graph_entity"
    assert listing["items"][0]["id"] == graph_entity_id

    get_resp = client.get(f"/api/v1/projects/{project_id}/graph-entities/{graph_entity_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["artifact_kind"] == "graph_entity"

    create_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers",
        json={
            "source_artifact_ids": [graph_entity_id],
            "retriever_type": "FuzzyRetriever",
            "params": {"threshold": 30, "mode": "wratio"},
        },
    )
    assert create_resp.status_code == 201
    retriever = create_resp.json()

    query_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers/{retriever['id']}/query",
        json={"query": "probability"},
    )
    assert query_resp.status_code == 200
    items = query_resp.json()["items"]
    assert items
    assert items[0]["segment_artifact_id"] == graph_entity_id
    assert items[0]["segment"]["metadata"]["entity_id"] == "Probability"


def test_query_top_k_is_rejected_by_api_contract(client):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Project

    with SessionLocal() as db:
        project = Project(name="p-stage-top-k")
        db.add(project)
        db.flush()

        seg = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="enriched",
            artifact_key="seg_0",
            version=1,
            content_text="Title: Einstein Quotes\nSummary: Famous Einstein quotes",
            content_json={
                "content": "Title: Einstein Quotes\nSummary: Famous Einstein quotes",
                "metadata": {"source": "quotes"},
                "segment_id": "seg-enriched-1",
            },
            metadata_json={"source": "quotes"},
        )
        db.add(seg)
        db.commit()

        project_id = project.id
        seg_id = seg.id

    create_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers",
        json={
            "source_artifact_ids": [seg_id],
            "retriever_type": "FuzzyRetriever",
            "params": {"threshold": 40, "mode": "wratio"},
        },
    )
    assert create_resp.status_code == 201
    retriever = create_resp.json()

    query_resp = client.post(
        f"/api/v1/projects/{project_id}/retrievers/{retriever['id']}/query",
        json={"query": "einstein", "top_k": 5},
    )
    assert query_resp.status_code == 422
    body = query_resp.json()
    assert body["message"] == "Validation failed"
    assert any(error["loc"][-1] == "top_k" for error in body["details"]["errors"])


def test_query_top_k_is_rejected_for_index_backed_retrievers(client, monkeypatch):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, ArtifactInput, Project
    from app.services import jobs
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

    monkeypatch.setattr(
        jobs,
        "create_retriever_runtime",
        lambda *args, **kwargs: None,
    )

    with SessionLocal() as db:
        project = Project(name="p-vector-top-k")
        db.add(project)
        db.flush()

        seg = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="chunks",
            artifact_key="seg_0",
            version=1,
            content_text="alpha",
            content_json={"content": "alpha", "metadata": {"segment_id": "seg-1"}, "segment_id": "seg-1"},
            metadata_json={"segment_id": "seg-1"},
        )
        db.add(seg)
        db.flush()

        index = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="idx",
            version=1,
            content_text=None,
            content_json={"index_type": "chroma"},
            metadata_json={"index_type": "chroma"},
        )
        db.add(index)
        db.flush()
        db.add(ArtifactInput(artifact_id=index.id, input_artifact_id=seg.id))
        db.commit()

        project_id = project.id
        index_id = index.id

    INDEX_RUNTIME_REGISTRY[index_id] = {
        "vector_store": object(),
        "doc_store": None,
        "embeddings": object(),
        "segments": [{"content": "alpha", "metadata": {"segment_id": "seg-1"}, "segment_id": "seg-1"}],
        "parent_segments": [],
        "params": {},
        "runtime_extras": {},
    }

    try:
        create_resp = client.post(
            f"/api/v1/projects/{project_id}/retrievers",
            json={
                "index_artifact_id": index_id,
                "retriever_type": "create_vector_retriever",
                "params": {"top_k": 2},
            },
        )
        assert create_resp.status_code == 201
        retriever = create_resp.json()

        mismatch_resp = client.post(
            f"/api/v1/projects/{project_id}/retrievers/{retriever['id']}/query",
            json={"query": "alpha", "top_k": 3},
        )
        assert mismatch_resp.status_code == 422
        body = mismatch_resp.json()
        assert body["message"] == "Validation failed"
        assert any(error["loc"][-1] == "top_k" for error in body["details"]["errors"])
    finally:
        INDEX_RUNTIME_REGISTRY.pop(index_id, None)


def test_query_retriever_uses_retriever_provenance_for_artifact_linking(client, monkeypatch):
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, ArtifactInput, Project
    from app.services import jobs
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

    monkeypatch.setattr(
        jobs,
        "execute_retriever_query",
        lambda **_: [
            {
                "score": 0.88,
                "segment": {
                    "content": "parent content",
                    "metadata": {"segment_id": "shared-seg"},
                    "segment_id": "shared-seg",
                },
            }
        ],
    )
    monkeypatch.setattr(
        jobs,
        "create_retriever_runtime",
        lambda *args, **kwargs: None,
    )

    with SessionLocal() as db:
        project = Project(name="p-artifact-linking")
        db.add(project)
        db.flush()

        unrelated = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="other",
            artifact_key="seg_unrelated",
            version=1,
            content_text="unrelated",
            content_json={"content": "unrelated", "metadata": {"segment_id": "shared-seg"}, "segment_id": "shared-seg"},
            metadata_json={"segment_id": "shared-seg"},
        )
        db.add(unrelated)
        db.flush()

        source = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="segment",
            stage_name="chunks",
            artifact_key="seg_source",
            version=1,
            content_text="source",
            content_json={"content": "source", "metadata": {"segment_id": "shared-seg"}, "segment_id": "shared-seg"},
            metadata_json={"segment_id": "shared-seg"},
        )
        db.add(source)
        db.flush()

        index = Artifact(
            project_id=project.id,
            pipeline_id=None,
            job_id=None,
            artifact_kind="index",
            stage_name="INDEXING",
            artifact_key="idx",
            version=1,
            content_text=None,
            content_json={"index_type": "chroma"},
            metadata_json={"index_type": "chroma"},
        )
        db.add(index)
        db.flush()
        db.add(ArtifactInput(artifact_id=index.id, input_artifact_id=source.id))
        db.commit()

        project_id = project.id
        index_id = index.id
        source_id = source.id
        unrelated_id = unrelated.id

    INDEX_RUNTIME_REGISTRY[index_id] = {
        "vector_store": object(),
        "doc_store": None,
        "embeddings": object(),
        "segments": [{"content": "source", "metadata": {"segment_id": "shared-seg"}, "segment_id": "shared-seg"}],
        "parent_segments": [],
        "params": {},
        "runtime_extras": {},
    }

    try:
        create_resp = client.post(
            f"/api/v1/projects/{project_id}/retrievers",
            json={
                "index_artifact_id": index_id,
                "retriever_type": "create_vector_retriever",
                "params": {"top_k": 2},
            },
        )
        assert create_resp.status_code == 201
        retriever = create_resp.json()

        query_resp = client.post(
            f"/api/v1/projects/{project_id}/retrievers/{retriever['id']}/query",
            json={"query": "source"},
        )
        assert query_resp.status_code == 200
        result_id = query_resp.json()["retrieval_result_id"]

        result_resp = client.get(f"/api/v1/projects/{project_id}/retrieval-results/{result_id}")
        assert result_resp.status_code == 200
        item = result_resp.json()["items"][0]
        assert item["segment_artifact_id"] == source_id
        assert item["segment_artifact_id"] != unrelated_id
    finally:
        INDEX_RUNTIME_REGISTRY.pop(index_id, None)
