from __future__ import annotations

import json


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


def test_loader_segmentation_run_flow(client):
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
    assert job["result"]["artifacts_produced"]["document"] == 1
    assert job["result"]["artifacts_produced"]["segment"] > 0

    docs = client.get(f"/api/v1/projects/{pid}/pipelines/{plid}/documents").json()
    segs = client.get(f"/api/v1/projects/{pid}/pipelines/{plid}/segments/s1").json()
    assert docs["total"] == 1
    assert segs["total"] > 0


def test_retriever_creation_rejects_missing_index_runtime():
    from app.core.database import SessionLocal
    from app.models.entities import Artifact, Pipeline, PipelineIndexingConfig, Project
    from app.services.jobs import create_retriever
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

    INDEX_RUNTIME_REGISTRY.clear()

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
                params={"collection_name": "rehydration_test", "cleanup": True},
                collection_name="rehydration_test",
                docstore_name=None,
            )
        )
        db.flush()

        index_metadata = {
            "index_type": "chroma",
            "collection_name": "rehydration_test",
            "dual_storage": False,
            "segments_indexed": 0,
        }
        index_artifact = Artifact(
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
            assert "Index runtime is not available" in str(exc)
        else:
            raise AssertionError("Expected missing index runtime to be rejected")


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
