from __future__ import annotations

from pathlib import Path

from langchain_core.documents import Document

from app.services import rag_adapter
from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY, RETRIEVER_RUNTIME_REGISTRY, execute_retriever_query


def _storage_descriptor(tmp_path: Path, index_id: str, *, index_type: str = "chroma", dual_storage: bool = False) -> dict[str, object]:
    vector_store: dict[str, object] = {"provider": index_type, "persist_path": None, "collection_name": None}
    if index_type == "chroma":
        vector_store = {
            "provider": "chroma",
            "persist_path": str(tmp_path / "chroma"),
            "collection_name": f"idx_{index_id.replace('-', '')}",
        }
    storage: dict[str, object] = {
        "version": 1,
        "backend": "filesystem",
        "vector_store": vector_store,
        "doc_store": None,
    }
    if dual_storage:
        storage["doc_store"] = {
            "provider": "local_pickle",
            "file_path": str(tmp_path / "docstore" / f"{index_id}.pkl"),
        }
    return storage


def test_execute_retriever_query_derives_scores_from_metadata():
    class _FakeRetriever:
        def invoke(self, query: str):
            _ = query
            return [
                Document(
                    id="doc-1",
                    page_content="chunk content",
                    metadata={
                        "segment_id": "seg-1",
                        "parent_id": "parent-1",
                        "similarity_score": 0.73,
                        "max_similarity_score": 0.81,
                        "relevance_score": 0.6,
                    },
                )
            ]

    RETRIEVER_RUNTIME_REGISTRY["retriever-scores"] = {"retriever": _FakeRetriever()}
    try:
        scored = execute_retriever_query(retriever_id="retriever-scores", query="crm")
    finally:
        RETRIEVER_RUNTIME_REGISTRY.pop("retriever-scores", None)

    assert len(scored) == 1
    item = scored[0]
    assert item["score"] == 0.73
    assert item["segment"]["document_id"] == "doc-1"
    assert item["segment"]["metadata"]["similarity_score"] == 0.73
    assert item["segment"]["metadata"]["max_similarity_score"] == 0.81
    assert item["segment"]["score_details"]["similarity_score"] == 0.73
    assert item["segment"]["score_details"]["max_similarity_score"] == 0.81
    assert item["segment"]["score_details"]["relevance_score"] == 0.6


def test_run_loader_forces_headless_playwright(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeWebLoader:
        def __init__(self, **kwargs):
            captured["kwargs"] = dict(kwargs)

        def load(self):
            return [Document(page_content="playwright doc", metadata={"source": "web"})]

    monkeypatch.setattr(rag_adapter, "resolve_loader_class", lambda _: _FakeWebLoader)

    result = rag_adapter.run_loader(
        loader_type="WebLoader",
        params={
            "depth": 1,
            "fetch_mode": "playwright",
            "playwright_visible": True,
            "playwright_headless": False,
        },
        run_payload={"url": "https://example.com"},
    )

    assert result["kind"] == "document"
    assert captured["kwargs"]["playwright_headless"] is True
    assert "playwright_visible" not in captured["kwargs"]


def test_create_retriever_runtime_passes_scored_dual_params(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_create_scored_dual_storage_retriever(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(
        rag_adapter.retrieval_composition,
        "create_scored_dual_storage_retriever",
        _fake_create_scored_dual_storage_retriever,
    )

    INDEX_RUNTIME_REGISTRY["idx-scored"] = {
        "vector_store": object(),
        "doc_store": object(),
        "segments": [{"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}}],
    }

    try:
        rag_adapter.create_retriever_runtime(
            retriever_id="retriever-scored",
            retriever_type="create_scored_dual_storage_retriever",
            index_artifact_id="idx-scored",
            params={
                "id_key": "parent_id",
                "search_kwargs": {"k": 8},
                "search_type": "similarity_score_threshold",
                "score_threshold": 0.4,
                "hydration_mode": "children_enriched",
                "enrichment_separator": "\n--sep--\n",
            },
        )
        assert captured["id_key"] == "parent_id"
        assert captured["search_kwargs"] == {"k": 8}
        assert captured["search_type"] == "similarity_score_threshold"
        assert captured["score_threshold"] == 0.4
        assert captured["hydration_mode"] == "children_enriched"
        assert captured["enrichment_separator"] == "\n--sep--\n"
    finally:
        RETRIEVER_RUNTIME_REGISTRY.pop("retriever-scored", None)
        INDEX_RUNTIME_REGISTRY.pop("idx-scored", None)


def test_build_index_passes_parent_segments_to_indexer(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class _FakeStore:
        def __init__(self, file_path: str):
            self.file_path = file_path

    class _FakeIndexer:
        def __init__(self, vector_store, embeddings, doc_store=None):
            _ = vector_store
            _ = embeddings
            _ = doc_store

        def index(self, segments, parent_segments=None, batch_size=100):
            captured["segments"] = segments
            captured["parent_segments"] = parent_segments
            captured["batch_size"] = batch_size

    monkeypatch.setattr(rag_adapter, "create_vector_store", lambda **_: object())
    monkeypatch.setattr(rag_adapter, "Indexer", _FakeIndexer)
    monkeypatch.setattr(rag_adapter, "LocalPickleStore", _FakeStore)
    monkeypatch.setattr(rag_adapter, "_build_segments", lambda raw: list(raw))

    index_id = "idx-parent-pass-through"
    raw_segments = [{"segment_id": "child-1", "content": "child content", "metadata": {"parent_id": "parent-1"}}]
    raw_parent_segments = [{"segment_id": "parent-1", "content": "parent content", "metadata": {}}]
    params = {
        "embeddings": object(),
        "cleanup": True,
        "dual_storage": True,
    }
    storage = _storage_descriptor(tmp_path, index_id, dual_storage=True)

    INDEX_RUNTIME_REGISTRY.pop(index_id, None)
    payload = rag_adapter.build_index(
        index_artifact_id=index_id,
        index_type="chroma",
        params=params,
        raw_segments=raw_segments,
        raw_parent_segments=raw_parent_segments,
        logical_collection_name="test_collection",
        logical_docstore_name="test_collection_docstore",
        storage=storage,
    )
    runtime = INDEX_RUNTIME_REGISTRY.pop(index_id)

    assert captured["segments"] == raw_segments
    assert captured["parent_segments"] == raw_parent_segments
    assert captured["batch_size"] == 100
    assert runtime["parent_segments"] == raw_parent_segments
    assert runtime["storage"] == storage
    assert payload["logical_collection_name"] == "test_collection"
    assert payload["logical_docstore_name"] == "test_collection_docstore"
    assert payload["storage"] == storage


def test_restore_index_runtime_reopens_existing_chroma_without_reindex(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class _FakeVectorStore:
        def get(self, *, limit: int):
            assert limit == 1
            return {"ids": ["seg-1"]}

    def _fake_create_vector_store(**kwargs):
        captured["vector_store_kwargs"] = kwargs
        return _FakeVectorStore()

    class _FailIfConstructed:
        def __init__(self, *args, **kwargs):
            raise AssertionError("restore_index_runtime should not rebuild persistent stores")

    index_id = "idx-restore-chroma"
    raw_segments = [{"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}}]
    storage = _storage_descriptor(tmp_path, index_id)

    monkeypatch.setattr(rag_adapter, "create_vector_store", _fake_create_vector_store)
    monkeypatch.setattr(rag_adapter, "Indexer", _FailIfConstructed)

    INDEX_RUNTIME_REGISTRY.pop(index_id, None)
    metadata = rag_adapter.restore_index_runtime(
        index_artifact_id=index_id,
        index_type="chroma",
        params={"embeddings": object(), "cleanup": True},
        raw_segments=raw_segments,
        logical_collection_name="restored_collection",
        storage=storage,
    )
    runtime = INDEX_RUNTIME_REGISTRY.pop(index_id)

    assert metadata["logical_collection_name"] == "restored_collection"
    assert metadata["storage"] == storage
    assert captured["vector_store_kwargs"]["cleanup"] is False
    assert captured["vector_store_kwargs"]["collection_name"] == storage["vector_store"]["collection_name"]
    assert runtime["segments"] == raw_segments
    assert runtime["storage"] == storage


def test_restore_index_runtime_rebuilds_missing_chroma_storage(monkeypatch, tmp_path):
    captured: dict[str, object] = {"vector_store_kwargs": []}

    class _MissingVectorStore:
        def __init__(self, cleanup: bool):
            self.cleanup = cleanup

        def get(self, *, limit: int):
            assert limit == 1
            return {"ids": []}

    def _fake_create_vector_store(**kwargs):
        captured["vector_store_kwargs"].append(kwargs)
        return _MissingVectorStore(cleanup=bool(kwargs.get("cleanup")))

    class _FakeIndexer:
        def __init__(self, vector_store, embeddings, doc_store=None):
            captured["vector_store"] = vector_store
            captured["embeddings"] = embeddings
            captured["doc_store"] = doc_store

        def index(self, segments, parent_segments=None, batch_size=100):
            captured["segments"] = segments
            captured["parent_segments"] = parent_segments
            captured["batch_size"] = batch_size

    index_id = "idx-restore-missing-chroma"
    raw_segments = [{"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}}]
    storage = _storage_descriptor(tmp_path, index_id)

    monkeypatch.setattr(rag_adapter, "create_vector_store", _fake_create_vector_store)
    monkeypatch.setattr(rag_adapter, "Indexer", _FakeIndexer)
    monkeypatch.setattr(rag_adapter, "_build_segments", lambda raw: list(raw))

    INDEX_RUNTIME_REGISTRY.pop(index_id, None)
    rag_adapter.restore_index_runtime(
        index_artifact_id=index_id,
        index_type="chroma",
        params={"embeddings": object(), "cleanup": True, "batch_size": 12},
        raw_segments=raw_segments,
        storage=storage,
    )
    runtime = INDEX_RUNTIME_REGISTRY.pop(index_id)

    assert captured["vector_store_kwargs"][0]["cleanup"] is False
    assert captured["vector_store_kwargs"][1]["cleanup"] is True
    assert captured["segments"] == raw_segments
    assert captured["parent_segments"] is None
    assert captured["batch_size"] == 12
    assert runtime["segments"] == raw_segments
    assert runtime["storage"] == storage


def test_restore_index_runtime_rebuilds_nonpersistent_store(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def _fake_create_vector_store(**kwargs):
        captured["vector_store_kwargs"] = kwargs
        return object()

    class _FakeIndexer:
        def __init__(self, vector_store, embeddings, doc_store=None):
            captured["vector_store"] = vector_store
            captured["embeddings"] = embeddings
            captured["doc_store"] = doc_store

        def index(self, segments, parent_segments=None, batch_size=100):
            captured["segments"] = segments
            captured["parent_segments"] = parent_segments
            captured["batch_size"] = batch_size

    index_id = "idx-restore-faiss"
    raw_segments = [{"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}}]
    storage = _storage_descriptor(tmp_path, index_id, index_type="faiss")

    monkeypatch.setattr(rag_adapter, "create_vector_store", _fake_create_vector_store)
    monkeypatch.setattr(rag_adapter, "Indexer", _FakeIndexer)
    monkeypatch.setattr(rag_adapter, "_build_segments", lambda raw: list(raw))

    INDEX_RUNTIME_REGISTRY.pop(index_id, None)
    rag_adapter.restore_index_runtime(
        index_artifact_id=index_id,
        index_type="faiss",
        params={"embeddings": object(), "cleanup": True, "batch_size": 12},
        raw_segments=raw_segments,
        storage=storage,
    )
    runtime = INDEX_RUNTIME_REGISTRY.pop(index_id)

    assert captured["vector_store_kwargs"]["cleanup"] is False
    assert captured["segments"] == raw_segments
    assert captured["parent_segments"] is None
    assert captured["batch_size"] == 12
    assert runtime["segments"] == raw_segments
    assert runtime["storage"] == storage


def test_create_retriever_runtime_injects_source_documents_and_segments(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_retriever_factory(documents=None, segments=None):
        captured["documents"] = documents
        captured["segments"] = segments
        return object()

    monkeypatch.setattr(rag_adapter, "_retriever_target", lambda _: _fake_retriever_factory)

    rag_adapter.create_retriever_runtime(
        retriever_id="retriever-source-context",
        retriever_type="FuzzyRetriever",
        index_artifact_id=None,
        params={},
        source_payloads=[
            {"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}},
        ],
        source_artifact_kind="segment",
    )
    try:
        assert "documents" in captured
        assert "segments" in captured
        assert len(captured["documents"]) == 1
        assert len(captured["segments"]) == 1
    finally:
        RETRIEVER_RUNTIME_REGISTRY.pop("retriever-source-context", None)


def test_create_retriever_runtime_does_not_hidden_inject_llm_embeddings_or_graph_store(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_retriever_factory(
        vector_store=None,
        doc_store=None,
        llm=None,
        embeddings=None,
        graph_store=None,
        store=None,
    ):
        captured["vector_store"] = vector_store
        captured["doc_store"] = doc_store
        captured["llm"] = llm
        captured["embeddings"] = embeddings
        captured["graph_store"] = graph_store
        captured["store"] = store
        return object()

    monkeypatch.setattr(rag_adapter, "_retriever_target", lambda _: _fake_retriever_factory)

    INDEX_RUNTIME_REGISTRY["idx-no-hidden-context"] = {
        "vector_store": object(),
        "doc_store": object(),
        "embeddings": object(),
        "segments": [{"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}}],
        "parent_segments": [],
        "params": {},
        "runtime_extras": {"llm": object(), "graph_store": object()},
    }

    try:
        rag_adapter.create_retriever_runtime(
            retriever_id="retriever-no-hidden-context",
            retriever_type="create_vector_retriever",
            index_artifact_id="idx-no-hidden-context",
            params={},
        )
        assert captured["vector_store"] is not None
        assert captured["doc_store"] is not None
        assert captured["llm"] is None
        assert captured["embeddings"] is None
        assert captured["graph_store"] is None
        assert captured["store"] is None
    finally:
        RETRIEVER_RUNTIME_REGISTRY.pop("retriever-no-hidden-context", None)
        INDEX_RUNTIME_REGISTRY.pop("idx-no-hidden-context", None)


def test_create_retriever_runtime_injects_project_graph_store_for_graph_retriever(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_graph_retriever(vector_store=None, doc_store=None, graph_store=None, config=None, llm=None):
        captured["vector_store"] = vector_store
        captured["doc_store"] = doc_store
        captured["graph_store"] = graph_store
        captured["config"] = config
        captured["llm"] = llm
        return object()

    def _fake_materialize(value):
        if isinstance(value, dict) and value.get("object_type") == "create_graph_store":
            captured["graph_store_spec"] = dict(value)
            return "neo4j-graph-store"
        return value

    monkeypatch.setattr(rag_adapter, "_retriever_target", lambda _: _fake_graph_retriever)
    monkeypatch.setattr(rag_adapter, "_materialize_runtime_value", _fake_materialize)

    INDEX_RUNTIME_REGISTRY["idx-graph-project-config"] = {
        "vector_store": object(),
        "doc_store": object(),
        "segments": [{"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}}],
        "runtime_extras": {},
    }

    try:
        rag_adapter.create_retriever_runtime(
            retriever_id="retriever-graph-project-config",
            retriever_type="GraphRetriever",
            index_artifact_id="idx-graph-project-config",
            params={},
            project_graph_store_config={
                "provider": "neo4j",
                "params": {
                    "uri": "bolt://neo4j:7687",
                    "username": "neo4j",
                    "password": "neo4j_password",
                    "database": "neo4j",
                },
            },
        )
        assert captured["vector_store"] is not None
        assert captured["doc_store"] is not None
        assert captured["graph_store"] == "neo4j-graph-store"
        assert captured["graph_store_spec"] == {
            "object_type": "create_graph_store",
            "provider": "neo4j",
            "uri": "bolt://neo4j:7687",
            "username": "neo4j",
            "password": "neo4j_password",
            "database": "neo4j",
        }
    finally:
        RETRIEVER_RUNTIME_REGISTRY.pop("retriever-graph-project-config", None)
        INDEX_RUNTIME_REGISTRY.pop("idx-graph-project-config", None)


def test_create_retriever_runtime_rejects_none_runtime(monkeypatch):
    monkeypatch.setattr(rag_adapter, "_retriever_target", lambda _: (lambda **kwargs: None))

    try:
        rag_adapter.create_retriever_runtime(
            retriever_id="retriever-none-runtime",
            retriever_type="FuzzyRetriever",
            index_artifact_id=None,
            params={},
            source_payloads=[
                {"segment_id": "seg-1", "content": "alpha", "metadata": {"segment_id": "seg-1"}},
            ],
            source_artifact_kind="segment",
        )
    except Exception as exc:
        assert "returned no runtime" in getattr(exc, "message", str(exc)).lower()
    else:
        raise AssertionError("Expected None retriever runtime to be rejected")


def test_run_processor_entity_extractor_emits_persisted_graph_segments(monkeypatch):
    class _FakeGraph:
        def nodes(self, data=True):
            assert data is True
            return [
                (
                    "entity-1",
                    {
                        "id": "Математика",
                        "label": "Математика",
                        "type": "Concept",
                        "description": "Mathematics subject for grades 5-6",
                        "source_segment_id": "seg-1",
                    },
                ),
                (
                    "entity-2",
                    {
                        "id": "Физика",
                        "label": "Физика",
                        "type": "Concept",
                        "description": "Physics",
                        "source_segment_id": "seg-other",
                    },
                ),
            ]

        def edges(self, data=True):
            assert data is True
            return [
                ("entity-1", "entity-3", {"source_segment_id": "seg-1", "relation_type": "INCLUDES"}),
                ("entity-2", "entity-4", {"source_segment_id": "seg-other", "relation_type": "INCLUDES"}),
            ]

    class _FakeStore:
        def __init__(self):
            self._graph = _FakeGraph()

    class _FakeEntityExtractor:
        def __init__(self, llm, store):
            self.llm = llm
            self.store = store

        def process_segments(self, segments):
            assert len(segments) == 1
            return None

    monkeypatch.setattr(rag_adapter, "resolve_processor_class", lambda _: _FakeEntityExtractor)

    store = _FakeStore()
    result = rag_adapter.run_processor(
        processor_type="EntityExtractor",
        params={"llm": object(), "store": store},
        source_segments=[
            {
                "content": "### Математика (5-6):",
                "metadata": {"segment_id": "seg-1"},
                "segment_id": "seg-1",
                "parent_id": None,
                "level": 0,
                "path": [],
                "type": "text",
                "original_format": "text",
            }
        ],
    )

    assert result["kind"] == "none"
    assert result["runtime_extras"]["graph_store"] is store
    assert result["diagnostics"]["graph_entities_persisted"] == 1
    assert result["diagnostics"]["graph_edges_seen"] == 1
    assert "graph_artifact_persistence_error" not in result["diagnostics"]
    assert len(result["persisted_artifacts"]) == 1
    persisted_artifact = result["persisted_artifacts"][0]
    assert persisted_artifact["artifact_kind"] == "graph_entity"
    assert len(persisted_artifact["payload"]) == 1
    persisted = persisted_artifact["payload"][0]
    assert persisted["content"] == (
        "Entity: Математика\n"
        "Type: Concept\n"
        "Description: Mathematics subject for grades 5-6"
    )
    assert persisted["parent_id"] == "seg-1"
    assert persisted["metadata"]["entity_id"] == "Математика"
    assert persisted["metadata"]["graph_artifact_type"] == "entity"
    assert persisted["metadata"]["source_segment_id"] == "seg-1"
