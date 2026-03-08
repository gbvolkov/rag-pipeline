from __future__ import annotations

from langchain_core.documents import Document

from app.services import rag_adapter
from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY, RETRIEVER_RUNTIME_REGISTRY, execute_retriever_query


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
        "collection_name": "test_collection",
        "embeddings": object(),
        "cleanup": True,
        "dual_storage": True,
        "doc_store_path": str(tmp_path / "docstore.pkl"),
    }

    INDEX_RUNTIME_REGISTRY.pop(index_id, None)
    rag_adapter.build_index(
        index_artifact_id=index_id,
        index_type="chroma",
        params=params,
        raw_segments=raw_segments,
        raw_parent_segments=raw_parent_segments,
    )
    runtime = INDEX_RUNTIME_REGISTRY.pop(index_id)

    assert captured["segments"] == raw_segments
    assert captured["parent_segments"] == raw_parent_segments
    assert captured["batch_size"] == 100
    assert runtime["parent_segments"] == raw_parent_segments


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
