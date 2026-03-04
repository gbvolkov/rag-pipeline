from __future__ import annotations

import asyncio
import base64
import importlib
import inspect
import sys
import tempfile
import types
from pathlib import Path
from typing import Any

from app.core.config import get_settings
from app.core.errors import APIError, ServiceUnavailableError, UnprocessableError
from app.services.plugins import materialize_plugin_refs

# Runtime registries (process-local). DB remains source of truth for metadata/state.
INDEX_RUNTIME_REGISTRY: dict[str, dict[str, Any]] = {}
RETRIEVER_RUNTIME_REGISTRY: dict[str, Any] = {}
SESSION_RUNTIME_REGISTRY: dict[str, dict[str, Any]] = {}


def _bootstrap_rag_lib_namespace() -> None:
    settings = get_settings()
    source_root = settings.rag_lib_source_dir
    if "rag_lib" not in sys.modules:
        pkg = types.ModuleType("rag_lib")
        pkg.__path__ = [str(source_root)]
        sys.modules["rag_lib"] = pkg
    for sub in ("chunkers", "loaders", "core", "retrieval", "vectors", "embeddings", "graph", "processors", "llm"):
        key = f"rag_lib.{sub}"
        if key not in sys.modules:
            subpkg = types.ModuleType(key)
            subpkg.__path__ = [str(source_root / sub)]
            sys.modules[key] = subpkg


def _import(module_name: str):
    _bootstrap_rag_lib_namespace()
    return importlib.import_module(module_name)


def _call_maybe_async(fn, *args, **kwargs):
    if inspect.iscoroutinefunction(fn):
        return asyncio.run(fn(*args, **kwargs))
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


def _document_class():
    mod = _import("langchain_core.documents")
    return getattr(mod, "Document")


def _segment_class():
    mod = _import("rag_lib.core.domain")
    return getattr(mod, "Segment")


def serialize_document(doc: Any) -> dict[str, Any]:
    content = getattr(doc, "page_content", None)
    if content is None:
        content = getattr(doc, "content", "")
    metadata = getattr(doc, "metadata", None) or {}
    return {
        "content": content,
        "metadata": metadata,
    }


def serialize_segment(seg: Any) -> dict[str, Any]:
    if hasattr(seg, "model_dump"):
        return seg.model_dump()
    return {
        "content": getattr(seg, "content", ""),
        "metadata": getattr(seg, "metadata", {}) or {},
        "segment_id": getattr(seg, "segment_id", None),
        "parent_id": getattr(seg, "parent_id", None),
        "level": getattr(seg, "level", 0),
        "path": getattr(seg, "path", []),
        "type": str(getattr(seg, "type", "text")),
        "original_format": getattr(seg, "original_format", "text"),
    }


def _write_temp_file(file_name: str, b64_payload: str) -> str:
    payload = base64.b64decode(b64_payload)
    suffix = Path(file_name).suffix or ".bin"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as fp:
        fp.write(payload)
        return fp.name


def _normalize_loader_kwargs(loader_type: str, params: dict[str, Any], run_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = run_payload or {}
    kwargs = dict(params)
    if "url" in kwargs:
        return kwargs

    if loader_type in {"WebLoader", "AsyncWebLoader"}:
        if payload.get("url") is None:
            raise UnprocessableError("Run payload must include 'url' for web loaders")
        kwargs["url"] = payload["url"]
        return kwargs

    if "file_path" not in kwargs:
        if payload.get("file_content_b64") and payload.get("file_name"):
            kwargs["file_path"] = _write_temp_file(payload["file_name"], payload["file_content_b64"])
        elif payload.get("text") is not None:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as fp:
                fp.write(payload["text"])
                kwargs["file_path"] = fp.name
        else:
            raise UnprocessableError("Run payload must provide file input (file_content_b64+file_name) or text.")
    return kwargs


def run_loader(loader_type: str, module_name: str, params: dict[str, Any], run_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    try:
        mod = _import(f"rag_lib.loaders.{module_name}")
        loader_cls = getattr(mod, loader_type)
        kwargs = _normalize_loader_kwargs(loader_type, materialize_plugin_refs(params), run_payload)
        loader = loader_cls(**kwargs)
        docs = _call_maybe_async(loader.load)
        return [serialize_document(doc) for doc in docs]
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib loader execution failed for '{loader_type}'",
            details={"error": str(exc), "loader_type": loader_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def _create_embeddings_if_needed(kwargs: dict[str, Any], param_names: set[str]) -> dict[str, Any]:
    if "embeddings" in param_names and "embeddings" not in kwargs:
        provider = kwargs.pop("embeddings_provider", None)
        model_name = kwargs.pop("embeddings_model_name", None)
        emb_factory = _import("rag_lib.embeddings.factory")
        kwargs["embeddings"] = emb_factory.create_embeddings_model(provider=provider, model_name=model_name)
    return kwargs


def _build_documents(raw_documents: list[dict[str, Any]]) -> list[Any]:
    Document = _document_class()
    docs = []
    for item in raw_documents:
        docs.append(Document(page_content=item["content"], metadata=item.get("metadata") or {}))
    return docs


def _build_segments(raw_segments: list[dict[str, Any]]) -> list[Any]:
    Segment = _segment_class()
    segments = []
    for item in raw_segments:
        payload = dict(item)
        payload.pop("type", None)
        segments.append(
            Segment(
                content=item.get("content", ""),
                metadata=item.get("metadata") or {},
                segment_id=item.get("segment_id"),
                parent_id=item.get("parent_id"),
                level=item.get("level", 0),
                path=item.get("path") or [],
                original_format=item.get("original_format", "text"),
            )
        )
    return segments


def run_splitter(
    splitter_type: str,
    module_name: str,
    params: dict[str, Any],
    source_documents: list[dict[str, Any]] | None = None,
    source_segments: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    try:
        mod = _import(f"rag_lib.chunkers.{module_name}")
        splitter_cls = getattr(mod, splitter_type)
        ctor_params = inspect.signature(splitter_cls.__init__).parameters
        kwargs = _create_embeddings_if_needed(materialize_plugin_refs(dict(params)), set(ctor_params.keys()))
        splitter = splitter_cls(**kwargs)

        if source_segments:
            segs = _build_segments(source_segments)
            if hasattr(splitter, "split_segments"):
                out = splitter.split_segments(segs)
            else:
                docs = [seg.to_langchain() for seg in segs]
                out = splitter.split_documents(docs)
        else:
            docs = _build_documents(source_documents or [])
            out = splitter.split_documents(docs)
        return [serialize_segment(seg) for seg in out]
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib splitter execution failed for '{splitter_type}'",
            details={"error": str(exc), "splitter_type": splitter_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def build_index(
    index_artifact_id: str,
    index_type: str,
    params: dict[str, Any],
    raw_segments: list[dict[str, Any]],
) -> dict[str, Any]:
    try:
        vectors_factory = _import("rag_lib.vectors.factory")
        emb_factory = _import("rag_lib.embeddings.factory")
        indexer_mod = _import("rag_lib.core.indexer")
        store_mod = _import("rag_lib.core.store")

        embeddings_provider = params.get("embeddings_provider")
        embeddings_model_name = params.get("embeddings_model_name")
        collection_name = params.get("collection_name", f"collection_{index_artifact_id}")
        connection_uri = params.get("connection_uri")
        cleanup = params.get("cleanup", True)

        embeddings = emb_factory.create_embeddings_model(provider=embeddings_provider, model_name=embeddings_model_name)
        vector_store = vectors_factory.create_vector_store(
            provider=index_type,
            embeddings=embeddings,
            collection_name=collection_name,
            connection_uri=connection_uri,
            cleanup=cleanup,
        )

        doc_store = None
        if params.get("dual_storage") or "dual" in index_type:
            file_path = params.get("doc_store_path", f"./data/docstore/{index_artifact_id}.pkl")
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            doc_store = store_mod.LocalPickleStore(file_path=file_path)

        Indexer = getattr(indexer_mod, "Indexer")
        indexer = Indexer(vector_store=vector_store, embeddings=embeddings, doc_store=doc_store)
        segments = _build_segments(raw_segments)
        indexer.index(segments=segments, batch_size=params.get("batch_size", 100))

        INDEX_RUNTIME_REGISTRY[index_artifact_id] = {
            "index_type": index_type,
            "vector_store": vector_store,
            "doc_store": doc_store,
            "segments": raw_segments,
            "params": params,
        }

        return {
            "index_type": index_type,
            "collection_name": collection_name,
            "dual_storage": bool(doc_store),
            "segments_indexed": len(raw_segments),
        }
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib index build failed for '{index_type}'",
            details={"error": str(exc), "index_type": index_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def create_retriever_runtime(
    retriever_id: str,
    retriever_type: str,
    index_artifact_id: str,
    params: dict[str, Any],
    require_session: bool = False,
) -> None:
    try:
        retrievers_mod = _import("rag_lib.retrieval.retrievers")
        composition_mod = _import("rag_lib.retrieval.composition")
        index_runtime = INDEX_RUNTIME_REGISTRY.get(index_artifact_id)
        if not index_runtime:
            raise UnprocessableError(f"Index runtime '{index_artifact_id}' is not available")

        vector_store = index_runtime.get("vector_store")
        docs = _build_documents(
            [
                {
                    "content": s.get("content", ""),
                    "metadata": s.get("metadata") or {},
                }
                for s in index_runtime.get("segments", [])
            ]
        )

        retriever = None
        if retriever_type == "create_vector_retriever":
            retriever = retrievers_mod.create_vector_retriever(
                vector_store=vector_store,
                top_k=params.get("top_k", 4),
                search_type=params.get("search_type", "similarity"),
                score_threshold=params.get("score_threshold"),
            )
        elif retriever_type == "create_bm25_retriever":
            retriever = retrievers_mod.create_bm25_retriever(
                documents=docs,
                top_k=params.get("top_k", 4),
            )
        elif retriever_type == "create_dual_storage_retriever":
            retriever = composition_mod.create_dual_storage_retriever(
                vector_store=vector_store,
                doc_store=index_runtime.get("doc_store"),
                id_key=params.get("id_key", "segment_id"),
                search_kwargs=params.get("search_kwargs"),
            )
        elif retriever_type == "create_scored_dual_storage_retriever":
            retriever = composition_mod.create_scored_dual_storage_retriever(
                vector_store=vector_store,
                doc_store=index_runtime.get("doc_store"),
                id_key=params.get("id_key", "segment_id"),
                search_kwargs=params.get("search_kwargs"),
            )
        elif hasattr(retrievers_mod, retriever_type):
            klass = getattr(retrievers_mod, retriever_type)
            retriever = klass(documents=docs, **materialize_plugin_refs(dict(params)))
        else:
            raise UnprocessableError(f"Unknown retriever type '{retriever_type}'")

        RETRIEVER_RUNTIME_REGISTRY[retriever_id] = {
            "retriever": retriever,
            "retriever_type": retriever_type,
            "require_session": require_session,
            "index_artifact_id": index_artifact_id,
            "params": params,
        }
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib retriever creation failed for '{retriever_type}'",
            details={"error": str(exc), "retriever_type": retriever_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def init_retriever_session(retriever_id: str, session_id: str, state: dict[str, Any] | None = None) -> None:
    SESSION_RUNTIME_REGISTRY[session_id] = {
        "retriever_id": retriever_id,
        "state": state or {},
    }


def release_retriever_session(session_id: str) -> None:
    SESSION_RUNTIME_REGISTRY.pop(session_id, None)


def execute_retriever_query(
    retriever_id: str,
    query: str,
    top_k: int,
) -> list[dict[str, Any]]:
    runtime = RETRIEVER_RUNTIME_REGISTRY.get(retriever_id)
    if runtime is None:
        raise UnprocessableError(f"Retriever runtime '{retriever_id}' not initialized")

    retriever = runtime["retriever"]
    try:
        if hasattr(retriever, "invoke"):
            docs = retriever.invoke(query)
        elif hasattr(retriever, "get_relevant_documents"):
            docs = retriever.get_relevant_documents(query)
        else:
            raise RuntimeError("Retriever has no invoke/get_relevant_documents method")
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message="rag-lib query execution failed",
            details={"error": str(exc), "retriever_id": retriever_id},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc

    scored = []
    for idx, doc in enumerate(docs[:top_k]):
        payload = serialize_document(doc)
        score = None
        metadata = payload.get("metadata") or {}
        for key in ("score", "relevance_score", "fuzzy_score"):
            if key in metadata:
                try:
                    score = float(metadata[key])
                    break
                except Exception:
                    score = None
        segment_payload = {
            "content": payload["content"],
            "metadata": metadata,
            "segment_id": metadata.get("segment_id"),
            "parent_id": metadata.get("parent_id"),
            "level": metadata.get("level", 0),
            "path": metadata.get("path", []),
            "type": metadata.get("type", "text"),
            "original_format": metadata.get("original_format", "text"),
        }
        scored.append({"score": score, "segment": segment_payload})
    return scored
