from __future__ import annotations

import asyncio
import inspect
import math
from pathlib import Path
from typing import Any

from langchain_core.documents import Document
from rag_lib.core.domain import Segment
from rag_lib.core.indexer import Indexer
from rag_lib.core.store import LocalPickleStore
from rag_lib.retrieval import composition as retrieval_composition
from rag_lib.retrieval import graph_retriever as retrieval_graph
from rag_lib.retrieval import retrievers as retrieval_retrievers
from rag_lib.vectors.factory import create_vector_store

from app.core.errors import APIError, ServiceUnavailableError, UnprocessableError
from app.services.capabilities import resolve_loader_class, resolve_processor_class, resolve_splitter_class
from app.services.runtime_objects import materialize_runtime_object_value

INDEX_RUNTIME_REGISTRY: dict[str, dict[str, Any]] = {}
RETRIEVER_RUNTIME_REGISTRY: dict[str, Any] = {}
SESSION_RUNTIME_REGISTRY: dict[str, dict[str, Any]] = {}
_PRIMARY_SCORE_KEYS = (
    "score",
    "rerank_score",
    "similarity_score",
    "fuzzy_score",
    "max_similarity_score",
)


def _call_maybe_async(fn, *args, **kwargs):
    if inspect.iscoroutinefunction(fn):
        return asyncio.run(fn(*args, **kwargs))
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


def _coerce_score(value: Any) -> float | None:
    try:
        converted = float(value)
    except Exception:
        return None
    if not math.isfinite(converted):
        return None
    return converted


def _extract_score_details(metadata: dict[str, Any], raw_details: Any = None) -> dict[str, float]:
    details: dict[str, float] = {}
    if isinstance(raw_details, dict):
        for key, value in raw_details.items():
            if not isinstance(key, str):
                continue
            converted = _coerce_score(value)
            if converted is not None:
                details[key] = converted

    for key, value in metadata.items():
        if not isinstance(key, str) or key in details:
            continue
        if key != "score" and not key.endswith("_score"):
            continue
        converted = _coerce_score(value)
        if converted is not None:
            details[key] = converted
    return details


def _resolve_primary_score(score_details: dict[str, float]) -> float | None:
    for key in _PRIMARY_SCORE_KEYS:
        if key in score_details:
            return score_details[key]
    return None


def serialize_document(doc: Any) -> dict[str, Any]:
    if not isinstance(doc, Document):
        raise TypeError(f"Expected langchain_core.documents.Document, got {type(doc).__name__}")
    if not isinstance(doc.metadata, dict):
        raise TypeError("Document.metadata must be a dictionary")
    doc_id = getattr(doc, "id", None)
    return {
        "id": str(doc_id) if doc_id is not None else None,
        "content": doc.page_content,
        "metadata": doc.metadata,
    }


def serialize_segment(seg: Any) -> dict[str, Any]:
    if not isinstance(seg, Segment):
        raise TypeError(f"Expected rag_lib.core.domain.Segment, got {type(seg).__name__}")
    if not isinstance(seg.metadata, dict):
        raise TypeError("Segment.metadata must be a dictionary")
    return {
        "content": seg.content,
        "metadata": seg.metadata,
        "segment_id": str(seg.segment_id) if seg.segment_id is not None else None,
        "parent_id": seg.parent_id,
        "level": seg.level,
        "path": list(seg.path),
        "type": seg.type.value,
        "original_format": seg.original_format,
    }


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
        uploaded_file_path = payload.get("uploaded_file_path")
        if isinstance(uploaded_file_path, str) and uploaded_file_path.strip():
            kwargs["file_path"] = uploaded_file_path
        else:
            raise UnprocessableError(
                "Run payload must provide an uploaded file for file-backed loaders",
            )
    return kwargs


def _materialize_runtime_value(value: Any) -> Any:
    return materialize_runtime_object_value(value)


def _inject_context_defaults(kwargs: dict[str, Any], param_names: set[str], context: dict[str, Any]) -> dict[str, Any]:
    if "vector_store" in param_names and "vector_store" not in kwargs and context.get("vector_store") is not None:
        kwargs["vector_store"] = context["vector_store"]
    if "doc_store" in param_names and "doc_store" not in kwargs and context.get("doc_store") is not None:
        kwargs["doc_store"] = context["doc_store"]
    if "documents" in param_names and "documents" not in kwargs and context.get("documents") is not None:
        kwargs["documents"] = context["documents"]
    if "segments" in param_names and "segments" not in kwargs and context.get("segments") is not None:
        kwargs["segments"] = context["segments"]
    return kwargs


def _prepare_component_kwargs(target, raw_params: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    context = dict(context or {})
    kwargs = _materialize_runtime_value(dict(raw_params))
    signature = inspect.signature(target)
    param_names = set(signature.parameters.keys())
    param_names.discard("self")
    return _inject_context_defaults(kwargs, param_names, context)


def _serialize_documents(docs: list[Any]) -> list[dict[str, Any]]:
    return [serialize_document(doc) for doc in docs]


def _serialize_segments(segments: list[Any]) -> list[dict[str, Any]]:
    return [serialize_segment(seg) for seg in segments]


def _build_documents(raw_documents: list[dict[str, Any]]) -> list[Document]:
    docs: list[Document] = []
    for item in raw_documents:
        if not isinstance(item, dict):
            raise TypeError("Each document payload must be an object")
        if "content" not in item:
            raise ValueError("Document payload is missing required field 'content'")
        metadata = item.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("Document payload field 'metadata' must be an object")
        docs.append(Document(id=item.get("id"), page_content=item["content"], metadata=metadata))
    return docs


def _build_segments(raw_segments: list[dict[str, Any]]) -> list[Segment]:
    segments: list[Segment] = []
    for item in raw_segments:
        if not isinstance(item, dict):
            raise TypeError("Each segment payload must be an object")
        if "content" not in item:
            raise ValueError("Segment payload is missing required field 'content'")
        metadata = item.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("Segment payload field 'metadata' must be an object")
        path = item.get("path", [])
        if not isinstance(path, list):
            raise ValueError("Segment payload field 'path' must be an array")
        segments.append(
            Segment(
                content=item["content"],
                metadata=metadata,
                segment_id=item.get("segment_id"),
                parent_id=item.get("parent_id"),
                level=item.get("level", 0),
                path=path,
                type=item.get("type", metadata.get("type", "text")),
                original_format=item.get("original_format", "text"),
            )
        )
    return segments


def run_loader(loader_type: str, params: dict[str, Any], run_payload: dict[str, Any] | None) -> dict[str, Any]:
    try:
        loader_cls = resolve_loader_class(loader_type)
        kwargs = _normalize_loader_kwargs(loader_type, dict(params), run_payload)
        kwargs = _prepare_component_kwargs(loader_cls.__init__, kwargs)
        loader = loader_cls(**kwargs)
        docs = _call_maybe_async(loader.load)
        diagnostics: dict[str, Any] = {}
        if hasattr(loader, "last_stats"):
            diagnostics["loader_stats"] = getattr(loader, "last_stats")
        if hasattr(loader, "last_errors"):
            diagnostics["loader_errors"] = getattr(loader, "last_errors")
        return {
            "kind": "document",
            "payload": _serialize_documents(docs),
            "runtime_extras": {},
            "diagnostics": diagnostics,
        }
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib loader execution failed for '{loader_type}'",
            details={"error": str(exc), "loader_type": loader_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def run_splitter(
    splitter_type: str,
    params: dict[str, Any],
    source_documents: list[dict[str, Any]] | None = None,
    source_segments: list[dict[str, Any]] | None = None,
    runtime_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        splitter_cls = resolve_splitter_class(splitter_type)
        kwargs = _prepare_component_kwargs(splitter_cls.__init__, params, runtime_context)
        splitter = splitter_cls(**kwargs)

        if source_segments and source_documents:
            raise UnprocessableError("Splitter input must be either documents or segments, but not both")
        if source_segments:
            segs = _build_segments(source_segments)
            out = splitter.split_segments(segs)
        else:
            docs = _build_documents(source_documents or [])
            out = splitter.split_documents(docs)
        return {
            "kind": "segment",
            "payload": _serialize_segments(out),
            "runtime_extras": {},
            "diagnostics": {},
        }
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib splitter execution failed for '{splitter_type}'",
            details={"error": str(exc), "splitter_type": splitter_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def run_processor(
    processor_type: str,
    params: dict[str, Any],
    source_documents: list[dict[str, Any]] | None = None,
    source_segments: list[dict[str, Any]] | None = None,
    runtime_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        processor_cls = resolve_processor_class(processor_type)
        kwargs = _prepare_component_kwargs(processor_cls.__init__, params, runtime_context)
        processor = processor_cls(**kwargs)

        result = None
        kind = None
        if source_segments:
            segs = _build_segments(source_segments)
            if hasattr(processor, "process_segments"):
                result = _call_maybe_async(processor.process_segments, segs)
            elif hasattr(processor, "enrich"):
                result = _call_maybe_async(processor.enrich, segs)
            else:
                raise UnprocessableError(
                    f"Processor '{processor_type}' does not support segment inputs",
                )
            kind = "segment"
        elif source_documents:
            docs = _build_documents(source_documents)
            if hasattr(processor, "process_documents"):
                result = _call_maybe_async(processor.process_documents, docs)
            else:
                raise UnprocessableError(
                    f"Processor '{processor_type}' does not support document inputs",
                )
            kind = "document"
        else:
            raise UnprocessableError(f"Processor '{processor_type}' requires stage inputs")

        if result is None:
            return {
                "kind": "none",
                "payload": [],
                "runtime_extras": {},
                "diagnostics": {},
            }

        if not isinstance(result, list):
            raise TypeError(f"Processor '{processor_type}' returned unsupported result type '{type(result).__name__}'")

        if result and isinstance(result[0], Segment):
            return {
                "kind": "segment",
                "payload": _serialize_segments(result),
                "runtime_extras": {},
                "diagnostics": {},
            }
        if result and isinstance(result[0], Document):
            return {
                "kind": "document",
                "payload": _serialize_documents(result),
                "runtime_extras": {},
                "diagnostics": {},
            }
        if not result:
            return {
                "kind": kind,
                "payload": [],
                "runtime_extras": {},
                "diagnostics": {},
            }
        raise TypeError(f"Processor '{processor_type}' returned unsupported list item type '{type(result[0]).__name__}'")
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib processor execution failed for '{processor_type}'",
            details={"error": str(exc), "processor_type": processor_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def build_index(
    index_artifact_id: str,
    index_type: str,
    params: dict[str, Any],
    raw_segments: list[dict[str, Any]],
    raw_parent_segments: list[dict[str, Any]] | None = None,
    runtime_extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        legacy_keys = [key for key in ("embeddings_provider", "embeddings_model_name") if key in params]
        if legacy_keys:
            joined = ", ".join(sorted(legacy_keys))
            raise UnprocessableError(
                f"Indexing params use unsupported legacy key(s): {joined}; provide explicit 'embeddings' runtime object",
            )

        materialized_params = _materialize_runtime_value(dict(params))
        embeddings = materialized_params.get("embeddings")
        if embeddings is None:
            raise UnprocessableError("Indexing params must include explicit 'embeddings' runtime object")
        collection_name = materialized_params.get("collection_name")
        vector_store_kwargs: dict[str, Any] = {"provider": index_type, "embeddings": embeddings}
        if "collection_name" in materialized_params:
            vector_store_kwargs["collection_name"] = materialized_params["collection_name"]
        if "connection_uri" in materialized_params:
            vector_store_kwargs["connection_uri"] = materialized_params["connection_uri"]
        if "cleanup" in materialized_params:
            vector_store_kwargs["cleanup"] = materialized_params["cleanup"]
        vector_store = create_vector_store(**vector_store_kwargs)

        doc_store = None
        if materialized_params.get("dual_storage"):
            file_path = materialized_params.get("doc_store_path")
            if not isinstance(file_path, str) or not file_path:
                raise UnprocessableError("dual_storage index requires explicit 'doc_store_path'")
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            doc_store = LocalPickleStore(file_path=file_path)

        indexer = Indexer(vector_store=vector_store, embeddings=embeddings, doc_store=doc_store)
        segments = _build_segments(raw_segments)
        parent_segments = _build_segments(raw_parent_segments or []) if raw_parent_segments else None
        index_kwargs: dict[str, Any] = {"segments": segments, "parent_segments": parent_segments}
        if "batch_size" in materialized_params:
            index_kwargs["batch_size"] = materialized_params["batch_size"]
        indexer.index(**index_kwargs)

        INDEX_RUNTIME_REGISTRY[index_artifact_id] = {
            "index_type": index_type,
            "vector_store": vector_store,
            "doc_store": doc_store,
            "embeddings": embeddings,
            "segments": raw_segments,
            "parent_segments": raw_parent_segments or [],
            "params": params,
            "runtime_extras": dict(runtime_extras or {}),
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


def _retriever_target(retriever_type: str):
    for mod in (retrieval_retrievers, retrieval_composition):
        candidate = getattr(mod, retriever_type, None)
        if inspect.isfunction(candidate) or isinstance(candidate, type):
            return candidate
    graph_candidate = getattr(retrieval_graph, retriever_type, None)
    if inspect.isfunction(graph_candidate) or isinstance(graph_candidate, type):
        return graph_candidate
    raise UnprocessableError(f"Unknown retriever type '{retriever_type}'")


def _is_retriever_spec(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("retriever_type"), str)


def create_retriever_runtime(
    retriever_id: str,
    retriever_type: str,
    index_artifact_id: str | None,
    params: dict[str, Any],
    *,
    source_payloads: list[dict[str, Any]] | None = None,
    source_artifact_kind: str | None = None,
) -> None:
    try:
        index_runtime = None
        if index_artifact_id is not None:
            index_runtime = INDEX_RUNTIME_REGISTRY.get(index_artifact_id)
            if not index_runtime:
                raise UnprocessableError(f"Index runtime '{index_artifact_id}' is not available")

        source_kind = source_artifact_kind or "segment"
        raw_segments = list(source_payloads or [])
        raw_documents: list[dict[str, Any]] = []
        if index_runtime is not None:
            raw_segments = list(index_runtime.get("segments", []))
            source_kind = "segment"
        if source_kind == "document":
            raw_documents = raw_segments
            source_objects = _build_documents(raw_documents)
            runtime_context = {
                "vector_store": index_runtime.get("vector_store") if index_runtime else None,
                "doc_store": index_runtime.get("doc_store") if index_runtime else None,
                "documents": source_objects,
                "segments": None,
            }
        else:
            source_objects = _build_segments(raw_segments)
            runtime_context = {
                "vector_store": index_runtime.get("vector_store") if index_runtime else None,
                "doc_store": index_runtime.get("doc_store") if index_runtime else None,
                "documents": source_objects,
                "segments": source_objects,
            }

        def build_runtime_retriever(spec_type: str, spec_params: dict[str, Any]):
            target = _retriever_target(spec_type)

            def materialize_value(value: Any) -> Any:
                if _is_retriever_spec(value):
                    nested_params = value.get("params", {})
                    if not isinstance(nested_params, dict):
                        raise ValueError("Nested retriever params must be an object")
                    return build_runtime_retriever(value["retriever_type"], nested_params)
                if isinstance(value, dict):
                    return _materialize_runtime_value({key: materialize_value(item) for key, item in value.items()})
                if isinstance(value, list):
                    return [materialize_value(item) for item in value]
                return value

            kwargs = {key: materialize_value(item) for key, item in dict(spec_params).items()}
            signature = inspect.signature(target)
            param_names = set(signature.parameters.keys())
            param_names.discard("self")
            kwargs = _inject_context_defaults(kwargs, param_names, runtime_context)
            if inspect.isfunction(target):
                return _call_maybe_async(target, **kwargs)
            return target(**kwargs)

        retriever = build_runtime_retriever(retriever_type, params)
        RETRIEVER_RUNTIME_REGISTRY[retriever_id] = {
            "retriever": retriever,
            "retriever_type": retriever_type,
            "index_artifact_id": index_artifact_id,
            "params": params,
            "source_artifact_kind": source_kind,
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
) -> list[dict[str, Any]]:
    runtime = RETRIEVER_RUNTIME_REGISTRY.get(retriever_id)
    if runtime is None:
        raise UnprocessableError(f"Retriever runtime '{retriever_id}' not initialized")

    retriever = runtime["retriever"]
    try:
        if not hasattr(retriever, "invoke"):
            raise RuntimeError("Retriever has no invoke method")
        docs = retriever.invoke(query)
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message="rag-lib query execution failed",
            details={"error": str(exc), "retriever_id": retriever_id},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc

    scored = []
    for doc in docs:
        payload = serialize_document(doc)
        metadata = payload.get("metadata") or {}
        segment_payload = dict(payload)
        segment_payload.update(
            {
                "content": payload["content"],
                "metadata": metadata,
                "document_id": payload.get("id"),
                "segment_id": metadata.get("segment_id"),
                "parent_id": metadata.get("parent_id"),
                "level": metadata.get("level", 0),
                "path": metadata.get("path", []),
                "type": metadata.get("type", "text"),
                "original_format": metadata.get("original_format", "text"),
            }
        )
        score_details = _extract_score_details(metadata, segment_payload.get("score_details"))
        if score_details:
            segment_payload["score_details"] = score_details
        scored.append({"score": _resolve_primary_score(score_details), "segment": segment_payload})
    return scored
