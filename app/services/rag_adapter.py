from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import os
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

from app.core.config import get_settings
from app.core.errors import APIError, ServiceUnavailableError, UnprocessableError
from app.services.capabilities import resolve_loader_class, resolve_processor_class, resolve_splitter_class
from app.services.runtime_objects import materialize_runtime_object_value

INDEX_RUNTIME_REGISTRY: dict[str, dict[str, Any]] = {}
RETRIEVER_RUNTIME_REGISTRY: dict[str, Any] = {}
SESSION_RUNTIME_REGISTRY: dict[str, dict[str, Any]] = {}
_INDEX_STORAGE_VERSION = 1
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


def _force_headless_playwright(loader_type: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    if loader_type not in {"WebLoader", "AsyncWebLoader"}:
        return kwargs
    normalized = dict(kwargs)
    normalized.pop("playwright_visible", None)
    normalized["playwright_headless"] = True
    return normalized


def _serialize_documents(docs: list[Any]) -> list[dict[str, Any]]:
    return [serialize_document(doc) for doc in docs]


def _serialize_segments(segments: list[Any]) -> list[dict[str, Any]]:
    return [serialize_segment(seg) for seg in segments]


def _graph_entity_segment_id(entity_id: str, source_segment_id: str | None) -> str:
    seed = f"{entity_id}|{source_segment_id or ''}".encode("utf-8")
    return f"graph_entity_{hashlib.sha1(seed).hexdigest()}"


def _serialize_graph_entity_segment(entity: dict[str, Any]) -> dict[str, Any]:
    entity_id = str(entity.get("id") or entity.get("label") or "unknown_entity")
    label = str(entity.get("label") or entity_id)
    entity_type = str(entity.get("type") or "Entity")
    description_raw = entity.get("description")
    description = str(description_raw).strip() if description_raw is not None else ""
    source_segment_id_raw = entity.get("source_segment_id")
    source_segment_id = str(source_segment_id_raw) if source_segment_id_raw is not None else None

    content_lines = [f"Entity: {label}", f"Type: {entity_type}"]
    if description:
        content_lines.append(f"Description: {description}")
    metadata: dict[str, Any] = {
        "entity_id": entity_id,
        "entity_label": label,
        "entity_type": entity_type,
        "graph_artifact_type": "entity",
        "retrieval_kind": "entity",
        "title": label,
    }
    if source_segment_id is not None:
        metadata["source_segment_id"] = source_segment_id

    return {
        "content": "\n".join(content_lines),
        "metadata": metadata,
        "segment_id": _graph_entity_segment_id(entity_id, source_segment_id),
        "parent_id": source_segment_id,
        "level": 0,
        "path": [],
        "type": "text",
        "original_format": "text",
    }


def _collect_networkx_graph_entities(graph_store: Any, source_segment_ids: set[str]) -> tuple[list[dict[str, Any]], int]:
    graph = getattr(graph_store, "_graph", None)
    if graph is None:
        return [], 0

    entities: list[dict[str, Any]] = []
    matched_node_ids: set[str] = set()
    for node_id, data in graph.nodes(data=True):
        if not isinstance(data, dict):
            continue
        source_segment_id = data.get("source_segment_id")
        if source_segment_id is None or str(source_segment_id) not in source_segment_ids:
            continue
        matched_node_ids.add(str(data.get("id") or node_id))
        entities.append(
            {
                "id": data.get("id") or node_id,
                "type": data.get("type") or "Entity",
                "label": data.get("label") or data.get("id") or node_id,
                "description": data.get("description"),
                "source_segment_id": source_segment_id,
            }
        )

    edge_count = 0
    for source_id, target_id, data in graph.edges(data=True):
        edge_data = data if isinstance(data, dict) else {}
        edge_source_segment_id = edge_data.get("source_segment_id")
        if edge_source_segment_id is not None and str(edge_source_segment_id) in source_segment_ids:
            edge_count += 1
            continue
        if str(source_id) in matched_node_ids or str(target_id) in matched_node_ids:
            edge_count += 1

    return entities, edge_count


def _collect_neo4j_graph_entities(graph_store: Any, source_segment_ids: list[str]) -> tuple[list[dict[str, Any]], int]:
    driver = getattr(graph_store, "driver", None)
    database = getattr(graph_store, "database", None)
    if driver is None or not isinstance(database, str) or not database:
        return [], 0

    entities_query = """
        MATCH (n:Entity)
        WHERE n.source_segment_id IN $source_segment_ids
        RETURN DISTINCT
            n.id AS id,
            n.type AS type,
            n.label AS label,
            n.description AS description,
            n.source_segment_id AS source_segment_id
        ORDER BY coalesce(n.label, n.id)
    """
    edge_count_query = """
        MATCH (n:Entity)-[r]-()
        WHERE n.source_segment_id IN $source_segment_ids OR r.source_segment_id IN $source_segment_ids
        RETURN count(DISTINCT r) AS edge_count
    """
    with driver.session(database=database) as session:
        entity_rows = session.run(entities_query, source_segment_ids=source_segment_ids)
        entities = [record.data() for record in entity_rows]
        edge_row = session.run(edge_count_query, source_segment_ids=source_segment_ids).single()
    edge_count = int(edge_row["edge_count"]) if edge_row is not None and edge_row["edge_count"] is not None else 0
    return entities, edge_count


def _collect_graph_entity_segments(
    graph_store: Any,
    source_segments: list[Segment],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_segment_ids = [
        str(segment.segment_id)
        for segment in source_segments
        if getattr(segment, "segment_id", None) is not None
    ]
    diagnostics: dict[str, Any] = {
        "graph_store_type": type(graph_store).__name__ if graph_store is not None else None,
        "graph_source_segments": len(source_segment_ids),
        "graph_entities_persisted": 0,
        "graph_edges_seen": 0,
    }
    if graph_store is None or not source_segment_ids:
        return [], diagnostics

    try:
        if hasattr(graph_store, "driver") and hasattr(graph_store, "database"):
            raw_entities, edge_count = _collect_neo4j_graph_entities(graph_store, source_segment_ids)
        elif hasattr(graph_store, "_graph"):
            raw_entities, edge_count = _collect_networkx_graph_entities(graph_store, set(source_segment_ids))
        else:
            diagnostics["graph_artifact_persistence_error"] = (
                f"Unsupported graph store type '{type(graph_store).__name__}'"
            )
            return [], diagnostics
    except Exception as exc:
        diagnostics["graph_artifact_persistence_error"] = str(exc)
        return [], diagnostics

    payloads: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for entity in raw_entities:
        entity_id = str(entity.get("id") or entity.get("label") or "unknown_entity")
        source_segment_id_raw = entity.get("source_segment_id")
        source_segment_id = str(source_segment_id_raw) if source_segment_id_raw is not None else None
        key = (entity_id, source_segment_id)
        if key in seen:
            continue
        seen.add(key)
        payloads.append(_serialize_graph_entity_segment(entity))

    diagnostics["graph_entities_persisted"] = len(payloads)
    diagnostics["graph_edges_seen"] = edge_count
    return payloads, diagnostics


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
        payload = dict(item)
        if "content" not in item:
            raise ValueError("Segment payload is missing required field 'content'")
        metadata = payload.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("Segment payload field 'metadata' must be an object")
        metadata = dict(metadata)
        segment_id = payload.get("segment_id", metadata.get("segment_id"))
        parent_id = payload.get("parent_id", metadata.get("parent_id"))
        if segment_id is not None:
            segment_id = str(segment_id)
            metadata.setdefault("segment_id", segment_id)
        if parent_id is not None:
            parent_id = str(parent_id)
            metadata.setdefault("parent_id", parent_id)
        path = payload.get("path", [])
        if not isinstance(path, list):
            raise ValueError("Segment payload field 'path' must be an array")
        segments.append(
            Segment(
                content=payload["content"],
                metadata=metadata,
                segment_id=segment_id,
                parent_id=parent_id,
                level=payload.get("level", 0),
                path=path,
                type=payload.get("type", metadata.get("type", "text")),
                original_format=payload.get("original_format", "text"),
            )
        )
    return segments


def build_index_storage_descriptor(
    *,
    index_artifact_id: str,
    index_type: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    settings = get_settings()
    provider = str(index_type).strip().lower()
    storage: dict[str, Any] = {
        "version": _INDEX_STORAGE_VERSION,
        "backend": "filesystem",
        "vector_store": {
            "provider": index_type,
            "persist_path": None,
            "collection_name": None,
        },
        "doc_store": None,
    }
    if provider == "chroma":
        storage["vector_store"] = {
            "provider": "chroma",
            "persist_path": str(settings.index_storage_root / "chroma"),
            "collection_name": f"idx_{index_artifact_id.replace('-', '')}",
        }
    if params.get("dual_storage"):
        storage["doc_store"] = {
            "provider": "local_pickle",
            "file_path": str(settings.index_storage_root / "docstore" / f"{index_artifact_id}.pkl"),
        }
    return storage


def _storage_vector_store(storage: dict[str, Any]) -> dict[str, Any]:
    vector_store = storage.get("vector_store")
    if isinstance(vector_store, dict):
        return vector_store
    return {}


def _storage_doc_store(storage: dict[str, Any]) -> dict[str, Any] | None:
    doc_store = storage.get("doc_store")
    if isinstance(doc_store, dict):
        return doc_store
    return None


def _compose_index_payload(
    *,
    index_type: str,
    params: dict[str, Any],
    logical_collection_name: str | None,
    logical_docstore_name: str | None,
    raw_segments: list[dict[str, Any]],
    storage: dict[str, Any],
) -> dict[str, Any]:
    return {
        "index_type": index_type,
        "params": dict(params),
        "logical_collection_name": logical_collection_name,
        "logical_docstore_name": logical_docstore_name,
        "dual_storage": bool(_storage_doc_store(storage)),
        "segments_indexed": len(raw_segments),
        "storage": storage,
    }


def _materialize_index_runtime(
    *,
    index_type: str,
    params: dict[str, Any],
    storage: dict[str, Any],
    raw_segments: list[dict[str, Any]],
    raw_parent_segments: list[dict[str, Any]] | None = None,
    runtime_extras: dict[str, Any] | None = None,
    cleanup_override: bool | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
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

    vector_store_kwargs: dict[str, Any] = {"provider": index_type, "embeddings": embeddings}
    vector_store_cfg = _storage_vector_store(storage)
    provider = str(index_type).strip().lower()
    collection_name = vector_store_cfg.get("collection_name")
    if provider == "chroma":
        persist_path = vector_store_cfg.get("persist_path")
        if not isinstance(persist_path, str) or not persist_path.strip():
            raise UnprocessableError("Managed Chroma indexes require a persisted storage path")
        Path(persist_path).mkdir(parents=True, exist_ok=True)
        os.environ["VECTOR_PATH"] = persist_path
    if isinstance(collection_name, str) and collection_name.strip():
        vector_store_kwargs["collection_name"] = collection_name
    if "connection_uri" in materialized_params:
        vector_store_kwargs["connection_uri"] = materialized_params["connection_uri"]
    if cleanup_override is None:
        if "cleanup" in materialized_params:
            vector_store_kwargs["cleanup"] = materialized_params["cleanup"]
    else:
        vector_store_kwargs["cleanup"] = cleanup_override
    vector_store = create_vector_store(**vector_store_kwargs)

    doc_store = None
    if materialized_params.get("dual_storage"):
        doc_store_cfg = _storage_doc_store(storage)
        file_path = doc_store_cfg.get("file_path") if doc_store_cfg is not None else None
        if not isinstance(file_path, str) or not file_path:
            raise UnprocessableError("dual_storage index requires persisted doc store storage metadata")
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        doc_store = LocalPickleStore(file_path=file_path)

    runtime = {
        "index_type": index_type,
        "vector_store": vector_store,
        "doc_store": doc_store,
        "embeddings": embeddings,
        "segments": list(raw_segments),
        "parent_segments": list(raw_parent_segments or []),
        "params": params,
        "storage": storage,
        "runtime_extras": dict(runtime_extras or {}),
    }
    return runtime, materialized_params


def _index_segments(
    *,
    vector_store: Any,
    embeddings: Any,
    doc_store: Any,
    materialized_params: dict[str, Any],
    raw_segments: list[dict[str, Any]],
    raw_parent_segments: list[dict[str, Any]] | None = None,
) -> None:
    indexer = Indexer(vector_store=vector_store, embeddings=embeddings, doc_store=doc_store)
    segments = _build_segments(raw_segments)
    parent_segments = _build_segments(raw_parent_segments or []) if raw_parent_segments else None
    index_kwargs: dict[str, Any] = {"segments": segments, "parent_segments": parent_segments}
    if "batch_size" in materialized_params:
        index_kwargs["batch_size"] = materialized_params["batch_size"]
    indexer.index(**index_kwargs)


def _can_attach_existing_index(index_type: str, storage: dict[str, Any]) -> bool:
    provider = str(index_type).strip().lower()
    vector_store_cfg = _storage_vector_store(storage)
    if provider != "chroma":
        return False
    return bool(vector_store_cfg.get("persist_path")) and bool(vector_store_cfg.get("collection_name"))


def _vector_store_has_documents(vector_store: Any) -> bool:
    get_fn = getattr(vector_store, "get", None)
    if not callable(get_fn):
        return False
    try:
        payload = get_fn(limit=1)
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    ids = payload.get("ids")
    return isinstance(ids, list) and bool(ids)


def _storage_is_present(runtime: dict[str, Any], storage: dict[str, Any]) -> bool:
    if not _vector_store_has_documents(runtime["vector_store"]):
        return False
    doc_store_cfg = _storage_doc_store(storage)
    if doc_store_cfg is None:
        return True
    file_path = doc_store_cfg.get("file_path")
    return isinstance(file_path, str) and Path(file_path).exists()


def run_loader(loader_type: str, params: dict[str, Any], run_payload: dict[str, Any] | None) -> dict[str, Any]:
    try:
        loader_cls = resolve_loader_class(loader_type)
        kwargs = _normalize_loader_kwargs(loader_type, dict(params), run_payload)
        kwargs = _prepare_component_kwargs(loader_cls.__init__, kwargs)
        kwargs = _force_headless_playwright(loader_type, kwargs)
        loader = loader_cls(**kwargs)
        docs = _call_maybe_async(loader.load)
        diagnostics: dict[str, Any] = {}
        if hasattr(loader, "last_stats"):
            diagnostics["loader_stats"] = getattr(loader, "last_stats")
        if hasattr(loader, "last_errors"):
            diagnostics["loader_errors"] = getattr(loader, "last_errors")
        if not docs:
            raise ServiceUnavailableError(
                message=f"rag-lib loader '{loader_type}' returned no documents",
                details={
                    "loader_type": loader_type,
                    "loader_kwargs": sorted(kwargs.keys()),
                    "diagnostics": diagnostics,
                },
                rag_lib_exception_type="SilentEmptyResult",
            )
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
        if not out:
            raise ServiceUnavailableError(
                message=f"rag-lib splitter '{splitter_type}' returned no segments",
                details={"splitter_type": splitter_type},
                rag_lib_exception_type="SilentEmptyResult",
            )
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
        graph_store = kwargs.get("store") or kwargs.get("graph_store")

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
            diagnostics: dict[str, Any] = {}
            persisted_artifacts: list[dict[str, Any]] = []
            if processor_type == "EntityExtractor" and source_segments:
                graph_entities, diagnostics = _collect_graph_entity_segments(graph_store, segs)
                if graph_entities:
                    persisted_artifacts.append(
                        {
                            "artifact_kind": "graph_entity",
                            "payload": graph_entities,
                        }
                    )
            return {
                "kind": "none",
                "payload": [],
                "persisted_artifacts": persisted_artifacts,
                "runtime_extras": {"graph_store": graph_store} if graph_store is not None else {},
                "diagnostics": diagnostics,
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
            raise ServiceUnavailableError(
                message=f"rag-lib processor '{processor_type}' returned an empty {kind} payload",
                details={"processor_type": processor_type},
                rag_lib_exception_type="SilentEmptyResult",
            )
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
    logical_collection_name: str | None = None,
    logical_docstore_name: str | None = None,
    storage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        storage_descriptor = dict(storage or build_index_storage_descriptor(
            index_artifact_id=index_artifact_id,
            index_type=index_type,
            params=params,
        ))
        runtime, materialized_params = _materialize_index_runtime(
            index_type=index_type,
            params=params,
            storage=storage_descriptor,
            raw_segments=raw_segments,
            raw_parent_segments=raw_parent_segments,
            runtime_extras=runtime_extras,
        )
        _index_segments(
            vector_store=runtime["vector_store"],
            embeddings=runtime["embeddings"],
            doc_store=runtime["doc_store"],
            materialized_params=materialized_params,
            raw_segments=raw_segments,
            raw_parent_segments=raw_parent_segments,
        )
        if not raw_segments:
            raise ServiceUnavailableError(
                message=f"rag-lib index build for '{index_type}' received zero segments",
                details={"index_type": index_type},
                rag_lib_exception_type="SilentEmptyResult",
            )
        INDEX_RUNTIME_REGISTRY[index_artifact_id] = runtime
        return _compose_index_payload(
            index_type=index_type,
            params=params,
            logical_collection_name=logical_collection_name,
            logical_docstore_name=logical_docstore_name,
            raw_segments=raw_segments,
            storage=storage_descriptor,
        )
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib index build failed for '{index_type}'",
            details={"error": str(exc), "index_type": index_type},
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def restore_index_runtime(
    index_artifact_id: str,
    index_type: str,
    params: dict[str, Any],
    raw_segments: list[dict[str, Any]],
    raw_parent_segments: list[dict[str, Any]] | None = None,
    runtime_extras: dict[str, Any] | None = None,
    logical_collection_name: str | None = None,
    logical_docstore_name: str | None = None,
    storage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        if not raw_segments:
            raise ServiceUnavailableError(
                message=f"rag-lib index runtime restore for '{index_type}' received zero source segments",
                details={"index_type": index_type, "index_artifact_id": index_artifact_id},
                rag_lib_exception_type="SilentEmptyResult",
            )
        storage_descriptor = dict(storage or build_index_storage_descriptor(
            index_artifact_id=index_artifact_id,
            index_type=index_type,
            params=params,
        ))
        runtime, materialized_params = _materialize_index_runtime(
            index_type=index_type,
            params=params,
            storage=storage_descriptor,
            raw_segments=raw_segments,
            raw_parent_segments=raw_parent_segments,
            runtime_extras=runtime_extras,
            cleanup_override=False,
        )
        if _can_attach_existing_index(index_type, storage_descriptor):
            if not _storage_is_present(runtime, storage_descriptor):
                runtime, materialized_params = _materialize_index_runtime(
                    index_type=index_type,
                    params=params,
                    storage=storage_descriptor,
                    raw_segments=raw_segments,
                    raw_parent_segments=raw_parent_segments,
                    runtime_extras=runtime_extras,
                    cleanup_override=True,
                )
                _index_segments(
                    vector_store=runtime["vector_store"],
                    embeddings=runtime["embeddings"],
                    doc_store=runtime["doc_store"],
                    materialized_params=materialized_params,
                    raw_segments=raw_segments,
                    raw_parent_segments=raw_parent_segments,
                )
        else:
            _index_segments(
                vector_store=runtime["vector_store"],
                embeddings=runtime["embeddings"],
                doc_store=runtime["doc_store"],
                materialized_params=materialized_params,
                raw_segments=raw_segments,
                raw_parent_segments=raw_parent_segments,
            )
        INDEX_RUNTIME_REGISTRY[index_artifact_id] = runtime
        return _compose_index_payload(
            index_type=index_type,
            params=params,
            logical_collection_name=logical_collection_name,
            logical_docstore_name=logical_docstore_name,
            raw_segments=raw_segments,
            storage=storage_descriptor,
        )
    except APIError:
        raise
    except Exception as exc:
        raise ServiceUnavailableError(
            message=f"rag-lib index runtime restore failed for '{index_type}'",
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


def _project_graph_store_spec(project_graph_store_config: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(project_graph_store_config, dict):
        return None
    provider = project_graph_store_config.get("provider")
    params = project_graph_store_config.get("params")
    if not isinstance(provider, str) or not provider.strip():
        return None
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise UnprocessableError("Project graph_store_config.params must be an object")
    return {
        "object_type": "create_graph_store",
        "provider": provider,
        **dict(params),
    }


def create_retriever_runtime(
    retriever_id: str,
    retriever_type: str,
    index_artifact_id: str | None,
    params: dict[str, Any],
    *,
    source_payloads: list[dict[str, Any]] | None = None,
    source_artifact_kind: str | None = None,
    project_graph_store_config: dict[str, Any] | None = None,
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
        index_runtime_extras = (
            index_runtime.get("runtime_extras")
            if isinstance(index_runtime, dict) and isinstance(index_runtime.get("runtime_extras"), dict)
            else {}
        )
        graph_store_from_runtime = index_runtime_extras.get("graph_store")
        graph_store_spec = _project_graph_store_spec(project_graph_store_config)
        graph_store_from_project = None

        def build_runtime_retriever(spec_type: str, spec_params: dict[str, Any]):
            nonlocal graph_store_from_project
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
            if spec_type == "GraphRetriever" and "graph_store" in param_names and "graph_store" not in kwargs:
                graph_store = graph_store_from_runtime
                if graph_store is None and graph_store_spec is not None:
                    if graph_store_from_project is None:
                        graph_store_from_project = _materialize_runtime_value(graph_store_spec)
                    graph_store = graph_store_from_project
                if graph_store is not None:
                    kwargs["graph_store"] = graph_store
            if inspect.isfunction(target):
                return _call_maybe_async(target, **kwargs)
            return target(**kwargs)

        if not source_objects:
            raise ServiceUnavailableError(
                message=f"rag-lib retriever '{retriever_type}' received no source documents or segments",
                details={"retriever_type": retriever_type, "index_artifact_id": index_artifact_id},
                rag_lib_exception_type="SilentEmptyResult",
            )
        retriever = build_runtime_retriever(retriever_type, params)
        if retriever is None:
            raise ServiceUnavailableError(
                message=f"rag-lib retriever creation returned no runtime for '{retriever_type}'",
                details={"retriever_type": retriever_type},
                rag_lib_exception_type="SilentEmptyResult",
            )
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
