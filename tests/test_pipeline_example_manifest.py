from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.lib.pipeline_example_manifest import (
    PipelineExampleManifest,
    PipelineExampleSpec,
    PipelineRunSpec,
    RetrievalPlan,
    RetrievalQueryPlan,
    build_run_payload,
    load_manifest,
    select_examples,
)


MANIFEST_PATH = Path("examples/pipeline_examples/manifest.v1.yaml")
EXPECTED_IDS = [
    "01_text_basic",
    "02_markdown_enrichment",
    "02_markdown_enrichment_vector",
    "03_pdf_semantic",
    "04_pdf_raptor",
    "05_docx_graph",
    "06_docx_regex",
    "07_csv_table_summary",
    "07_md_table_summary",
    "08_excel_csv_basic",
    "08_excel_md_basic",
    "09_json_hybrid",
    "10_text_ensemble",
    "11_log_regex_loader",
    "12_qa_loader",
    "13_dual_storage",
    "14_mineru_pdf",
    "15_pptx_unsupported",
    "16_html_html",
    "16_html_md",
    "17A_web_loader_plantpad",
    "17B_web_loader_quotes",
    "17C_web_loader_example",
    "17_web_loader",
]


def _query_plan() -> RetrievalQueryPlan:
    return RetrievalQueryPlan(name="hello", query="hello", top_k=3, strict_match=True)


def _retrieval_plan() -> RetrievalPlan:
    return RetrievalPlan(
        name="vector",
        source_kind="index",
        source_stage_name=None,
        retriever_type="create_vector_retriever",
        retriever_params={"top_k": 3},
        requires_session=False,
        queries=[_query_plan()],
    )


def _run_spec(example_id: str) -> PipelineRunSpec:
    return PipelineRunSpec(
        run_name="main",
        pipeline_create_payload={
            "name": f"{example_id}-pipeline",
            "loader": {"type": "TextLoader", "params": {}},
            "inputs": [],
            "stages": [],
            "indexing": {"index_type": "chroma", "params": {}},
        },
        run_payload_template={"metadata": {"source": "test"}},
        retrievals=[_retrieval_plan()],
    )


def _spec(*, example_id: str, input_mode: str, input_spec: dict[str, object]) -> PipelineExampleSpec:
    return PipelineExampleSpec(
        example_id=example_id,
        source_example_file=f"{example_id}.py",
        input_mode=input_mode,
        input_spec=input_spec,
        runs=[_run_spec(example_id)],
        expected_outcome="success",
        notes="test",
    )


def test_manifest_contains_expected_ids() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    assert manifest.version == "v2"
    assert [item.example_id for item in manifest.examples] == EXPECTED_IDS


def test_select_examples_respects_requested_order() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    selected = select_examples(manifest, ["10_text_ensemble", "01_text_basic"])
    assert [item.example_id for item in selected] == ["10_text_ensemble", "01_text_basic"]


def test_select_examples_rejects_unknown_ids() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    with pytest.raises(ValueError, match="Unknown example_id"):
        select_examples(manifest, ["does_not_exist"])


def test_manifest_rejects_duplicate_ids(tmp_path: Path) -> None:
    entry = {
        "example_id": "dup",
        "source_example_file": "dup.py",
        "input_mode": "file",
        "input_spec": {"file": "abc.txt"},
        "runs": [
            {
                "run_name": "main",
                "pipeline_create_payload": {
                    "name": "p",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [],
                },
                "run_payload_template": {},
                "retrievals": [
                    {
                        "name": "vector",
                        "source": {"kind": "index"},
                        "create": {"retriever_type": "create_vector_retriever", "params": {}},
                        "queries": [{"name": "q", "query": "q", "top_k": 1, "strict_match": False}],
                    }
                ],
            }
        ],
        "expected_outcome": "success",
        "notes": "x",
    }
    manifest_payload = {"version": "v2", "examples": [entry, dict(entry)]}
    path = tmp_path / "dup.json"
    path.write_text(json.dumps(manifest_payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate example_id"):
        load_manifest(path)


def test_manifest_requires_runs(tmp_path: Path) -> None:
    manifest_payload = {
        "version": "v2",
        "examples": [
            {
                "example_id": "missing_runs",
                "source_example_file": "missing_runs.py",
                "input_mode": "file",
                "input_spec": {"file": "doc.txt"},
                "expected_outcome": "success",
                "notes": "x",
            }
        ],
    }
    path = tmp_path / "missing_runs.json"
    path.write_text(json.dumps(manifest_payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required key 'runs'"):
        load_manifest(path)


def test_build_run_payload_url_mode() -> None:
    spec = _spec(example_id="url_example", input_mode="url", input_spec={"url": "https://example.com"})
    payload = build_run_payload(spec, spec.runs[0], Path("."))
    assert payload["url"] == "https://example.com"


def test_build_run_payload_file_mode(tmp_path: Path) -> None:
    file_path = tmp_path / "demo.txt"
    file_path.write_text("payload-content", encoding="utf-8")

    spec = _spec(example_id="file_example", input_mode="file", input_spec={"file": "demo.txt"})
    payload = build_run_payload(spec, spec.runs[0], tmp_path)

    assert payload["file_name"] == "demo.txt"
    assert payload["upload_file_path"] == str(file_path)


def test_build_run_payload_segments_mode() -> None:
    spec = _spec(
        example_id="segments_example",
        input_mode="segments",
        input_spec={"segments": [{"content": "hello", "metadata": {}, "segment_id": "seg-1"}]},
    )
    payload = build_run_payload(spec, spec.runs[0], Path("."))
    assert payload["segments"][0]["segment_id"] == "seg-1"


def test_build_run_payload_file_mode_rejects_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("x", encoding="utf-8")
    spec = _spec(example_id="escape", input_mode="file", input_spec={"file": "../outside.txt"})

    with pytest.raises(ValueError, match="escapes example_docs root"):
        build_run_payload(spec, spec.runs[0], tmp_path)
