from __future__ import annotations

from pathlib import Path
from typing import Callable

import pytest

import scripts.run_pipeline_examples as cli
from scripts.lib.pipeline_example_manifest import PipelineExampleManifest, PipelineExampleSpec, RetrievalPlan
from scripts.lib.pipeline_example_runner import ExampleRunResult


def _spec(example_id: str) -> PipelineExampleSpec:
    return PipelineExampleSpec(
        example_id=example_id,
        source_example_file=f"{example_id}.py",
        input_mode="text",
        input_spec={"text": f"payload-{example_id}"},
        pipeline_create_payload={
            "name": f"pipeline-{example_id}",
            "loader": {"type": "TextLoader", "params": {}},
            "inputs": [],
            "segmentation_stages": [],
            "indexing": {"index_type": "chroma", "params": {}},
        },
        run_payload_template={},
        retrieval_plan=RetrievalPlan(
            retriever_type="create_vector_retriever",
            retriever_params={"top_k": 3},
            requires_session=False,
            query="hello",
            top_k=3,
            strict_match=False,
        ),
        expected_outcome="success",
        notes="test",
    )


class _FakeApiClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: int):
        self.base_url = base_url
        self.token = token
        self.timeout_seconds = timeout_seconds


class _FakeRunner:
    instances: list["_FakeRunner"] = []
    fail_ids: set[str] = set()

    def __init__(self, api_client, exporter, config):
        self.api_client = api_client
        self.exporter = exporter
        self.config = config
        self.run_many_calls: list[list[str]] = []
        self.run_one_calls: list[str] = []
        _FakeRunner.instances.append(self)

    def run_many(self, specs: list[PipelineExampleSpec]) -> list[ExampleRunResult]:
        ids = [spec.example_id for spec in specs]
        self.run_many_calls.append(ids)
        return [self._result(example_id) for example_id in ids]

    def run_one(self, spec: PipelineExampleSpec) -> ExampleRunResult:
        self.run_one_calls.append(spec.example_id)
        return self._result(spec.example_id)

    @classmethod
    def _result(cls, example_id: str) -> ExampleRunResult:
        status = "failed" if example_id in cls.fail_ids else "passed"
        err = {"error_code": "forced_failure"} if status == "failed" else None
        return ExampleRunResult(
            example_id=example_id,
            status=status,
            started_at="2026-03-04T00:00:00Z",
            finished_at="2026-03-04T00:00:01Z",
            error=err,
        )


def _setup_cli_mocks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> PipelineExampleManifest:
    _FakeRunner.instances = []
    _FakeRunner.fail_ids = set()

    manifest = PipelineExampleManifest(version="v1", examples=[_spec("ex1"), _spec("ex2"), _spec("ex3")])

    run_root = tmp_path / "results" / "20260304T000000Z"
    run_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli, "load_manifest", lambda _: manifest)
    monkeypatch.setattr(cli, "make_run_root", lambda _: run_root)
    monkeypatch.setattr(cli, "ApiClient", _FakeApiClient)
    monkeypatch.setattr(cli, "PipelineExampleRunner", _FakeRunner)
    monkeypatch.setenv("RAG_API_TOKEN", "test-token")
    return manifest


def _next_input(values: list[str]) -> Callable[[str], str]:
    iterator = iter(values)

    def _inner(_: str) -> str:
        return next(iterator)

    return _inner


def test_cli_run_all_precedence_over_examples(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    manifest = _setup_cli_mocks(monkeypatch, tmp_path)
    out: list[str] = []

    rc = cli.main(
        ["--base-url", "http://api.local/api/v1", "--run-all", "--examples", "unknown_id"],
        input_func=lambda _: (_ for _ in ()).throw(AssertionError("interactive input should not be used")),
        print_func=out.append,
    )

    assert rc == 0
    runner = _FakeRunner.instances[-1]
    assert runner.run_many_calls == [[spec.example_id for spec in manifest.examples]]
    assert runner.run_one_calls == []


def test_cli_examples_mode_runs_selected_subset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _setup_cli_mocks(monkeypatch, tmp_path)
    out: list[str] = []

    rc = cli.main(
        ["--base-url", "http://api.local/api/v1", "--examples", "ex3,ex1"],
        input_func=lambda _: (_ for _ in ()).throw(AssertionError("interactive input should not be used")),
        print_func=out.append,
    )

    assert rc == 0
    runner = _FakeRunner.instances[-1]
    assert runner.run_many_calls == [["ex3", "ex1"]]
    assert runner.run_one_calls == []


def test_cli_interactive_list_invalid_then_select_and_all(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _setup_cli_mocks(monkeypatch, tmp_path)
    output: list[str] = []
    input_func = _next_input(["list", "invalid", "2", "all"])

    rc = cli.main(
        ["--base-url", "http://api.local/api/v1", "--interactive"],
        input_func=input_func,
        print_func=output.append,
    )

    assert rc == 0
    runner = _FakeRunner.instances[-1]
    assert runner.run_one_calls == ["ex2"]
    assert runner.run_many_calls == [["ex1", "ex3"]]
    assert any("Invalid input" in line for line in output)


def test_cli_interactive_quit_before_any_execution(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _setup_cli_mocks(monkeypatch, tmp_path)
    output: list[str] = []

    rc = cli.main(
        ["--base-url", "http://api.local/api/v1", "--interactive"],
        input_func=_next_input(["quit"]),
        print_func=output.append,
    )

    assert rc == 0
    runner = _FakeRunner.instances[-1]
    assert runner.run_one_calls == []
    assert runner.run_many_calls == []


def test_cli_interactive_quit_after_failure_returns_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _setup_cli_mocks(monkeypatch, tmp_path)
    _FakeRunner.fail_ids = {"ex1"}
    output: list[str] = []

    rc = cli.main(
        ["--base-url", "http://api.local/api/v1", "--interactive"],
        input_func=_next_input(["1", "quit"]),
        print_func=output.append,
    )

    assert rc == 1
    runner = _FakeRunner.instances[-1]
    assert runner.run_one_calls == ["ex1"]


def test_cli_no_mode_defaults_to_interactive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _setup_cli_mocks(monkeypatch, tmp_path)
    output: list[str] = []

    rc = cli.main(
        ["--base-url", "http://api.local/api/v1"],
        input_func=_next_input(["quit"]),
        print_func=output.append,
    )

    assert rc == 0
    runner = _FakeRunner.instances[-1]
    assert runner.run_one_calls == []
    assert runner.run_many_calls == []
    assert not any("No execution mode selected" in line for line in output)
