from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Callable

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[1]

if __package__ in {None, ""}:
    # Allow direct execution from IDEs by making the repo root importable.
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from scripts.lib.pipeline_example_export import SnapshotExporter, make_run_root
from scripts.lib.pipeline_example_manifest import PipelineExampleSpec, load_manifest, select_examples
from scripts.lib.pipeline_example_runner import ApiClient, ExampleRunResult, PipelineExampleRunner, RunnerConfig


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pipeline example simulations against remote API.")
    parser.add_argument("--base-url", required=False, default="http://127.0.0.1:8007/api/v1", help="Remote API base URL (for example: https://host/api/v1)")
    parser.add_argument("--run-all", action="store_true", help="Run all manifest examples in batch mode")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive selection mode")
    parser.add_argument("--examples", help="Comma-separated example IDs to run")
    parser.add_argument("--manifest", default="examples/pipeline_examples/manifest.v1.yaml")
    parser.add_argument("--results-dir", default="results/pipeline_examples")
    parser.add_argument("--token-env", default="RAG_API_TOKEN")
    parser.add_argument("--timeout-seconds", type=int, default=1200)
    parser.add_argument("--poll-interval-seconds", type=int, default=2)
    parser.add_argument(
        "--get-retry-attempts",
        type=int,
        default=5,
        help="Retries for transient GET request socket/network errors",
    )
    parser.add_argument(
        "--get-retry-backoff-seconds",
        type=float,
        default=0.5,
        help="Base exponential backoff for transient GET request retries",
    )
    parser.add_argument(
        "--continue-on-error",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Continue with next example after failure",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--example-docs-root",
        default="example_docs",
        help="Root folder containing source files referenced by manifest",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level for runner internals",
    )
    return parser


def _parse_examples_arg(raw: str | None) -> list[str] | None:
    if raw is None or not raw.strip():
        return None
    values = [value.strip() for value in raw.split(",") if value.strip()]
    return values or None


def _resolve_repo_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _print_example_list(
    print_func: Callable[[str], None],
    specs: list[PipelineExampleSpec],
    *,
    executed_ids: set[str] | None = None,
) -> None:
    executed_ids = executed_ids or set()
    print_func("Available examples:")
    for idx, spec in enumerate(specs, start=1):
        suffix = " [done]" if spec.example_id in executed_ids else ""
        print_func(f"{idx:02d}. {spec.example_id} ({spec.source_example_file}){suffix}")


def _select_one_interactive(
    specs: list[PipelineExampleSpec],
    executed_ids: set[str],
    input_func: Callable[[str], str],
    print_func: Callable[[str], None],
) -> PipelineExampleSpec | str:
    while True:
        raw = input_func("Select example (number/id), or command [list|all|quit]: ").strip()
        lowered = raw.lower()
        if lowered == "list":
            _print_example_list(print_func, specs, executed_ids=executed_ids)
            continue
        if lowered == "all":
            return "all"
        if lowered == "quit":
            return "quit"
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(specs):
                spec = specs[idx]
                if spec.example_id in executed_ids:
                    print_func("Example already executed. Select another one.")
                    continue
                return spec
            print_func("Invalid number. Try again.")
            continue
        matches = [spec for spec in specs if spec.example_id == raw]
        if matches:
            spec = matches[0]
            if spec.example_id in executed_ids:
                print_func("Example already executed. Select another one.")
                continue
            return spec
        print_func("Invalid input. Use number, example_id, list, all, or quit.")


def _interactive_next_action(input_func: Callable[[str], str], print_func: Callable[[str], None]) -> str:
    while True:
        raw = input_func("Next action [select|all|quit]: ").strip().lower()
        if raw in {"select", "all", "quit"}:
            return raw
        print_func("Invalid input. Use select, all, or quit.")


def _write_global_summaries(exporter: SnapshotExporter, results: list[ExampleRunResult]) -> None:
    passed = [result for result in results if result.passed]
    failed = [result for result in results if not result.passed]
    exporter.write_run_json(
        "run.summary.json",
        {
            "total_executed": len(results),
            "passed": len(passed),
            "failed": len(failed),
            "results": [asdict(result) for result in results],
        },
    )
    exporter.write_run_json(
        "run.errors.json",
        {
            "failed": [asdict(result) for result in failed],
        },
    )


def _run_interactive(
    runner: PipelineExampleRunner,
    specs: list[PipelineExampleSpec],
    input_func: Callable[[str], str],
    print_func: Callable[[str], None],
) -> tuple[list[ExampleRunResult], bool]:
    pending = list(specs)
    executed_ids: set[str] = set()
    results: list[ExampleRunResult] = []
    if not pending:
        return results, False

    _print_example_list(print_func, specs, executed_ids=executed_ids)
    while pending:
        selected = _select_one_interactive(specs, executed_ids, input_func, print_func)
        if selected == "quit":
            return results, True
        if selected == "all":
            results.extend(runner.run_many(pending))
            return results, False

        result = runner.run_one(selected)
        results.append(result)
        executed_ids.add(selected.example_id)
        pending = [spec for spec in pending if spec.example_id != selected.example_id]
        if not result.passed and not runner.config.continue_on_error:
            return results, False
        if not pending:
            return results, False

        next_action = _interactive_next_action(input_func, print_func)
        if next_action == "quit":
            return results, True
        if next_action == "all":
            results.extend(runner.run_many(pending))
            return results, False
    return results, False


def main(
    argv: list[str] | None = None,
    *,
    input_func: Callable[[str], str] = input,
    print_func: Callable[[str], None] = print,
) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    manifest = load_manifest(_resolve_repo_path(args.manifest))
    example_filter = _parse_examples_arg(args.examples)
    if args.run_all:
        mode = "all"
        filtered_specs = list(manifest.examples)
    elif example_filter:
        mode = "subset"
        filtered_specs = select_examples(manifest, example_filter)
    else:
        mode = "interactive"
        filtered_specs = list(manifest.examples)

    if args.dry_run:
        if args.run_all:
            dry_specs = list(manifest.examples)
        elif example_filter:
            dry_specs = filtered_specs
        else:
            dry_specs = list(manifest.examples)
        print_func("Dry run selected examples:")
        for spec in dry_specs:
            print_func(f"- {spec.example_id}")
        return 0

    token = os.getenv(args.token_env)
    if not token:
        print_func(f"Missing bearer token in env var '{args.token_env}'")
        return 1

    run_root = make_run_root(_resolve_repo_path(args.results_dir))
    exporter = SnapshotExporter(run_root=run_root)
    client = ApiClient(
        base_url=args.base_url,
        token=token,
        timeout_seconds=args.timeout_seconds,
        get_retry_attempts=args.get_retry_attempts,
        get_retry_backoff_seconds=args.get_retry_backoff_seconds,
    )
    runner = PipelineExampleRunner(
        api_client=client,
        exporter=exporter,
        config=RunnerConfig(
            example_docs_root=_resolve_repo_path(args.example_docs_root),
            poll_interval_seconds=args.poll_interval_seconds,
            poll_timeout_seconds=args.timeout_seconds,
            continue_on_error=args.continue_on_error == "true",
        ),
    )

    if mode == "all":
        results = runner.run_many(manifest.examples)
        quit_early = False
    elif mode == "subset":
        results = runner.run_many(filtered_specs)
        quit_early = False
    else:
        results, quit_early = _run_interactive(runner, list(manifest.examples), input_func, print_func)

    _write_global_summaries(exporter, results)
    print_func(f"Results exported to: {run_root.as_posix()}")
    passed = [result for result in results if result.passed]
    failed = [result for result in results if not result.passed]
    print_func(f"Executed={len(results)} Passed={len(passed)} Failed={len(failed)}")

    if quit_early:
        return 0 if not failed else 1
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
