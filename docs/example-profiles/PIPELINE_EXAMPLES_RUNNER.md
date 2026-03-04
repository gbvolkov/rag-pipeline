# Pipeline Examples Runner

`run_pipeline_examples` is a remote-only harness that simulates selected `rag-lib` examples through the `rag-api` endpoints, then exports pipeline and retrieval snapshots as prettified JSON.

## Files

- Manifest: `examples/pipeline_examples/manifest.v1.yaml`
- CLI entrypoint: `scripts/run_pipeline_examples.py`
- Library modules:
  - `scripts/lib/pipeline_example_manifest.py`
  - `scripts/lib/pipeline_example_runner.py`
  - `scripts/lib/pipeline_example_export.py`

## Command

```bash
uv run python scripts/run_pipeline_examples.py --base-url <url> [options]
```

Required:

- `--base-url` API base URL (for example `http://localhost:8000/api/v1`)

Options:

- `--run-all` run all examples from manifest
- `--interactive` run step-by-step selection prompts
- `--examples` comma-separated IDs (for example `01_text_basic,03_pdf_semantic`)
- `--manifest` defaults to `examples/pipeline_examples/manifest.v1.yaml`
- `--results-dir` defaults to `results/pipeline_examples`
- `--token-env` defaults to `RAG_API_TOKEN`
- `--timeout-seconds` defaults to `1200`
- `--poll-interval-seconds` defaults to `2`
- `--continue-on-error` defaults to `true` (`true|false`)
- `--dry-run` print selected examples without executing API calls
- `--example-docs-root` defaults to `C:/Projects/rag-pipeline/example_docs`

Authentication:

- Bearer token is required for actual execution modes.
- Token is loaded from `--token-env`.
- Missing token exits immediately with code `1`.

## Mode Precedence

1. If `--run-all` is set, all manifest examples run (no prompts).
2. Else if `--examples` is set, only selected IDs run (no prompts).
3. Else interactive mode starts by default.

## Interactive Mode

Startup:

- Prints indexed list of example IDs.
- Prompt: `Select example (number/id), or command [list|all|quit]:`

After one example completes:

- Prompt: `Next action [select|all|quit]:`

Commands:

- `list` print remaining examples
- `all` run remaining examples in batch mode
- `quit` stop and emit summary
- Invalid input re-prompts

## Execution Flow Per Example

1. Validate manifest entry and resolve input from `example_docs`.
2. `POST /projects`
3. `POST /projects/{pid}/pipelines`
4. `POST /projects/{pid}/pipelines/{plid}/runs`
5. Poll `GET /jobs/{job_id}` to terminal status
6. Export run/job snapshots
7. Export artifact list/detail snapshots (documents, segments, indices)
8. If index exists: create/init/query/release retriever and export retrieval snapshots
9. Write per-example `summary.json`

## Results Layout

Root:

- `results/pipeline_examples/<timestamp>/`

Per example:

- `results/pipeline_examples/<timestamp>/<example_id>/`

Global files:

- `run.summary.json`
- `run.errors.json`

Per-example snapshots include:

- project/pipeline/run submit request/response
- final job response
- artifact list/detail payloads
- retriever create/init/query/result/list/release payloads (when applicable)
- `summary.json`
- `error.json` on failure

## Exit Codes

- `0` if all executed examples passed
- `1` if any executed example failed
- Interactive `quit` returns `0` only when all executed examples passed
