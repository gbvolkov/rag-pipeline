## rag-pipeline

FastAPI orchestration layer for `rag-lib` with:

- strict `rag-lib` execution (no hidden fallback logic),
- full runtime capability discovery (`/api/v1/capabilities`),
- immutable pipeline definitions plus artifact versioning and lineage persistence,
- pipeline runs, reindexing, retriever lifecycle, and retrieval result APIs,
- Celery + Redis async job execution with Postgres as canonical state,
- example manifest tooling for remote pipeline simulation and parity checks.

## Quick Start

1. Install dependencies:

```bash
uv sync --all-groups
```

If you plan to use `WebLoader` or `AsyncWebLoader` with `fetch_mode="playwright"` outside Docker, install Chromium once:

```bash
uv run python -m playwright install chromium
```

2. Set environment (example):

```bash
set DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/rag_api
set REDIS_URL=redis://localhost:6379/0
set CELERY_ALWAYS_EAGER=true
```

3. Run API:

```bash
uv run uvicorn app.main:app --reload
```

4. Open docs:

- `http://127.0.0.1:8000/api/v1/docs`

## Documentation

- `docs/DEVELOPERS_GUIDE.md`: detailed pipeline developer guide covering entry points, production pipeline manifests, execution flow, example manifests, and manifest enums/types.
- `docs/DEVOPS_GUIDE.md`: Docker Compose operations guide covering service topology, startup ordering, storage, configuration precedence, and troubleshooting.
- `docs/example-profiles/PIPELINE_EXAMPLES_RUNNER.md`: CLI usage for `scripts/run_pipeline_examples.py`.
- `GET /api/v1/capabilities`: live runtime inventory of discovered `rag-lib` loaders, splitters, processors, retrievers, and advisory literals.

## Docker Compose

Run full stack (API + worker + Postgres + Redis + MinIO + Neo4j):

For full operational details, see `docs/DEVOPS_GUIDE.md`.

1. Prepare env file:

```bash
cp .env.template .env
```

Set at least `OPENAI_API_KEY` (required for default OpenAI embeddings/indexing).

2. Start services:

```bash
docker compose up -d --build
```

Dependency install is cached in the image build:

- changing app code rebuilds the image, but reuses the existing dependency layer;
- changing `pyproject.toml`, `uv.lock`, or `.python-version` rebuilds dependencies;
- package upgrades should go through `uv lock` / `uv lock --upgrade-package ...`, then `docker compose up -d --build`.

Only one external port is published to avoid conflicts:

- `http://127.0.0.1:8007/api/v1/docs`

Notes:

- API, worker, and `nltk-init` now reuse the same built app image.
- The app image installs the Linux Chromium runtime libraries via `apt`, then downloads the Playwright Chromium binary during `docker compose build`.
- This avoids the heavier `playwright install --with-deps` path, which can be OOM-killed on smaller Docker builders.

## Tests

```bash
uv run pytest -q
```

## Parity Tools

- Drift gate:

```bash
uv run python scripts/check_parity_drift.py
```

- Example conformance runner:

```bash
uv run python scripts/run_example_conformance.py
```

Output report:

- `reports/example_conformance_report.json`

## Project Structure

- `app/api/routes.py`: all public API endpoints.
- `app/models/entities.py`: persistence model and constraints.
- `app/services/capabilities.py`: runtime `rag-lib` feature discovery.
- `app/services/jobs.py`: orchestration state machine and artifact persistence.
- `app/services/rag_adapter.py`: strict adapter into `rag-lib`.
- `app/services/pipeline_validator.py`: production pipeline manifest validation and shape classification.
- `app/services/runtime_objects.py`: nested manifest `object_type` validation/materialization.
- `scripts/run_pipeline_examples.py`: remote example-manifest runner.
- `scripts/lib/pipeline_example_manifest.py`: example manifest loader and schema helpers.
