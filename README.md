## rag-pipeline

FastAPI orchestration layer for `rag-lib` with:

- strict `rag-lib` execution (no hidden fallback logic),
- full runtime capability discovery (`/api/v1/capabilities`),
- example-profile coverage matrix (`/api/v1/capabilities/examples`),
- 33 required orchestration/retrieval/lineage endpoints from TRD + 2 parity endpoints,
- Celery + Redis async job execution with Postgres as canonical state,
- artifact versioning and lineage persistence.

## Quick Start

1. Install dependencies:

```bash
uv sync --all-groups
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

## Docker Compose

Run full stack (API + worker + Postgres + Redis + MinIO):

1. Prepare env file:

```bash
cp .env.template .env
```

Set at least `OPENAI_API_KEY` (required for default OpenAI embeddings/indexing).

2. Start services:

```bash
docker compose up -d --build
```

Only one external port is published to avoid conflicts:

- `http://127.0.0.1:8007/api/v1/docs`

Notes:

- The compose file mounts sibling repo `../rag-lib` into containers as `/opt/rag-lib`.
- If your local path is different, update `docker-compose.yml` volume paths.

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
- `app/services/example_profiles.py`: example profile catalog/matrix sync.
- `app/services/jobs.py`: orchestration state machine and artifact persistence.
- `app/services/rag_adapter.py`: strict adapter into `rag-lib`.
