# DevOps Guide

This document describes the `docker compose` stack shipped with `rag-pipeline`.

It is a local or shared-development deployment guide, not a production hardening guide. The focus here is:

- what each service does
- how startup ordering works
- which settings actually come from `.env`
- what data is persisted
- how to operate, inspect, and reset the stack safely

## 1. Compose Stack Overview

The stack is defined in `docker-compose.yml`.

| Service | Image / command | Responsibility | Host exposure |
| --- | --- | --- | --- |
| `rag-pipeline` | `rag-pipeline-app:dev`, `uvicorn app.main:app` | FastAPI HTTP API | `8007:8007` |
| `rag-worker` | `rag-pipeline-app:dev`, `celery -A app.core.celery_app.celery_app worker` | Async pipeline, reindex, and MinerU jobs | none |
| `postgres` | `postgres:16-alpine` | Canonical relational state | none |
| `redis` | `redis:7-alpine` | Celery broker and result backend | none |
| `minio` | `minio/minio:latest` | S3-compatible object storage | none |
| `neo4j` | `neo4j:5-community` | Graph storage for graph-oriented flows | none |
| `minio-init` | `minio/mc:latest` | One-shot bucket bootstrap | none |
| `nltk-init` | `rag-pipeline-app:dev`, inline Python | One-shot NLTK data bootstrap | none |

Important consequences:

- Only the API is published to the host by default.
- Postgres, Redis, MinIO, and Neo4j are reachable inside Compose by service name only: `postgres`, `redis`, `minio`, `neo4j`.
- External GUI tools cannot connect to those services unless you add explicit `ports:` mappings.

Primary entry URL after startup:

- `http://127.0.0.1:8007/api/v1/docs`

## 2. Startup Order and Readiness

The application containers do not start immediately. Both `rag-pipeline` and `rag-worker` wait for:

- `postgres` to become healthy
- `redis` to become healthy
- `neo4j` to become healthy
- `minio-init` to finish successfully
- `nltk-init` to finish successfully

Configured healthchecks:

- Postgres: `pg_isready -U postgres -d rag_api`
- Redis: `redis-cli ping`
- Neo4j: `cypher-shell -u neo4j -p neo4j_password 'RETURN 1;'`

Bootstrap containers:

- `minio-init` loops until MinIO accepts credentials, then creates bucket `rag-artifacts`
- `nltk-init` downloads `punkt` and `punkt_tab` into the shared `/data/nltk_data` volume

MinIO itself has no explicit healthcheck in the compose file. Readiness is handled indirectly by the retry loop in `minio-init`.

## 3. Application Runtime Behavior Inside Compose

The compose file forces container-safe connection settings through the `x-app-env` anchor:

- `DATABASE_URL=postgresql+psycopg://postgres:postgres@postgres:5432/rag_api`
- `REDIS_URL=redis://redis:6379/0`
- `CELERY_ALWAYS_EAGER=false`
- `LOCAL_BLOB_ROOT=/data/blobstore`
- `INDEX_STORAGE_ROOT=/data/indexes`
- `NLTK_DATA=/data/nltk_data`
- `BLOB_BACKEND=filesystem`
- `MINIO_ENDPOINT=minio:9000`
- `MINIO_ACCESS_KEY=minioadmin`
- `MINIO_SECRET_KEY=minioadmin`
- `MINIO_SECURE=false`
- `MINIO_BUCKET=rag-artifacts`
- `VECTOR_PATH=/data/indexes/chroma`

What this means operationally:

- API requests enqueue background jobs instead of running synchronously in-process.
- The worker must be up for pipeline jobs, reindex jobs, and MinerU jobs to complete.
- Blob storage defaults to the shared filesystem volume, not MinIO.
- Vector persistence also lives on a shared filesystem volume.

The worker command is:

```text
celery -A app.core.celery_app.celery_app worker --loglevel=INFO --queues rag-jobs,rag-mineru --concurrency 2
```

So one worker container consumes both queues with concurrency `2`.

## 4. Database Schema Initialization

There is no separate migration container in this compose stack.

On API startup, `app.main` calls `create_all()`, which:

1. creates missing SQLAlchemy tables
2. validates a few schema expectations for existing databases

This is convenient for clean local starts, but it has an important limitation:

- if your Postgres volume contains an old incompatible schema, the API can fail during startup with a schema validation error

In that case, the usual local fix is to recreate the Postgres volume rather than trying to run an unsupported in-place migration.

Typical reset:

```bash
docker compose down -v
docker compose up -d --build
```

That is destructive for all persisted local data, not only Postgres.

## 5. Build and Image Behavior

`rag-pipeline`, `rag-worker`, and `nltk-init` all reuse the same image:

- image name: `rag-pipeline-app:dev`
- build context: repository root
- Dockerfile: `Dockerfile`

The image build performs these steps:

1. starts from `python:3.13-slim`
2. installs Linux packages required by Playwright Chromium
3. installs `uv`
4. copies `pyproject.toml`, `uv.lock`, and `.python-version`
5. runs `uv sync --frozen --no-dev --no-install-project`
6. installs `psycopg[binary]`
7. runs `python -m playwright install chromium`
8. copies the repo into `/app`

Practical implications:

- Python dependency installation is cached until dependency files change.
- Editing application code usually rebuilds quickly because the dependency layer is reused.
- Editing `pyproject.toml`, `uv.lock`, or `.python-version` forces a dependency rebuild.
- Playwright Chromium is installed at image-build time, not container start time.

Standard rebuild command:

```bash
docker compose up -d --build
```

## 6. Environment Files and Precedence

Both application containers use:

- `env_file: .env`
- explicit `environment:` values from the compose file

Precedence matters:

- values in the compose `environment:` block override values from `.env`

This is the most common source of confusion. For example, changing `DATABASE_URL` only in `.env` does not affect the running containers, because compose already injects a different explicit value.

Variables that are effectively fixed by `docker-compose.yml` unless you edit the compose file:

- `DATABASE_URL`
- `REDIS_URL`
- `CELERY_ALWAYS_EAGER`
- `LOCAL_BLOB_ROOT`
- `INDEX_STORAGE_ROOT`
- `NLTK_DATA`
- `BLOB_BACKEND`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_SECURE`
- `MINIO_BUCKET`
- `VECTOR_PATH`

Variables that still matter in `.env` for the default stack:

- `OPENAI_API_KEY`
- `RAG_API_TOKEN`
- provider-specific API keys
- optional proxy variables like `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`

`RAG_API_TOKEN` is mainly relevant for local tooling such as `scripts/run_pipeline_examples.py`, which always sends a bearer token even though the current API does not enforce auth.

## 7. Storage and Persistence

Named volumes created by the stack:

| Volume | Used by | Purpose |
| --- | --- | --- |
| `postgres_data` | `postgres` | relational data |
| `redis_data` | `redis` | Redis append-only state |
| `minio_data` | `minio` | MinIO object data |
| `neo4j_data` | `neo4j` | graph data |
| `rag_blob_data` | `rag-pipeline`, `rag-worker` | filesystem blob storage and uploaded payloads |
| `rag_index_data` | `rag-pipeline`, `rag-worker` | persisted indexes, vector state, docstores |
| `rag_nltk_data` | `rag-pipeline`, `rag-worker`, `nltk-init` | NLTK corpora |

Important details:

- API and worker share the same blob and index volumes, so worker-executed jobs produce artifacts that the API can later read.
- `BLOB_BACKEND=filesystem` means persisted artifact URIs are filesystem-backed by default.
- MinIO is available but idle unless you intentionally switch `BLOB_BACKEND=minio` in compose.
- `VECTOR_PATH=/data/indexes/chroma` also lives inside the shared index volume.

## 8. Standard Operations

### 8.1 First-time setup

```bash
cp .env.template .env
```

Fill at least:

- `OPENAI_API_KEY`
- `RAG_API_TOKEN`

### 8.2 Start the full stack

```bash
docker compose up -d --build
```

### 8.3 Check status

```bash
docker compose ps
```

### 8.4 Follow logs

```bash
docker compose logs -f rag-pipeline
docker compose logs -f rag-worker
docker compose logs -f postgres
docker compose logs -f redis
docker compose logs -f neo4j
```

### 8.5 Restart only the API or worker

```bash
docker compose restart rag-pipeline
docker compose restart rag-worker
```

### 8.6 Stop the stack without deleting data

```bash
docker compose down
```

### 8.7 Reset all persisted data

Destructive:

```bash
docker compose down -v
```

This removes all named volumes, including:

- database state
- Redis state
- Neo4j state
- MinIO data
- blob artifacts
- persisted indexes
- cached NLTK data

## 9. Inspection and Debugging

Useful interactive commands:

Postgres:

```bash
docker compose exec postgres psql -U postgres -d rag_api
```

Redis:

```bash
docker compose exec redis redis-cli ping
```

Neo4j:

```bash
docker compose exec neo4j cypher-shell -u neo4j -p neo4j_password "RETURN 1;"
```

API shell:

```bash
docker compose exec rag-pipeline /bin/sh
```

Worker shell:

```bash
docker compose exec rag-worker /bin/sh
```

To inspect effective environment inside the API container:

```bash
docker compose exec rag-pipeline env
```

If you need direct host access for Postgres, Redis, Neo4j, or MinIO, add explicit port mappings in `docker-compose.yml`; they are intentionally internal-only in the current file.

## 10. Common Issues

### 10.1 API is up but jobs never complete

Check:

- `rag-worker` is running
- `redis` is healthy
- worker logs show queue consumption

Useful commands:

```bash
docker compose logs -f rag-worker
docker compose logs -f redis
```

Remember that compose forces `CELERY_ALWAYS_EAGER=false`, so there is no in-process fallback execution in the API container.

### 10.2 API fails during startup with a schema error

Cause:

- the Postgres volume contains an older incompatible schema

Expected local fix:

```bash
docker compose down -v
docker compose up -d --build
```

### 10.3 You expected MinIO artifacts but only filesystem artifacts appear

Cause:

- compose sets `BLOB_BACKEND=filesystem`

MinIO is provisioned, but it is not the active blob backend unless you edit the compose environment block.

### 10.4 Graph flows fail

Check:

- `neo4j` is healthy
- graph settings point at `bolt://neo4j:7687`
- credentials match the compose defaults

Compose defaults:

- username: `neo4j`
- password: `neo4j_password`
- database: `neo4j`

### 10.5 Playwright-backed web loading fails in containers

The image already installs Chromium and the required Linux packages during build. When this still fails, the usual next steps are:

- rebuild the image after dependency changes
- inspect API or worker logs for Playwright or loader-specific errors

### 10.6 OpenAI-backed indexing or retrieval fails immediately

Check `.env`:

- `OPENAI_API_KEY` must be present in the containers

The compose file does not override that variable, so `.env` remains the source of truth for it.

## 11. Production Caveats

This compose file is useful for development and shared testing, but it is not a production deployment as-is.

Current development-oriented traits:

- default passwords are weak and committed in the compose file
- no TLS between services
- internal services are not exposed or fronted by managed networking
- API auth is not enforced by default
- one worker process with fixed concurrency `2`
- no formal migration service
- local named volumes instead of managed backups and retention

If you use this stack beyond local development, treat it as a baseline to harden rather than a finished deployment blueprint.
