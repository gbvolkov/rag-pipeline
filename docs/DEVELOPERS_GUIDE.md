# Pipeline Developer's Guide

This guide documents how pipelines work in `rag-pipeline` today.

It covers two different manifest surfaces:

1. The production pipeline definition accepted by `POST /api/v1/projects/{project_id}/pipelines`.
2. The example-runner manifest stored in `examples/pipeline_examples/manifest.v1.yaml`.

Those two schemas are related, but they are not the same thing:

- The production pipeline definition is the contract persisted in the database and executed by the API/worker.
- The example manifest is a test harness format that creates projects, creates pipelines, submits runs, and optionally executes retrieval checks against the API.

For the live component inventory of the currently installed `rag-lib`, use `GET /api/v1/capabilities`. This guide explains the repository contract and the manifest shapes enforced by this repo; the capabilities endpoint remains the runtime authority for which loader/splitter/processor/retriever names are actually importable.

## 1. Architecture Overview

At a high level the system is split into five layers:

1. FastAPI contract layer
   - Validates project, pipeline, run, artifact, and retriever requests.
   - Persists immutable pipeline definitions and job records.
2. Pipeline validation layer
   - Enforces structural rules on pipeline definitions.
   - Performs advisory capability checks against the installed `rag-lib`.
3. Worker orchestration layer
   - Resolves runtime inputs and upstream pipeline inputs.
   - Executes loader -> stages -> indexing.
   - Persists artifacts plus lineage edges.
4. `rag-lib` adapter layer
   - Materializes runtime object specs.
   - Instantiates loaders, splitters, processors, indexes, and retrievers.
   - Normalizes `rag-lib` outputs into JSON-safe payloads.
5. Example harness layer
   - Loads `examples/pipeline_examples/manifest.v1.yaml`.
   - Creates test projects/pipelines through the API.
   - Polls jobs and exports snapshots for parity and debugging.

The main design rule is strict orchestration with no hidden API-side fallback behavior. Unknown component names may be accepted with advisory warnings at create time, but execution still fails fast if the runtime component cannot be resolved.

## 2. Entry Points

### 2.1 Process entry points

These are the top-level runtime entry points for the system:

| Surface | Entry point | Purpose |
| --- | --- | --- |
| FastAPI app | `app.main:create_app` | Builds the FastAPI app, installs exception handlers, and mounts routes. |
| ASGI app object | `app.main:app` | The object used by `uvicorn`. |
| Celery app | `app.core.celery_app:celery_app` | Declares broker/backend config and registers worker tasks. |
| Worker task | `app.workers.tasks.run_pipeline_job_task` | Executes a pipeline run job. |
| Worker task | `app.workers.tasks.run_reindex_job_task` | Executes a reindex job. |
| Worker task | `app.workers.tasks.run_mineru_job_task` | Executes a MinerU pipeline run on the dedicated queue. |
| Orchestration function | `app.services.jobs.run_pipeline_job` | Main pipeline execution state machine. |
| Orchestration function | `app.services.jobs.run_reindex_job` | Rebuilds an index from stored segment artifacts. |
| Example runner CLI | `scripts/run_pipeline_examples.py` | Runs the example manifest against a remote API. |
| Example manifest generator | `scripts/generate_pipeline_example_manifest.py` | Regenerates `examples/pipeline_examples/manifest.v1.yaml`. |
| Parity drift gate | `scripts/check_parity_drift.py` | Checks example profile catalog drift. |
| Example conformance runner | `scripts/run_example_conformance.py` | Runs example-profile conformance checks. |

Typical local startup commands:

```bash
uv run uvicorn app.main:app --reload
celery -A app.core.celery_app worker --loglevel=INFO
uv run python scripts/run_pipeline_examples.py --base-url http://127.0.0.1:8007/api/v1 --run-all
```

### 2.2 HTTP API entry points

The pipeline-related HTTP surface is in `app/api/routes.py`.

Projects:

- `POST /api/v1/projects`
- `GET /api/v1/projects`
- `GET /api/v1/projects/{project_id}`
- `PATCH /api/v1/projects/{project_id}`
- `POST /api/v1/projects/{project_id}/archive`

Pipelines:

- `POST /api/v1/projects/{project_id}/pipelines`
- `POST /api/v1/projects/{project_id}/pipelines/validate`
- `GET /api/v1/projects/{project_id}/pipelines`
- `GET /api/v1/projects/{project_id}/pipelines/{pipeline_id}`
- `POST /api/v1/projects/{project_id}/pipelines/{pipeline_id}/copy`
- `DELETE /api/v1/projects/{project_id}/pipelines/{pipeline_id}`
- `POST /api/v1/projects/{project_id}/pipelines/{pipeline_id}/runs`
- `GET /api/v1/projects/{project_id}/pipelines/{pipeline_id}/runs`

Jobs:

- `GET /api/v1/jobs/{job_id}`
- `POST /api/v1/jobs/{job_id}/cancel`
- `POST /api/v1/projects/{project_id}/reindex`

Artifacts and lineage:

- `GET /api/v1/projects/{project_id}/pipelines/{pipeline_id}/documents`
- `GET /api/v1/projects/{project_id}/documents/{artifact_id}`
- `GET /api/v1/projects/{project_id}/pipelines/{pipeline_id}/segments/{stage_name}`
- `GET /api/v1/projects/{project_id}/segments/{artifact_id}`
- `GET /api/v1/projects/{project_id}/pipelines/{pipeline_id}/graph-entities/{stage_name}`
- `GET /api/v1/projects/{project_id}/graph-entities/{artifact_id}`
- `GET /api/v1/projects/{project_id}/indices`
- `GET /api/v1/projects/{project_id}/indices/{artifact_id}`
- `GET /api/v1/projects/{project_id}/artifacts/{artifact_id}/lineage`
- `GET /api/v1/projects/{project_id}/artifacts/{artifact_id}/dependents`
- `GET /api/v1/projects/{project_id}/artifacts/{artifact_id}/lineage/versions`

Retrievers:

- `POST /api/v1/projects/{project_id}/retrievers`
- `GET /api/v1/projects/{project_id}/retrievers`
- `GET /api/v1/projects/{project_id}/retrievers/{retriever_id}`
- `DELETE /api/v1/projects/{project_id}/retrievers/{retriever_id}`
- `POST /api/v1/projects/{project_id}/retrievers/{retriever_id}/init`
- `POST /api/v1/projects/{project_id}/retrievers/{retriever_id}/query`
- `POST /api/v1/projects/{project_id}/retrievers/{retriever_id}/release`
- `GET /api/v1/projects/{project_id}/retrievers/{retriever_id}/results`
- `GET /api/v1/projects/{project_id}/retrieval-results/{retrieval_result_id}`

Capabilities:

- `GET /api/v1/capabilities`

### 2.3 Manifest authoring entry points

If you are authoring or debugging manifests, these are the most important internal entry points:

- `app.schemas.pipelines.PipelineCreate`
  - Canonical production pipeline manifest schema.
- `app.services.pipeline_validator.validate_pipeline`
  - Structural validation and shape classification.
- `app.services.pipeline_advisory_validator.validate_pipeline_advisory`
  - Capability-driven warnings.
- `app.services.runtime_objects.validate_runtime_object_specs`
  - Validation for nested `object_type` specs.
- `scripts.lib.pipeline_example_manifest.load_manifest`
  - Canonical loader for the example manifest.
- `scripts.lib.pipeline_example_runner.PipelineExampleRunner`
  - Canonical executor for example runs.

## 3. Production Pipeline Definition Manifest

### 3.1 Root schema

The production pipeline definition is represented by `app.schemas.pipelines.PipelineCreate`.

Top-level fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | `string` | Yes | Pipeline name, unique per project. |
| `description` | `string \| null` | No | Human-readable description. |
| `loader` | `LoaderConfig \| null` | Conditionally | File-backed or URL-backed loading source. |
| `runtime_input` | `RuntimeInputConfig \| null` | Conditionally | Inline documents or segments supplied at run time. |
| `inputs` | `PipelineInputRef[]` | Conditionally | References to artifacts produced by other pipelines. |
| `stages` | `PipelineStageConfig[]` | No | Splitter and processor stages. |
| `indexing` | `IndexingConfig \| null` | No | Index build configuration. |
| `metadata` | `object` | No | Extra metadata stored in the pipeline definition only. |

Exactly one source model must be used:

- `loader`
- `runtime_input`
- non-empty `inputs`

The API rejects:

- manifests with none of those sources
- manifests with more than one source model at once

### 3.2 Source model: `loader`

`loader` shape:

```json
{
  "type": "TextLoader",
  "params": {}
}
```

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `type` | `string` | Yes | Loader class name resolved from `rag_lib.loaders`. |
| `params` | `object` | No | Loader constructor arguments. |

Runtime behavior:

- For file-backed loaders, the run submission must upload a file.
- For `WebLoader` and `AsyncWebLoader`, the run submission must include `url`.
- The adapter forces Playwright-backed loaders into headless mode even if the manifest requests visible mode.

The production run endpoint rejects inline file payload shortcuts:

- `file_content_b64`
- `text`

Those are intentionally unsupported. Use a real file upload or `runtime_input`.

### 3.3 Source model: `runtime_input`

`runtime_input` shape:

```json
{
  "alias": "RUNTIME_INPUT",
  "artifact_kind": "segment"
}
```

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `alias` | `string` | No | Stage alias under which the runtime artifacts are persisted. Defaults to `RUNTIME_INPUT`. |
| `artifact_kind` | `"document" \| "segment"` | Yes | What the run payload must provide. |

Run payload requirements:

- If `artifact_kind = "document"`, the run payload must include a non-empty `documents` array.
- If `artifact_kind = "segment"`, the run payload must include a non-empty `segments` array.

The runtime input payload is persisted as normal artifacts before downstream stages run:

- documents become `artifact_kind = "document"`
- segments become `artifact_kind = "segment"`
- `stage_name` is the configured alias

### 3.4 Source model: `inputs`

`inputs` is a list of `PipelineInputRef` objects:

```json
{
  "alias": "source_chunks",
  "source_pipeline_id": "pipeline-id",
  "source_stage_name": "chunks",
  "artifact_kind": "segment",
  "pinned_version": 1
}
```

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `alias` | `string` | Yes | Alias by which later stages reference this input. |
| `source_pipeline_id` | `string` | Yes | Upstream pipeline ID. |
| `source_stage_name` | `string` | Yes | Upstream stage name. |
| `artifact_kind` | `"document" \| "segment" \| "index"` | Yes | Requested upstream artifact kind. |
| `pinned_version` | `integer \| null` | No | Explicit version number; when omitted the latest version per `artifact_key` is used. |

Validation rules:

- input aliases must be unique
- all input refs must use the same `artifact_kind`
- if alias `__merged__` is used, it must be the only input alias
- different entries with the same alias cannot specify different pinned versions

Important runtime limitation:

- The schema accepts `artifact_kind = "index"`.
- The current executor only materializes input refs for `document` and `segment`.
- If an input ref uses `index`, pipeline execution fails with an unsupported input-kind error.

In other words, `index` is schema-valid but not currently executable as a pipeline input source.

### 3.5 Stage schema

Each entry in `stages` is a `PipelineStageConfig`:

```json
{
  "stage_name": "chunks",
  "stage_kind": "splitter",
  "component_type": "RecursiveCharacterTextSplitter",
  "params": {
    "chunk_size": 800,
    "chunk_overlap": 100
  },
  "input_aliases": ["LOADING"],
  "position": 0
}
```

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stage_name` | `string` | Yes | Unique artifact-producing stage name. |
| `stage_kind` | `"splitter" \| "processor"` | Yes | Which adapter path to execute. |
| `component_type` | `string` | Yes | Splitter or processor class name. |
| `params` | `object` | No | Constructor args. May include nested runtime object specs. |
| `input_aliases` | `string[]` | No | Upstream aliases to read from. |
| `position` | `integer >= 0` | Yes | Sort key used by execution. |

Validation and runtime rules:

- stage names must be unique
- stage names cannot collide with existing source aliases
- any explicit `input_aliases` must reference known aliases
- stages are sorted by `position`, not by file order
- an empty `input_aliases` list is allowed; at runtime it defaults to the immediately previous stage/source alias
- a stage cannot mix document and segment inputs
- only `splitter` and `processor` are valid `stage_kind` values

Result contracts:

- splitters may return only segment payloads
- processors may return document payloads, segment payloads, or `kind = "none"`
- a processor returning `kind = "none"` may still ask the executor to persist extra artifacts, currently `graph_entity`

### 3.6 Indexing schema

`indexing` shape:

```json
{
  "index_type": "chroma",
  "params": {
    "embeddings": {
      "object_type": "create_embeddings_model",
      "provider": "openai",
      "model_name": "text-embedding-3-small"
    },
    "cleanup": true,
    "dual_storage": true
  },
  "collection_name": "customer_faq",
  "docstore_name": "customer_faq_docstore"
}
```

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `index_type` | `string` | Yes | Vector store provider name used by `rag_lib.vectors.factory.create_vector_store`. |
| `params` | `object` | No | Index/runtime build parameters. |
| `collection_name` | `string \| null` | No | Logical collection name stored in the index artifact metadata. |
| `docstore_name` | `string \| null` | No | Logical doc store name stored in the index artifact metadata. |

Important indexing rules:

- `indexing.params.embeddings` is mandatory at runtime; legacy `embeddings_provider` and `embeddings_model_name` keys are rejected
- managed Chroma indexes reject physical storage keys in `params`:
  - `collection_name`
  - `doc_store_path`
- physical storage paths are generated by the API and persisted into the index artifact's `storage` descriptor
- when `dual_storage = true`, parent segments are resolved and persisted into a local pickle doc store for hydration-aware retrievers

### 3.7 Metadata

`metadata` is stored in the pipeline definition JSON and is not interpreted by the executor. It is safe for custom labeling, ownership tags, or documentation hints, but it does not affect loading, stage execution, or indexing.

## 4. Production Manifest Validation and Shape Classification

### 4.1 Strict validation

`validate_pipeline` performs hard validation and rejects the request on any structural error.

The most important hard checks are:

- exactly one source model must be defined
- stage names must be unique
- input aliases must be resolvable
- runtime object specs must use explicit `object_type`
- legacy runtime spec keys are forbidden:
  - `factory`
  - `__runtime_factory__`
  - `plugin_ref`
- `RegexHierarchySplitter.params.patterns` must use object entries like `{ "level": 1, "pattern": "^...$" }`
- managed Chroma physical storage params are forbidden

### 4.2 Advisory validation

`validate_pipeline_advisory` does not block pipeline creation. It emits warnings based on `GET /api/v1/capabilities` style discovery:

- unknown loader/splitter/processor type
- unknown params for a discovered component
- missing required params for a discovered component
- unknown index type in advisory metadata
- capability discovery failures

This means a pipeline can be created even if it references a component that is missing from the installed `rag-lib`. In that case:

- pipeline creation succeeds
- `validation_warnings` is populated
- the later run fails when the worker cannot resolve or execute the component

### 4.3 Shape strings

The executor stores a derived `shape` string on each pipeline.

Current shape values:

| Shape | Meaning |
| --- | --- |
| `full` | loader + stages + indexing |
| `loading_stages` | loader + stages |
| `loading_only` | loader only |
| `runtime_input_stages_indexing` | runtime input + stages + indexing |
| `runtime_input_stages_only` | runtime input + stages |
| `input_stages_indexing` | pipeline inputs + stages + indexing |
| `input_stages_only` | pipeline inputs + stages |
| `indexing_only` | runtime input or pipeline inputs + indexing, but no stages |
| `custom` | any remaining valid combination |

## 5. Manifest Runtime Objects, Types, and Enums

### 5.1 Runtime object spec contract

Any manifest params object may contain nested runtime object specs.

Generic shape:

```json
{
  "object_type": "create_llm",
  "provider": "openai",
  "model_name": "gpt-4.1-nano",
  "temperature": 0,
  "streaming": false
}
```

Rules:

- runtime object specs can appear anywhere inside `loader.params`, `stage.params`, `indexing.params`, retriever params, and project graph-store config adapters
- nested runtime object specs are materialized recursively
- validation only accepts known `object_type` values

### 5.2 Runtime `object_type` values accepted by this repo

The validator currently recognizes these runtime object types:

| `object_type` | Meaning |
| --- | --- |
| `create_llm` | Build an LLM instance from `rag_lib.llm.factory.create_llm`. |
| `create_embeddings_model` | Build an embeddings model from `rag_lib.embeddings.factory.create_embeddings_model`. |
| `create_graph_store` | Build a graph store from `rag_lib.graph.store.create_graph_store`. |
| `NetworkXGraphStore` | Instantiate the in-memory graph store class directly. |
| `GraphQueryConfig` | Instantiate `rag_lib.retrieval.graph_retriever.GraphQueryConfig`. |
| `LLMTableSummarizer` | Instantiate `rag_lib.summarizers.table_llm.LLMTableSummarizer`. |
| `WebCleanupConfig` | Instantiate web cleanup/navigation rules. |
| `PlaywrightNavigationConfig` | Instantiate Playwright navigation-state rules. |
| `PlaywrightExtractionConfig` | Instantiate Playwright extraction profile chains. |
| `PlaywrightProfileConfig` | Instantiate one Playwright extraction profile. |

### 5.3 Component type families used in manifests

Static manifests in this repo currently exercise these component names.

Loaders used by the example manifest:

- `TextLoader`
- `PyMuPDFLoader`
- `DocXLoader`
- `CSVLoader`
- `ExcelLoader`
- `JsonLoader`
- `RegexHierarchyLoader`
- `MinerULoader`
- `PPTXLoader`
- `HTMLLoader`
- `WebLoader`
- `AsyncWebLoader`

Splitters/chunkers used by the example manifest:

- `RegexSplitter`
- `TokenTextSplitter`
- `RecursiveCharacterTextSplitter`
- `RegexHierarchySplitter`
- `SemanticChunker`
- `SentenceSplitter`
- `CSVTableSplitter`
- `MarkdownTableSplitter`
- `JsonSplitter`
- `QASplitter`
- `HTMLSplitter`

Processors used by the example manifest:

- `SegmentEnricher`
- `RaptorProcessor`
- `EntityExtractor`

Retrievers used by the example manifest:

- `FuzzyRetriever`
- `RegexRetriever`
- `GraphRetriever`
- `create_vector_retriever`
- `create_bm25_retriever`
- `create_ensemble_retriever`
- `create_reranking_retriever`
- `create_scored_dual_storage_retriever`

These lists are not the full theoretical `rag-lib` surface. They are the names actively exercised by the repository's manifests and tests. The complete runtime inventory for the installed library should be read from `GET /api/v1/capabilities`.

### 5.4 Nested retriever specs and reserved names

Retriever params may recursively embed other retrievers. This matters for composition factories such as `create_ensemble_retriever`.

Nested retriever spec shape:

```json
{
  "retriever_type": "create_vector_retriever",
  "params": {
    "top_k": 3
  }
}
```

Reserved names with operational meaning:

- `LOADING`: reserved alias for loader output
- `RUNTIME_INPUT`: default alias for runtime-input sources
- `INPUT_RESOLUTION`: job-stage marker used while resolving pipeline inputs
- `SEGMENTING`: job-stage marker used while running splitters/processors
- `INDEXING`: job-stage marker used while building an index
- `RETRIEVAL_RESULT`: synthetic stage name used for persisted retrieval-hit artifacts
- `__merged__`: reserved pipeline-input alias that must stand alone if used

### 5.5 Manifest-relevant literal enums

Core pipeline literals enforced directly by this repo:

| Field | Allowed values |
| --- | --- |
| `runtime_input.artifact_kind` | `document`, `segment` |
| `inputs[].artifact_kind` | `document`, `segment`, `index` |
| `stages[].stage_kind` | `splitter`, `processor` |

Example-manifest literals enforced directly by this repo:

| Field | Allowed values |
| --- | --- |
| `input_mode` | `file`, `url`, `documents`, `segments` |
| `expected_outcome` | `success`, `error` |
| `retrieval.source.kind` | `index`, `stage` |

Web loader and Playwright literals referenced by pipeline params:

| Field | Allowed values |
| --- | --- |
| `SchemaDialect` | `dot_path` |
| `fetch_mode` | `requests`, `requests_fallback_playwright`, `playwright` |
| `crawl_scope` | `same_host`, `same_domain`, `allowed_domains`, `allow_all` |
| `output_format` | `markdown`, `html` |
| `PlaywrightProfileConfig.profile` | `anchors`, `attributes`, `onclick_regex`, `eval`, `paginated_eval` |
| `PlaywrightNavigationConfig.navigation_state_document_mode` | `separate_documents`, `single_document` |

Retriever and graph enums referenced by manifest params:

| Field | Allowed values |
| --- | --- |
| `SearchType` | `similarity`, `similarity_score_threshold`, `mmr` |
| `HydrationMode` | `parents_replace`, `children_enriched`, `children_plus_parents` |
| `GraphQueryConfig.mode` | `local`, `global`, `hybrid`, `mix` |
| `GraphQueryConfig.vector_relevance_mode` | `strict_0_1`, `normalize_minmax` |

Factory/provider enums commonly used inside runtime object specs:

| Factory | Common values |
| --- | --- |
| `create_vector_store.provider` / `index_type` | `chroma`, `faiss`, `qdrant`, `postgres` |
| `create_embeddings_model.provider` | `openai`, `local`, `huggingface` |
| `create_llm.provider` | `openai`, `openai_think`, `openai_4`, `openai_pers`, `mistral`, `yandex` |
| `MinerULoader.parse_mode` | `auto`, `txt`, `ocr` |
| `MinerULoader.backend` | `pipeline`, `hybrid-auto-engine`, `hybrid-http-client`, `vlm-auto-engine`, `vlm-http-client` |
| `MinerULoader.source` | `huggingface`, `modelscope`, `local` |

## 6. Production Pipeline Processing

### 6.1 Pipeline creation

When a pipeline is created:

1. The project must be active.
2. The payload is validated as `PipelineCreate`.
3. `validate_pipeline` performs hard checks.
4. `validate_pipeline_advisory` generates warnings.
5. The immutable pipeline definition is stored in `pipelines.definition`.
6. `inputs` are expanded into `pipeline_inputs`.
7. `indexing` is expanded into `pipeline_indexing_config`.

Important properties:

- pipeline definitions are immutable once created
- deletion is soft (`deleted = true`)
- copying a pipeline duplicates the stored definition, input refs, and indexing config

### 6.2 Run submission

The run endpoint accepts:

- JSON requests
- multipart form uploads with:
  - `form_payload`
  - `file`

Submission behavior:

- uploaded files are stored temporarily under `local_blob_root/_uploads`
- the temp file path is injected into the job payload as `uploaded_file_path`
- the temp file is cleaned up in a `finally` block after execution
- `example_profile_id` is explicitly rejected by the production API

### 6.3 Job lifecycle

Every run becomes a `Job` row plus `JobEvent` history.

Status values observed in practice:

- `queued`
- `running`
- `succeeded`
- `failed`
- `canceled`

Stage values used by orchestration:

- `INPUT_RESOLUTION`
- `LOADING`
- `SEGMENTING`
- `INDEXING`

The worker records transitions and emits job events on every state change.

### 6.4 Input resolution

The worker resolves three possible input sources in this order:

1. `inputs` from other pipelines
2. `runtime_input`
3. `loader`

Pipeline inputs:

- If `pinned_version` is provided, the exact version is loaded.
- Otherwise the latest version per `artifact_key` is selected.
- Resolved upstream artifacts are converted back into document or segment payloads.

Runtime input:

- The worker validates `documents` or `segments` in the run payload.
- The inline payload is persisted as normal artifacts immediately.

Loader:

- The worker calls `run_loader`.
- The adapter normalizes file/url arguments.
- Web loaders are forced headless.
- Loader diagnostics such as `last_stats` and `last_errors` are copied into `job.result.stage_diagnostics`.

### 6.5 Stage execution

Stages are executed in `position` order.

For each stage:

1. Resolve the effective `input_aliases`.
2. Gather upstream payloads plus upstream artifact IDs.
3. Merge `runtime_extras` from upstream outputs into the stage runtime context.
4. Execute either `run_splitter` or `run_processor`.
5. Persist the returned artifacts.
6. Store any diagnostics.

The runtime context may auto-inject these objects into downstream constructors if the target signature supports them:

- `vector_store`
- `doc_store`
- `documents`
- `segments`

No hidden injection happens for:

- `llm`
- `embeddings`
- `graph_store`
- `store`

The only special case is `GraphRetriever`, which may receive a graph store from:

1. index runtime extras
2. project-level `graph_store_config`

### 6.6 Processor `kind = "none"` and graph entities

Processors may return:

- document payloads
- segment payloads
- `kind = "none"`

`kind = "none"` is not a no-op. It means:

- the processor did not return a standard document/segment list
- it may still return `persisted_artifacts`
- the executor currently supports persisted `graph_entity` artifacts in this path

### 6.7 Index selection and build

If the pipeline defines `indexing`, the worker builds exactly one index artifact.

Index source selection:

- first choice: the last stage in reverse order whose output kind is `segment`
- fallback: flatten all segment outputs currently held in memory

Index build behavior:

1. Validate indexing params.
2. Resolve parent segments if `dual_storage = true`.
3. Generate a managed storage descriptor.
4. Materialize embeddings and other runtime objects.
5. Instantiate vector store and optional doc store.
6. Call `Indexer.index(...)`.
7. Persist the index artifact with storage metadata and lineage links.

Logical names vs physical names:

- `collection_name` and `docstore_name` in the manifest are logical labels
- physical collection names and storage paths are generated by the API
- those physical descriptors are stored in the index artifact's `storage`

### 6.8 Artifact persistence, versioning, and lineage

Every persisted artifact receives:

- monotonic `version`
- stable `artifact_key` inside a pipeline/stage/kind scope
- `ArtifactInput` edges to all upstream artifacts used to create it

Current persisted `artifact_kind` values:

- `document`
- `segment`
- `graph_entity`
- `index`

Artifacts created during retrieval also use `segment`, with `stage_name = "RETRIEVAL_RESULT"` if the system must synthesize a segment artifact for a returned hit that could not be matched back to an existing source artifact.

### 6.9 Failure behavior

Execution fails fast on:

- unknown or unsupported runtime component names
- empty loader results
- empty splitter/processor document or segment results
- mixed document and segment inputs to one stage
- missing source artifacts
- missing required runtime input arrays

Error mapping:

- validation problems become `422`
- not found becomes `404`
- conflicts become `409`
- adapter/runtime failures become `503`
- unknown exceptions become `500`

## 7. Reindexing and Retriever Processing

### 7.1 Reindex flow

`POST /api/v1/projects/{project_id}/reindex` accepts:

```json
{
  "source_segments": [
    {
      "pipeline_id": "pipeline-id",
      "stage_name": "chunks",
      "version": 1
    }
  ],
  "indexing": {
    "index_type": "chroma",
    "params": {
      "embeddings": {
        "object_type": "create_embeddings_model",
        "provider": "openai",
        "model_name": "text-embedding-3-small"
      }
    }
  }
}
```

Behavior:

- only segment artifacts are loaded
- parent-segment hydration is resolved when `dual_storage = true`
- the new index artifact is project-scoped and not tied to a pipeline

### 7.2 Retriever creation

A retriever can be created from exactly one source:

- `index_artifact_id`
- `source_artifact_ids`

Direct source artifacts must be homogeneous:

- all documents
- or all segments
- or all graph entities

The retriever runtime is cached in memory. If an index-backed retriever is created and the in-memory runtime is missing, the system attempts to restore the index runtime from the index artifact plus its lineage.

### 7.3 Query execution

The API query contract intentionally does not expose `top_k` on the query request body. `top_k` belongs in the retriever's creation params.

Returned score fields are normalized from:

- `score`
- `rerank_score`
- `similarity_score`
- `fuzzy_score`
- `max_similarity_score`

## 8. Example Runner Manifest

### 8.1 Root schema

The example manifest is loaded by `scripts.lib.pipeline_example_manifest.load_manifest`.

Root shape:

```yaml
version: v2
examples:
  - example_id: 01_text_basic
    source_example_file: 01_text_basic.py
    input_mode: file
    input_spec:
      file: terms&defs.txt
    project_create_payload: {}
    runs:
      - run_name: main
        pipeline_create_payload:
          ...
        run_payload_template: {}
        retrievals: []
    expected_outcome: success
    notes: Pipeline-only API example parity with rag-lib.
```

Root fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `version` | `string` | Yes | Manifest format version. Current value is `v2`. |
| `examples` | `PipelineExampleSpec[]` | Yes | Ordered list of runnable examples. |

### 8.2 `PipelineExampleSpec`

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `example_id` | `string` | Yes | Unique manifest ID. |
| `source_example_file` | `string` | Yes | Source example file name from the upstream example corpus. |
| `input_mode` | `file \| url \| documents \| segments` | Yes | How runtime input is provided. |
| `input_spec` | `object` | Yes | Input-mode-specific payload. |
| `project_create_payload` | `object` | No | Optional project payload, for example graph store config. |
| `runs` | `PipelineRunSpec[]` | Yes | One or more pipeline variants for the example. |
| `expected_outcome` | `success \| error` | Yes | Whether the overall example should pass or intentionally fail. |
| `notes` | `string` | Yes | Free-form notes. |

Validation rules:

- `example_id` must be unique
- `runs` must be present and non-empty
- `input_mode` must be one of the four supported literals
- `expected_outcome` must be `success` or `error`

### 8.3 `input_spec` by `input_mode`

`input_mode = file`

```yaml
input_mode: file
input_spec:
  file: statement.pdf
```

Behavior:

- path is resolved relative to `--example-docs-root`
- escaping that root is rejected
- the runner injects `upload_file_path` and `file_name` into the run payload

`input_mode = url`

```yaml
input_mode: url
input_spec:
  url: https://example.com
```

Behavior:

- the runner injects `url` into the run payload

`input_mode = documents`

```yaml
input_mode: documents
input_spec:
  documents:
    - content: Hello
      metadata: {}
```

Behavior:

- records are deep-copied into the run payload under `documents`

`input_mode = segments`

```yaml
input_mode: segments
input_spec:
  segments:
    - content: Hello
      metadata: {}
      segment_id: seg-1
```

Behavior:

- records are deep-copied into the run payload under `segments`

### 8.4 `project_create_payload`

This is passed directly to `POST /projects`.

The main current advanced use is project-level graph store configuration:

```yaml
project_create_payload:
  graph_store_config:
    provider: neo4j
    params:
      uri: bolt://neo4j:7687
      username: neo4j
      password: neo4j_password
      database: neo4j
```

### 8.5 `PipelineRunSpec`

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `run_name` | `string` | Yes | Variant name, used in snapshot filenames and result grouping. |
| `pipeline_create_payload` | `object` | Yes | Production pipeline definition payload. |
| `run_payload_template` | `object` | No | Extra request payload merged before input injection. |
| `retrievals` | `RetrievalPlan[]` | No | Retrieval checks to run after a successful pipeline run. |

Important detail:

- `pipeline_create_payload` is the production API manifest described in Sections 3 through 7.
- The example manifest does not define a second pipeline schema; it embeds the production one.

### 8.6 `RetrievalPlan`

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | `string` | Yes | Retrieval scenario name. |
| `source.kind` | `index \| stage` | Yes | Whether the retriever is created from the run's first index artifact or from artifacts produced by one named stage. |
| `source.stage_name` | `string` | Required when `source.kind = stage` | Stage to use as retriever source. |
| `create.retriever_type` | `string` | Yes | Retriever or retriever-factory name. |
| `create.params` | `object` | No | Retriever creation params. |
| `requires_session` | `boolean` | No | Whether the runner must call `init` and `release` around queries. |
| `queries` | `RetrievalQueryPlan[]` | Yes | Queries to execute against the created retriever. |

### 8.7 `RetrievalQueryPlan`

Fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | `string` | Yes | Snapshot-scoping name. |
| `query` | `string` | Yes | Query text sent to the API. |
| `top_k` | `integer` | Yes | Declared expectation/documentation field in the manifest. |
| `strict_match` | `boolean` | Yes | Whether missing artifacts or query failures should fail the example. |

Important detail:

- `top_k` is currently not sent to `POST /retrievers/{id}/query`.
- The runner sends only `query` and optional `session_id`.
- Effective top-k behavior must therefore be set in `create.params`, not here.

### 8.8 Example IDs currently defined

Current manifest examples:

- `01_text_basic`
- `02_markdown_enrichment`
- `02_markdown_enrichment_vector`
- `03_pdf_semantic`
- `04_pdf_raptor`
- `05_docx_graph`
- `06_docx_regex`
- `07_csv_table_summary`
- `07_md_table_summary`
- `08_excel_csv_basic`
- `08_excel_md_basic`
- `09_json_hybrid`
- `10_text_ensemble`
- `11_log_regex_loader`
- `12_qa_loader`
- `13_dual_storage`
- `14_mineru_pdf`
- `15_pptx_unsupported`
- `16_html_html`
- `16_html_md`
- `17A_web_loader_plantpad`
- `17B_web_loader_quotes`
- `17C_web_loader_example`
- `17_web_loader`

## 9. Example Manifest Processing

### 9.1 Selection flow

`scripts/run_pipeline_examples.py` uses this precedence:

1. `--run-all`
2. `--examples`
3. default interactive selection

### 9.2 Per-example execution flow

For each example:

1. Create the project from `project_create_payload`.
2. For each `run`:
   - create the pipeline from `pipeline_create_payload`
   - build the run payload from `run_payload_template + input_mode/input_spec`
   - submit the run
   - poll `GET /jobs/{job_id}` until terminal state
   - export documents, stage artifacts, graph entities, and indices
   - execute any retrieval plans
3. Write per-example `summary.json`.

### 9.3 Snapshot layout

Results are exported under:

```text
results/pipeline_examples/<timestamp>/
```

Per-example output includes:

- project create request/response
- pipeline create request/response
- run submit request/response
- final job response
- artifact list/detail payloads
- retriever create/init/query/release payloads
- retrieval result detail payloads
- `summary.json`
- `error.json` when failed

Global output includes:

- `run.summary.json`
- `run.errors.json`

## 10. Practical Authoring Guidance

When authoring production manifests:

1. Start with `POST /api/v1/projects/{pid}/pipelines/validate`.
2. Check `validation_warnings`; do not ignore unknown component warnings.
3. Use `GET /api/v1/capabilities` to confirm component names and parameter signatures.
4. Prefer explicit `input_aliases` even though implicit chaining is supported.
5. For Chroma, never put physical storage keys in `indexing.params`.
6. For dual storage, ensure segments have stable `segment_id` and `parent_id` semantics.

When authoring example manifests:

1. Remember that `pipeline_create_payload` is the production manifest.
2. Keep `input_spec.file` inside `example_docs`.
3. Put effective retrieval fanout in `create.params`, not `queries[].top_k`.
4. Use `expected_outcome: error` only for intentional rejection scenarios.
5. Use separate `run_name` values for sync/async or alternative pipeline variants.

## 11. Current Gotchas and Non-Obvious Rules

- `GET /api/v1/capabilities/examples` is not implemented; only `GET /api/v1/capabilities` exists.
- Unknown component names may create successfully with warnings, then fail during execution.
- `inputs[].artifact_kind = index` is schema-valid but not executable today.
- Web loaders are always forced to headless mode by the API adapter.
- Query-time `top_k` is intentionally not part of the public query contract.
- `example_profile_id` is rejected by the production run API.
- Dual-storage index restoration depends on artifact lineage being present.
- `EntityExtractor` can create persisted `graph_entity` artifacts even when the processor itself returns `kind = "none"`.

## 12. File Map

Core files for pipeline developers:

- `app/main.py`
- `app/api/routes.py`
- `app/schemas/pipelines.py`
- `app/schemas/projects.py`
- `app/schemas/retrievers.py`
- `app/schemas/jobs.py`
- `app/services/pipeline_validator.py`
- `app/services/pipeline_advisory_validator.py`
- `app/services/runtime_objects.py`
- `app/services/jobs.py`
- `app/services/rag_adapter.py`
- `app/services/artifacts.py`
- `app/models/entities.py`
- `scripts/run_pipeline_examples.py`
- `scripts/lib/pipeline_example_manifest.py`
- `scripts/lib/pipeline_example_runner.py`
- `scripts/generate_pipeline_example_manifest.py`
- `examples/pipeline_examples/manifest.v1.yaml`
