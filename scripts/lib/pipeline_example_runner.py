from __future__ import annotations

import copy
import json
import logging
import mimetypes
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

from scripts.lib.pipeline_example_export import SnapshotExporter
from scripts.lib.pipeline_example_manifest import (
    PipelineExampleSpec,
    PipelineRunSpec,
    RetrievalPlan,
    RetrievalQueryPlan,
    build_run_payload,
)


TERMINAL_JOB_STATUSES = {"succeeded", "failed", "canceled"}
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ApiResponse:
    method: str
    url: str
    status_code: int
    headers: dict[str, str]
    json_body: Any | None
    text_body: str

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def snapshot(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "url": self.url,
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.json_body if self.json_body is not None else self.text_body,
        }


class ApiClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: int,
        *,
        get_retry_attempts: int = 5,
        get_retry_backoff_seconds: float = 0.5,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.get_retry_attempts = max(1, int(get_retry_attempts))
        self.get_retry_backoff_seconds = max(0.0, float(get_retry_backoff_seconds))
        self.default_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

    def _build_url(self, path: str, query: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}/{path.lstrip('/')}"
        if query:
            query_string = parse.urlencode({k: v for k, v in query.items() if v is not None}, doseq=True)
            url = f"{url}?{query_string}"
        return url

    def request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        json_payload: dict[str, Any] | None = None,
    ) -> ApiResponse:
        method_upper = method.upper()
        url = self._build_url(path, query=query)
        body_bytes = None
        headers = dict(self.default_headers)
        if json_payload is not None:
            headers["Content-Type"] = "application/json"
            body_bytes = json.dumps(json_payload, ensure_ascii=False).encode("utf-8")

        max_attempts = self.get_retry_attempts if method_upper == "GET" else 1
        attempt = 0
        while True:
            attempt += 1
            req = request.Request(url=url, data=body_bytes, method=method_upper, headers=headers)
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8")
                    parsed = self._parse_json(raw)
                    return ApiResponse(
                        method=method_upper,
                        url=url,
                        status_code=resp.status,
                        headers={k: v for k, v in resp.headers.items()},
                        json_body=parsed,
                        text_body=raw,
                    )
            except HTTPError as exc:
                raw = exc.read().decode("utf-8")
                parsed = self._parse_json(raw)
                return ApiResponse(
                    method=method_upper,
                    url=url,
                    status_code=exc.code,
                    headers={k: v for k, v in exc.headers.items()} if exc.headers else {},
                    json_body=parsed,
                    text_body=raw,
                )
            except URLError as exc:
                reason_type = type(exc.reason).__name__ if getattr(exc, "reason", None) is not None else type(exc).__name__
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"Network error while calling {method_upper} {url}: {reason_type}: {exc}"
                    ) from exc
                delay = self.get_retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Transient request error: method=%s url=%s attempt=%s/%s error=%s retry_in=%.2fs",
                    method_upper,
                    url,
                    attempt,
                    max_attempts,
                    exc,
                    delay,
                )
                if delay > 0:
                    time.sleep(delay)
            except OSError as exc:
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"Socket error while calling {method_upper} {url}: {type(exc).__name__}: {exc}"
                    ) from exc
                delay = self.get_retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Transient socket error: method=%s url=%s attempt=%s/%s error=%s retry_in=%.2fs",
                    method_upper,
                    url,
                    attempt,
                    max_attempts,
                    exc,
                    delay,
                )
                if delay > 0:
                    time.sleep(delay)

    @staticmethod
    def _parse_json(raw: str) -> Any | None:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def get(self, path: str, *, query: dict[str, Any] | None = None) -> ApiResponse:
        return self.request("GET", path, query=query)

    def post(self, path: str, *, json_payload: dict[str, Any] | None = None) -> ApiResponse:
        return self.request("POST", path, json_payload=json_payload)

    def post_multipart(
        self,
        path: str,
        *,
        form_payload: dict[str, Any] | None,
        file_path: str,
        file_name: str,
    ) -> ApiResponse:
        boundary = f"----rag-pipeline-{uuid.uuid4().hex}"
        url = self._build_url(path)
        headers = dict(self.default_headers)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"

        file_bytes = Path(file_path).read_bytes()
        content_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        body = bytearray()

        if form_payload is not None:
            body.extend(f"--{boundary}\r\n".encode("utf-8"))
            body.extend(b'Content-Disposition: form-data; name="form_payload"\r\n')
            body.extend(b"Content-Type: application/json; charset=utf-8\r\n\r\n")
            body.extend(json.dumps(form_payload, ensure_ascii=False).encode("utf-8"))
            body.extend(b"\r\n")

        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="file"; filename="{file_name}"\r\n'.encode("utf-8")
        )
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        body.extend(file_bytes)
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))

        req = request.Request(url=url, data=bytes(body), method="POST", headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                parsed = self._parse_json(raw)
                return ApiResponse(
                    method="POST",
                    url=url,
                    status_code=resp.status,
                    headers={k: v for k, v in resp.headers.items()},
                    json_body=parsed,
                    text_body=raw,
                )
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            parsed = self._parse_json(raw)
            return ApiResponse(
                method="POST",
                url=url,
                status_code=exc.code,
                headers={k: v for k, v in exc.headers.items()} if exc.headers else {},
                json_body=parsed,
                text_body=raw,
            )

    def delete(self, path: str) -> ApiResponse:
        return self.request("DELETE", path)


@dataclass(slots=True)
class RunnerConfig:
    example_docs_root: Path
    poll_interval_seconds: int = 2
    poll_timeout_seconds: int = 1200
    continue_on_error: bool = True


@dataclass(slots=True)
class RunExecutionResult:
    run_name: str
    status: str
    pipeline_id: str | None = None
    job_id: str | None = None
    retrieval_result_ids: list[str] = field(default_factory=list)
    error: dict[str, Any] | None = None
    notes: str | None = None


@dataclass(slots=True)
class ExampleRunResult:
    example_id: str
    status: str
    started_at: str
    finished_at: str
    project_id: str | None = None
    pipeline_id: str | None = None
    job_id: str | None = None
    retrieval_result_id: str | None = None
    runs: list[RunExecutionResult] = field(default_factory=list)
    error: dict[str, Any] | None = None
    notes: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == "passed"


@dataclass(slots=True)
class ArtifactSnapshot:
    documents: list[dict[str, Any]] = field(default_factory=list)
    segments_by_stage: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    indices: list[dict[str, Any]] = field(default_factory=list)


def _slug(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in value)


class PipelineExampleRunner:
    def __init__(
        self,
        api_client: ApiClient,
        exporter: SnapshotExporter,
        config: RunnerConfig,
    ):
        self.api = api_client
        self.exporter = exporter
        self.config = config

    def run_many(self, examples: list[PipelineExampleSpec]) -> list[ExampleRunResult]:
        results: list[ExampleRunResult] = []
        for spec in examples:
            result = self.run_one(spec)
            results.append(result)
            if not result.passed and not self.config.continue_on_error:
                break
        return results

    def run_one(self, spec: PipelineExampleSpec) -> ExampleRunResult:
        started_at = datetime.now(tz=UTC).isoformat()
        project_id: str | None = None
        run_results: list[RunExecutionResult] = []
        logger.info("Starting example run: example_id=%s", spec.example_id)

        project_response = self.api.post("/projects", json_payload={"name": f"example-{spec.example_id}"})
        self._write_example_json(spec.example_id, "project.create.response.json", project_response.snapshot())
        if not project_response.ok:
            if spec.expected_outcome == "error":
                return self._pass_expected_error(
                    spec,
                    started_at,
                    project_id=None,
                    run_results=[],
                    notes="Expected error observed during project creation",
                    error_payload=project_response.snapshot(),
                )
            return self._fail(spec, started_at, "project_create_failed", project_response.snapshot())
        project_id = self._json_dict(project_response).get("id")

        for run_spec in spec.runs:
            run_result = self._run_pipeline_variant(spec, run_spec, project_id=project_id)
            run_results.append(run_result)
            if run_result.status == "failed":
                if spec.expected_outcome == "error":
                    return self._pass_expected_error(
                        spec,
                        started_at,
                        project_id=project_id,
                        run_results=run_results,
                        notes=f"Expected error observed during run '{run_spec.run_name}'",
                        error_payload=run_result.error,
                    )
                finished_at = datetime.now(tz=UTC).isoformat()
                result = ExampleRunResult(
                    example_id=spec.example_id,
                    status="failed",
                    started_at=started_at,
                    finished_at=finished_at,
                    project_id=project_id,
                    pipeline_id=run_result.pipeline_id,
                    job_id=run_result.job_id,
                    retrieval_result_id=run_result.retrieval_result_ids[0] if run_result.retrieval_result_ids else None,
                    runs=run_results,
                    error=run_result.error,
                )
                self._write_example_json(spec.example_id, "summary.json", asdict(result))
                return result

        if spec.expected_outcome == "error":
            return self._fail(
                spec,
                started_at,
                "expected_error_not_observed",
                {"message": "All example runs completed successfully"},
                project_id=project_id,
                run_results=run_results,
            )

        finished_at = datetime.now(tz=UTC).isoformat()
        first_run = run_results[0] if run_results else None
        result = ExampleRunResult(
            example_id=spec.example_id,
            status="passed",
            started_at=started_at,
            finished_at=finished_at,
            project_id=project_id,
            pipeline_id=first_run.pipeline_id if first_run else None,
            job_id=first_run.job_id if first_run else None,
            retrieval_result_id=(
                first_run.retrieval_result_ids[0]
                if first_run and first_run.retrieval_result_ids
                else None
            ),
            runs=run_results,
        )
        self._write_example_json(spec.example_id, "summary.json", asdict(result))
        logger.info("Example run completed: example_id=%s project_id=%s", spec.example_id, project_id)
        return result

    def _run_pipeline_variant(
        self,
        spec: PipelineExampleSpec,
        run_spec: PipelineRunSpec,
        *,
        project_id: str,
    ) -> RunExecutionResult:
        run_scope = run_spec.run_name
        pipeline_payload = copy.deepcopy(run_spec.pipeline_create_payload)
        pipeline_payload["name"] = pipeline_payload.get("name") or f"pipeline-{spec.example_id}-{run_scope}"
        self._write_example_json(spec.example_id, "pipeline.create.request.json", pipeline_payload, run_scope)
        pipeline_response = self.api.post(f"/projects/{project_id}/pipelines", json_payload=pipeline_payload)
        self._write_example_json(spec.example_id, "pipeline.create.response.json", pipeline_response.snapshot(), run_scope)
        if not pipeline_response.ok:
            return RunExecutionResult(
                run_name=run_scope,
                status="failed",
                error={"error_code": "pipeline_create_failed", "payload": pipeline_response.snapshot()},
            )
        pipeline_id = self._json_dict(pipeline_response).get("id")

        run_payload = build_run_payload(spec, run_spec, self.config.example_docs_root)
        self._write_example_json(spec.example_id, "run.submit.request.json", run_payload, run_scope)
        upload_file_path = run_payload.pop("upload_file_path", None)
        if isinstance(upload_file_path, str) and upload_file_path:
            run_submit_response = self.api.post_multipart(
                f"/projects/{project_id}/pipelines/{pipeline_id}/runs",
                form_payload=run_payload,
                file_path=upload_file_path,
                file_name=str(run_payload.get("file_name") or Path(upload_file_path).name),
            )
        else:
            run_submit_response = self.api.post(
                f"/projects/{project_id}/pipelines/{pipeline_id}/runs",
                json_payload=run_payload,
            )
        self._write_example_json(spec.example_id, "run.submit.response.json", run_submit_response.snapshot(), run_scope)
        if not run_submit_response.ok:
            return RunExecutionResult(
                run_name=run_scope,
                status="failed",
                pipeline_id=pipeline_id,
                error={"error_code": "run_submit_failed", "payload": run_submit_response.snapshot()},
            )
        job_id = self._json_dict(run_submit_response).get("job_id")
        if not job_id:
            return RunExecutionResult(
                run_name=run_scope,
                status="failed",
                pipeline_id=pipeline_id,
                error={"error_code": "run_submit_missing_job_id", "payload": run_submit_response.snapshot()},
            )

        job_final = self._poll_job(job_id)
        self._write_example_json(spec.example_id, "job.final.response.json", job_final.snapshot(), run_scope)
        job_body = self._json_dict(job_final)
        if str(job_body.get("status")) != "succeeded":
            return RunExecutionResult(
                run_name=run_scope,
                status="failed",
                pipeline_id=pipeline_id,
                job_id=job_id,
                error={"error_code": "job_not_succeeded", "payload": job_final.snapshot()},
            )

        try:
            artifacts = self._export_artifacts(
                spec,
                run_spec,
                project_id=project_id,
                pipeline_id=pipeline_id,
            )
            retrieval_result_ids = self._execute_retrievals(
                spec,
                run_spec,
                project_id=project_id,
                artifacts=artifacts,
            )
        except Exception as exc:
            return RunExecutionResult(
                run_name=run_scope,
                status="failed",
                pipeline_id=pipeline_id,
                job_id=job_id,
                error={"error_code": "artifact_or_retrieval_failed", "payload": {"message": str(exc)}},
            )
        return RunExecutionResult(
            run_name=run_scope,
            status="passed",
            pipeline_id=pipeline_id,
            job_id=job_id,
            retrieval_result_ids=retrieval_result_ids,
        )

    def _export_artifacts(
        self,
        spec: PipelineExampleSpec,
        run_spec: PipelineRunSpec,
        *,
        project_id: str,
        pipeline_id: str,
    ) -> ArtifactSnapshot:
        snapshot = ArtifactSnapshot()
        run_scope = run_spec.run_name

        snapshot.documents = self._fetch_all_paginated(
            f"/projects/{project_id}/pipelines/{pipeline_id}/documents",
            list_filename="artifacts.documents.list.json",
            example_id=spec.example_id,
            scope_parts=(run_scope,),
        )
        for item in snapshot.documents:
            doc_id = item.get("id")
            if not doc_id:
                continue
            response = self.api.get(f"/projects/{project_id}/documents/{doc_id}")
            self._require_ok(response, operation=f"Fetch document artifact '{doc_id}'")
            self._write_example_json(
                spec.example_id,
                f"artifact.document.{doc_id}.json",
                response.snapshot(),
                run_scope,
            )

        for stage_name in self._artifact_stage_names(run_spec.pipeline_create_payload):
            segments = self._fetch_all_paginated(
                f"/projects/{project_id}/pipelines/{pipeline_id}/segments/{stage_name}",
                list_filename=f"artifacts.segments.{_slug(stage_name)}.list.json",
                example_id=spec.example_id,
                scope_parts=(run_scope,),
            )
            snapshot.segments_by_stage[stage_name] = segments
            for item in segments:
                segment_id = item.get("id")
                if not segment_id:
                    continue
                response = self.api.get(f"/projects/{project_id}/segments/{segment_id}")
                self._require_ok(response, operation=f"Fetch segment artifact '{segment_id}'")
                self._write_example_json(
                    spec.example_id,
                    f"artifact.segment.{segment_id}.json",
                    response.snapshot(),
                    run_scope,
                )

        snapshot.indices = self._fetch_all_paginated(
            f"/projects/{project_id}/indices",
            list_filename="artifacts.indices.list.json",
            example_id=spec.example_id,
            base_query={"pipeline_id": pipeline_id},
            scope_parts=(run_scope,),
        )
        for item in snapshot.indices:
            index_id = item.get("artifact_id")
            if not index_id:
                continue
            response = self.api.get(f"/projects/{project_id}/indices/{index_id}")
            self._require_ok(response, operation=f"Fetch index artifact '{index_id}'")
            self._write_example_json(
                spec.example_id,
                f"artifact.index.{index_id}.json",
                response.snapshot(),
                run_scope,
            )

        return snapshot

    def _execute_retrievals(
        self,
        spec: PipelineExampleSpec,
        run_spec: PipelineRunSpec,
        *,
        project_id: str,
        artifacts: ArtifactSnapshot,
    ) -> list[str]:
        retrieval_result_ids: list[str] = []
        for retrieval in run_spec.retrievals:
            retrieval_result_ids.extend(
                self._execute_retrieval(
                    spec,
                    run_spec,
                    retrieval,
                    project_id=project_id,
                    artifacts=artifacts,
                )
            )
        return retrieval_result_ids

    def _execute_retrieval(
        self,
        spec: PipelineExampleSpec,
        run_spec: PipelineRunSpec,
        retrieval: RetrievalPlan,
        *,
        project_id: str,
        artifacts: ArtifactSnapshot,
    ) -> list[str]:
        strict = any(query.strict_match for query in retrieval.queries)
        create_request = {
            "retriever_type": retrieval.retriever_type,
            "params": retrieval.retriever_params,
        }
        if retrieval.source_kind == "index":
            if not artifacts.indices:
                if strict:
                    raise RuntimeError("No index artifacts available for strict retrieval execution")
                return []
            create_request["index_artifact_id"] = str(artifacts.indices[0]["artifact_id"])
        else:
            stage_name = retrieval.source_stage_name or ""
            stage_items = artifacts.segments_by_stage.get(stage_name, [])
            artifact_ids = [str(item["id"]) for item in stage_items if item.get("id")]
            if not artifact_ids:
                if strict:
                    raise RuntimeError(f"No stage artifacts available for strict retrieval source '{stage_name}'")
                return []
            create_request["source_artifact_ids"] = artifact_ids

        self._write_example_json(
            spec.example_id,
            "retriever.create.request.json",
            create_request,
            run_spec.run_name,
            retrieval.name,
        )
        create_response = self.api.post(f"/projects/{project_id}/retrievers", json_payload=create_request)
        self._write_example_json(
            spec.example_id,
            "retriever.create.response.json",
            create_response.snapshot(),
            run_spec.run_name,
            retrieval.name,
        )
        if not create_response.ok:
            if strict:
                self._require_ok(create_response, operation=f"Create retriever '{retrieval.name}'")
            logger.error(
                "Retriever creation failed in non-strict mode: example_id=%s run=%s retrieval=%s status=%s body=%s",
                spec.example_id,
                run_spec.run_name,
                retrieval.name,
                create_response.status_code,
                self._response_body_text(create_response),
            )
            return []
        retriever_id = str(self._json_dict(create_response).get("id"))

        session_id: str | None = None
        if retrieval.requires_session:
            init_response = self.api.post(f"/projects/{project_id}/retrievers/{retriever_id}/init", json_payload={})
            self._write_example_json(
                spec.example_id,
                "retriever.init.response.json",
                init_response.snapshot(),
                run_spec.run_name,
                retrieval.name,
            )
            self._require_ok(init_response, operation=f"Init retriever session '{retriever_id}'")
            session_id = str(self._json_dict(init_response).get("id"))

        result_ids: list[str] = []
        try:
            for query_plan in retrieval.queries:
                result_id = self._execute_retrieval_query(
                    spec,
                    run_spec,
                    retrieval,
                    query_plan,
                    project_id=project_id,
                    retriever_id=retriever_id,
                    session_id=session_id,
                )
                if result_id is not None:
                    result_ids.append(result_id)
        finally:
            if session_id:
                release_response = self.api.post(
                    f"/projects/{project_id}/retrievers/{retriever_id}/release",
                    json_payload={"session_id": session_id},
                )
                self._require_ok(release_response, operation=f"Release retriever session '{session_id}'")
                self._write_example_json(
                    spec.example_id,
                    "retriever.release.response.json",
                    release_response.snapshot(),
                    run_spec.run_name,
                    retrieval.name,
                )
        return result_ids

    def _execute_retrieval_query(
        self,
        spec: PipelineExampleSpec,
        run_spec: PipelineRunSpec,
        retrieval: RetrievalPlan,
        query_plan: RetrievalQueryPlan,
        *,
        project_id: str,
        retriever_id: str,
        session_id: str | None,
    ) -> str | None:
        query_request = {
            "query": query_plan.query,
        }
        if session_id:
            query_request["session_id"] = session_id

        self._write_example_json(
            spec.example_id,
            "retriever.query.request.json",
            query_request,
            run_spec.run_name,
            retrieval.name,
            query_plan.name,
        )
        query_response = self.api.post(
            f"/projects/{project_id}/retrievers/{retriever_id}/query",
            json_payload=query_request,
        )
        self._write_example_json(
            spec.example_id,
            "retriever.query.response.json",
            query_response.snapshot(),
            run_spec.run_name,
            retrieval.name,
            query_plan.name,
        )
        if not query_response.ok:
            if query_plan.strict_match:
                self._require_ok(query_response, operation=f"Query retriever '{retriever_id}'")
            logger.error(
                "Retriever query failed in non-strict mode: example_id=%s run=%s retrieval=%s query=%s status=%s body=%s",
                spec.example_id,
                run_spec.run_name,
                retrieval.name,
                query_plan.name,
                query_response.status_code,
                self._response_body_text(query_response),
            )
            return None

        query_body = self._json_dict(query_response)
        retrieval_result_id = str(query_body.get("retrieval_result_id"))
        detail_response = self.api.get(f"/projects/{project_id}/retrieval-results/{retrieval_result_id}")
        self._require_ok(detail_response, operation=f"Get retrieval result '{retrieval_result_id}'")
        self._write_example_json(
            spec.example_id,
            f"retrieval.result.{retrieval_result_id}.json",
            detail_response.snapshot(),
            run_spec.run_name,
            retrieval.name,
            query_plan.name,
        )
        list_response = self.api.get(f"/projects/{project_id}/retrievers/{retriever_id}/results")
        self._require_ok(list_response, operation=f"List retrieval results for retriever '{retriever_id}'")
        self._write_example_json(
            spec.example_id,
            "retriever.results.list.json",
            list_response.snapshot(),
            run_spec.run_name,
            retrieval.name,
            query_plan.name,
        )
        return retrieval_result_id

    def _fetch_all_paginated(
        self,
        path: str,
        *,
        list_filename: str,
        example_id: str,
        scope_parts: tuple[str, ...] = (),
        base_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        offset = 0
        limit = 200
        all_items: list[dict[str, Any]] = []
        total = None
        while total is None or len(all_items) < total:
            query = {"offset": offset, "limit": limit}
            if base_query:
                query.update(base_query)
            response = self.api.get(path, query=query)
            self._require_ok(response, operation=f"Artifact list call ({path})")
            body = self._json_dict(response)
            items = body.get("items")
            if not isinstance(items, list):
                raise RuntimeError(f"Artifact list call ({path}) did not return paginated items")
            total = int(body.get("total", len(items)))
            all_items.extend(item for item in items if isinstance(item, dict))
            if not items:
                break
            offset += limit
        payload = {"total": total or len(all_items), "items": all_items}
        self._write_example_json(example_id, list_filename, payload, *scope_parts)
        return all_items

    def _poll_job(self, job_id: str) -> ApiResponse:
        started = time.monotonic()
        last_status: str | None = None
        transient_errors = 0
        last_error: str | None = None
        while True:
            try:
                response = self.api.get(f"/jobs/{job_id}")
            except RuntimeError as exc:
                transient_errors += 1
                last_error = str(exc)
                logger.warning(
                    "Transient poll error: job_id=%s attempt=%s error=%s",
                    job_id,
                    transient_errors,
                    exc,
                )
                if time.monotonic() - started > self.config.poll_timeout_seconds:
                    raise TimeoutError(
                        f"Polling job '{job_id}' timed out after transient errors; last_error={last_error}"
                    ) from exc
                time.sleep(self.config.poll_interval_seconds)
                continue
            if not response.ok:
                return response
            body = self._json_dict(response)
            status_value = str(body.get("status"))
            if status_value != last_status:
                logger.info(
                    "Polled job status: job_id=%s status=%s stage=%s",
                    job_id,
                    status_value,
                    body.get("stage"),
                )
                last_status = status_value
            if status_value in TERMINAL_JOB_STATUSES:
                return response
            if time.monotonic() - started > self.config.poll_timeout_seconds:
                raise TimeoutError(f"Polling job '{job_id}' timed out")
            time.sleep(self.config.poll_interval_seconds)

    @staticmethod
    def _artifact_stage_names(pipeline_payload: dict[str, Any]) -> list[str]:
        names: list[str] = []
        runtime_input = pipeline_payload.get("runtime_input")
        if isinstance(runtime_input, dict):
            alias = runtime_input.get("alias")
            if isinstance(alias, str) and alias.strip() and runtime_input.get("artifact_kind") == "segment":
                names.append(alias)

        stages = pipeline_payload.get("stages")
        if isinstance(stages, list):
            for stage in stages:
                if not isinstance(stage, dict):
                    continue
                stage_name = stage.get("stage_name")
                if isinstance(stage_name, str) and stage_name.strip():
                    names.append(stage_name)
        return names

    @staticmethod
    def _json_dict(response: ApiResponse) -> dict[str, Any]:
        if isinstance(response.json_body, dict):
            return response.json_body
        return {}

    @staticmethod
    def _response_body_text(response: ApiResponse) -> str:
        body = response.json_body if response.json_body is not None else response.text_body
        if isinstance(body, (dict, list)):
            raw = json.dumps(body, ensure_ascii=False)
        else:
            raw = str(body)
        normalized = " ".join(raw.split())
        return normalized[:600]

    def _require_ok(self, response: ApiResponse, *, operation: str) -> None:
        if response.ok:
            return
        raise RuntimeError(
            f"{operation} failed with status {response.status_code}; body={self._response_body_text(response)}"
        )

    def _write_example_json(self, example_id: str, filename: str, payload: Any, *scope_parts: str) -> Path:
        parts = [_slug(part) for part in scope_parts if part and part != "main"]
        scoped_name = f"{'.'.join(parts)}.{filename}" if parts else filename
        return self.exporter.write_example_json(example_id, scoped_name, payload)

    def _pass_expected_error(
        self,
        spec: PipelineExampleSpec,
        started_at: str,
        *,
        project_id: str | None,
        run_results: list[RunExecutionResult],
        notes: str,
        error_payload: dict[str, Any] | None,
    ) -> ExampleRunResult:
        finished_at = datetime.now(tz=UTC).isoformat()
        result = ExampleRunResult(
            example_id=spec.example_id,
            status="passed",
            started_at=started_at,
            finished_at=finished_at,
            project_id=project_id,
            pipeline_id=run_results[0].pipeline_id if run_results else None,
            job_id=run_results[0].job_id if run_results else None,
            retrieval_result_id=(
                run_results[0].retrieval_result_ids[0]
                if run_results and run_results[0].retrieval_result_ids
                else None
            ),
            runs=run_results,
            notes=notes,
            error=error_payload,
        )
        self._write_example_json(spec.example_id, "summary.json", asdict(result))
        return result

    def _fail(
        self,
        spec: PipelineExampleSpec,
        started_at: str,
        error_code: str,
        error_payload: dict[str, Any],
        *,
        project_id: str | None = None,
        run_results: list[RunExecutionResult] | None = None,
    ) -> ExampleRunResult:
        finished_at = datetime.now(tz=UTC).isoformat()
        payload = {"error_code": error_code, "payload": error_payload}
        self._write_example_json(spec.example_id, "error.json", payload)
        logger.error(
            "Example marked failed: example_id=%s error_code=%s project_id=%s",
            spec.example_id,
            error_code,
            project_id,
        )
        result = ExampleRunResult(
            example_id=spec.example_id,
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            project_id=project_id,
            pipeline_id=run_results[0].pipeline_id if run_results else None,
            job_id=run_results[0].job_id if run_results else None,
            retrieval_result_id=(
                run_results[0].retrieval_result_ids[0]
                if run_results and run_results[0].retrieval_result_ids
                else None
            ),
            runs=list(run_results or []),
            error=payload,
        )
        self._write_example_json(spec.example_id, "summary.json", asdict(result))
        return result
