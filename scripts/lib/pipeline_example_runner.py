from __future__ import annotations

import copy
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

from scripts.lib.pipeline_example_export import SnapshotExporter
from scripts.lib.pipeline_example_manifest import PipelineExampleSpec, build_run_payload


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
    def __init__(self, base_url: str, token: str, timeout_seconds: int):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
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
        url = self._build_url(path, query=query)
        body_bytes = None
        headers = dict(self.default_headers)
        if json_payload is not None:
            headers["Content-Type"] = "application/json"
            body_bytes = json.dumps(json_payload, ensure_ascii=False).encode("utf-8")

        req = request.Request(url=url, data=body_bytes, method=method.upper(), headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                parsed = self._parse_json(raw)
                return ApiResponse(
                    method=method.upper(),
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
                method=method.upper(),
                url=url,
                status_code=exc.code,
                headers={k: v for k, v in exc.headers.items()} if exc.headers else {},
                json_body=parsed,
                text_body=raw,
            )
        except URLError as exc:
            reason_type = type(exc.reason).__name__ if getattr(exc, "reason", None) is not None else type(exc).__name__
            raise RuntimeError(
                f"Network error while calling {method.upper()} {url}: {reason_type}: {exc}"
            ) from exc
        except OSError as exc:
            raise RuntimeError(
                f"Socket error while calling {method.upper()} {url}: {type(exc).__name__}: {exc}"
            ) from exc

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

    def delete(self, path: str) -> ApiResponse:
        return self.request("DELETE", path)


@dataclass(slots=True)
class RunnerConfig:
    example_docs_root: Path
    poll_interval_seconds: int = 2
    poll_timeout_seconds: int = 1200
    continue_on_error: bool = True


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
    error: dict[str, Any] | None = None
    notes: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == "passed"


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
        pipeline_id: str | None = None
        job_id: str | None = None
        state: dict[str, Any] = {
            "example_id": spec.example_id,
            "source_example_file": spec.source_example_file,
            "started_at": started_at,
        }
        try:
            logger.info("Starting example run: example_id=%s", spec.example_id)
            project_response = self.api.post("/projects", json_payload={"name": f"example-{spec.example_id}"})
            self.exporter.write_example_json(spec.example_id, "project.create.response.json", project_response.snapshot())
            if not project_response.ok:
                return self._fail(spec, started_at, "project_create_failed", project_response.snapshot())
            project_id = self._json_dict(project_response).get("id")
            state["project_id"] = project_id

            pipeline_payload = copy.deepcopy(spec.pipeline_create_payload)
            pipeline_payload["name"] = pipeline_payload.get("name") or f"pipeline-{spec.example_id}"
            self.exporter.write_example_json(spec.example_id, "pipeline.create.request.json", pipeline_payload)
            pipeline_response = self.api.post(f"/projects/{project_id}/pipelines", json_payload=pipeline_payload)
            self.exporter.write_example_json(spec.example_id, "pipeline.create.response.json", pipeline_response.snapshot())
            if not pipeline_response.ok:
                return self._fail(spec, started_at, "pipeline_create_failed", pipeline_response.snapshot(), project_id=project_id)
            pipeline_id = self._json_dict(pipeline_response).get("id")
            state["pipeline_id"] = pipeline_id

            run_payload = build_run_payload(spec, self.config.example_docs_root)
            self.exporter.write_example_json(spec.example_id, "run.submit.request.json", run_payload)
            run_submit_response = self.api.post(
                f"/projects/{project_id}/pipelines/{pipeline_id}/runs",
                json_payload=run_payload,
            )
            self.exporter.write_example_json(spec.example_id, "run.submit.response.json", run_submit_response.snapshot())
            if not run_submit_response.ok:
                return self._fail(
                    spec,
                    started_at,
                    "run_submit_failed",
                    run_submit_response.snapshot(),
                    project_id=project_id,
                    pipeline_id=pipeline_id,
                )
            job_id = self._json_dict(run_submit_response).get("job_id")
            if not job_id:
                return self._fail(
                    spec,
                    started_at,
                    "run_submit_missing_job_id",
                    run_submit_response.snapshot(),
                    project_id=project_id,
                    pipeline_id=pipeline_id,
                )
            state["job_id"] = job_id

            job_final = self._poll_job(job_id)
            self.exporter.write_example_json(spec.example_id, "job.final.response.json", job_final.snapshot())
            job_body = self._json_dict(job_final)
            job_status = str(job_body.get("status"))
            if spec.expected_outcome == "success" and job_status != "succeeded":
                return self._fail(
                    spec,
                    started_at,
                    "job_not_succeeded",
                    job_final.snapshot(),
                    project_id=project_id,
                    pipeline_id=pipeline_id,
                    job_id=job_id,
                )
            if spec.expected_outcome == "error":
                if job_status in {"failed", "canceled"}:
                    finished_at = datetime.now(tz=UTC).isoformat()
                    result = ExampleRunResult(
                        example_id=spec.example_id,
                        status="passed",
                        started_at=started_at,
                        finished_at=finished_at,
                        project_id=project_id,
                        pipeline_id=pipeline_id,
                        job_id=job_id,
                        notes="Expected error outcome observed",
                    )
                    self.exporter.write_example_json(spec.example_id, "summary.json", asdict(result))
                    return result
                return self._fail(
                    spec,
                    started_at,
                    "expected_error_not_observed",
                    job_final.snapshot(),
                    project_id=project_id,
                    pipeline_id=pipeline_id,
                    job_id=job_id,
                )

            indices = self._export_artifacts(spec, project_id=project_id, pipeline_id=pipeline_id)
            retrieval_result_id = self._execute_retrieval(
                spec,
                project_id=project_id,
                index_items=indices,
            )

            finished_at = datetime.now(tz=UTC).isoformat()
            result = ExampleRunResult(
                example_id=spec.example_id,
                status="passed",
                started_at=started_at,
                finished_at=finished_at,
                project_id=project_id,
                pipeline_id=pipeline_id,
                job_id=job_id,
                retrieval_result_id=retrieval_result_id,
            )
            self.exporter.write_example_json(spec.example_id, "summary.json", asdict(result))
            logger.info(
                "Example run completed: example_id=%s project_id=%s pipeline_id=%s job_id=%s",
                spec.example_id,
                project_id,
                pipeline_id,
                job_id,
            )
            return result
        except Exception as exc:  # pragma: no cover - integration safety net
            logger.exception(
                "Example run failed: example_id=%s project_id=%s pipeline_id=%s job_id=%s",
                spec.example_id,
                project_id,
                pipeline_id,
                job_id,
            )
            return self._fail(
                spec,
                started_at,
                "unhandled_exception",
                {"error": str(exc), "error_type": type(exc).__name__},
                project_id=project_id,
                pipeline_id=pipeline_id,
                job_id=job_id,
            )

    def _export_artifacts(
        self,
        spec: PipelineExampleSpec,
        *,
        project_id: str,
        pipeline_id: str,
    ) -> list[dict[str, Any]]:
        docs = self._fetch_all_paginated(
            f"/projects/{project_id}/pipelines/{pipeline_id}/documents",
            list_filename="artifacts.documents.list.json",
            example_id=spec.example_id,
        )
        for item in docs:
            doc_id = item.get("id")
            if not doc_id:
                continue
            response = self.api.get(f"/projects/{project_id}/documents/{doc_id}")
            self.exporter.write_example_json(spec.example_id, f"artifact.document.{doc_id}.json", response.snapshot())

        stage_names = [
            stage.get("stage_name")
            for stage in spec.pipeline_create_payload.get("segmentation_stages", [])
            if isinstance(stage, dict) and stage.get("stage_name")
        ]
        for stage_name in stage_names:
            segments = self._fetch_all_paginated(
                f"/projects/{project_id}/pipelines/{pipeline_id}/segments/{stage_name}",
                list_filename=f"artifacts.segments.{stage_name}.list.json",
                example_id=spec.example_id,
            )
            for item in segments:
                segment_id = item.get("id")
                if not segment_id:
                    continue
                response = self.api.get(f"/projects/{project_id}/segments/{segment_id}")
                self.exporter.write_example_json(spec.example_id, f"artifact.segment.{segment_id}.json", response.snapshot())

        indices = self._fetch_all_paginated(
            f"/projects/{project_id}/indices",
            list_filename="artifacts.indices.list.json",
            example_id=spec.example_id,
            base_query={"pipeline_id": pipeline_id},
        )
        for item in indices:
            index_id = item.get("artifact_id")
            if not index_id:
                continue
            response = self.api.get(f"/projects/{project_id}/indices/{index_id}")
            self.exporter.write_example_json(spec.example_id, f"artifact.index.{index_id}.json", response.snapshot())
        return indices

    def _execute_retrieval(
        self,
        spec: PipelineExampleSpec,
        *,
        project_id: str,
        index_items: list[dict[str, Any]],
    ) -> str | None:
        if not index_items:
            if spec.retrieval_plan.strict_match:
                raise RuntimeError("No index artifacts available for strict retrieval execution")
            return None

        index_id = str(index_items[0]["artifact_id"])
        create_request = {
            "index_artifact_id": index_id,
            "retriever_type": spec.retrieval_plan.retriever_type,
            "params": spec.retrieval_plan.retriever_params,
        }
        self.exporter.write_example_json(spec.example_id, "retriever.create.request.json", create_request)
        create_response = self.api.post(f"/projects/{project_id}/retrievers", json_payload=create_request)
        self.exporter.write_example_json(spec.example_id, "retriever.create.response.json", create_response.snapshot())
        if not create_response.ok:
            if spec.retrieval_plan.strict_match:
                raise RuntimeError(
                    f"Retriever creation failed with status {create_response.status_code}"
                )
            return None
        retriever_id = str(self._json_dict(create_response).get("id"))

        session_id: str | None = None
        if spec.retrieval_plan.requires_session:
            init_response = self.api.post(f"/projects/{project_id}/retrievers/{retriever_id}/init", json_payload={})
            self.exporter.write_example_json(spec.example_id, "retriever.init.response.json", init_response.snapshot())
            if not init_response.ok:
                raise RuntimeError(f"Retriever init failed with status {init_response.status_code}")
            session_id = str(self._json_dict(init_response).get("id"))

        query_request = {
            "query": spec.retrieval_plan.query,
            "top_k": spec.retrieval_plan.top_k,
        }
        if session_id:
            query_request["session_id"] = session_id
        self.exporter.write_example_json(spec.example_id, "retriever.query.request.json", query_request)
        query_response = self.api.post(
            f"/projects/{project_id}/retrievers/{retriever_id}/query",
            json_payload=query_request,
        )
        self.exporter.write_example_json(spec.example_id, "retriever.query.response.json", query_response.snapshot())
        if not query_response.ok:
            raise RuntimeError(f"Retriever query failed with status {query_response.status_code}")

        query_body = self._json_dict(query_response)
        retrieval_result_id = str(query_body.get("retrieval_result_id"))
        detail_response = self.api.get(f"/projects/{project_id}/retrieval-results/{retrieval_result_id}")
        self.exporter.write_example_json(
            spec.example_id,
            f"retrieval.result.{retrieval_result_id}.json",
            detail_response.snapshot(),
        )
        list_response = self.api.get(f"/projects/{project_id}/retrievers/{retriever_id}/results")
        self.exporter.write_example_json(spec.example_id, "retriever.results.list.json", list_response.snapshot())

        if session_id:
            release_response = self.api.post(
                f"/projects/{project_id}/retrievers/{retriever_id}/release",
                json_payload={"session_id": session_id},
            )
            self.exporter.write_example_json(
                spec.example_id,
                "retriever.release.response.json",
                release_response.snapshot(),
            )
        return retrieval_result_id

    def _fetch_all_paginated(
        self,
        path: str,
        *,
        list_filename: str,
        example_id: str,
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
            if not response.ok:
                raise RuntimeError(f"Artifact list call failed ({path}) with status {response.status_code}")
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
        self.exporter.write_example_json(example_id, list_filename, payload)
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
    def _json_dict(response: ApiResponse) -> dict[str, Any]:
        if isinstance(response.json_body, dict):
            return response.json_body
        return {}

    def _fail(
        self,
        spec: PipelineExampleSpec,
        started_at: str,
        error_code: str,
        error_payload: dict[str, Any],
        *,
        project_id: str | None = None,
        pipeline_id: str | None = None,
        job_id: str | None = None,
    ) -> ExampleRunResult:
        finished_at = datetime.now(tz=UTC).isoformat()
        payload = {"error_code": error_code, "payload": error_payload}
        self.exporter.write_example_json(spec.example_id, "error.json", payload)
        logger.error(
            "Example marked failed: example_id=%s error_code=%s project_id=%s pipeline_id=%s job_id=%s",
            spec.example_id,
            error_code,
            project_id,
            pipeline_id,
            job_id,
        )
        result = ExampleRunResult(
            example_id=spec.example_id,
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            project_id=project_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            error=payload,
        )
        self.exporter.write_example_json(spec.example_id, "summary.json", asdict(result))
        return result
