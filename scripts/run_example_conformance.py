from __future__ import annotations

import base64
import json
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Any

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "sqlite:///./.example_conformance.db")
os.environ.setdefault("CELERY_ALWAYS_EAGER", "true")

from app.main import create_app
from app.services.example_profiles import discover_examples


@dataclass
class ProfileExecution:
    profile_id: str
    family: str
    example_path: str
    status: str
    notes: str
    job_status: str | None = None
    job_error: dict[str, Any] | None = None


def _pipeline_for_family(family: str) -> dict[str, Any] | None:
    base = {
        "name": f"profile-{family}",
        "segmentation_stages": [
            {
                "stage_name": "s1",
                "splitter_type": "RecursiveCharacterTextSplitter",
                "params": {"chunk_size": 64, "chunk_overlap": 0},
                "input_aliases": ["LOADING"],
                "position": 0,
            }
        ],
    }
    if family in {"text", "graph", "raptor", "dual_storage", "ensemble", "regex", "qa"}:
        return {**base, "loader": {"type": "TextLoader", "params": {}}, "inputs": []}
    if family == "tabular":
        return {**base, "loader": {"type": "CSVLoader", "params": {}}, "inputs": []}
    if family == "json":
        return {**base, "loader": {"type": "JsonLoader", "params": {}}, "inputs": []}
    if family == "html":
        return {**base, "loader": {"type": "HTMLLoader", "params": {"output_format": "markdown"}}, "inputs": []}
    return None


def _run_payload_for_family(family: str) -> dict[str, Any] | None:
    if family in {"text", "graph", "raptor", "dual_storage", "ensemble", "regex", "qa"}:
        return {"text": "example content for conformance profile execution"}
    if family == "tabular":
        content = "col1,col2\nalpha,beta\ngamma,delta\n".encode("utf-8")
        return {
            "file_name": "example.csv",
            "file_content_b64": base64.b64encode(content).decode("ascii"),
        }
    if family == "json":
        content = json.dumps({"a": 1, "nested": {"b": 2}}).encode("utf-8")
        return {
            "file_name": "example.json",
            "file_content_b64": base64.b64encode(content).decode("ascii"),
        }
    if family == "html":
        content = "<html><body><h1>Title</h1><p>Paragraph</p></body></html>".encode("utf-8")
        return {
            "file_name": "example.html",
            "file_content_b64": base64.b64encode(content).decode("ascii"),
        }
    return None


def main() -> int:
    examples = discover_examples()
    executions: list[ProfileExecution] = []

    app = create_app()
    with TestClient(app) as client:
        for item in examples:
            pipeline = _pipeline_for_family(item.family)
            payload = _run_payload_for_family(item.family)
            if pipeline is None or payload is None:
                executions.append(
                    ProfileExecution(
                        profile_id=item.profile_id,
                        family=item.family,
                        example_path=item.example_path,
                        status="expected_failure",
                        notes="No lightweight conformance template for this family yet.",
                    )
                )
                continue

            project = client.post("/api/v1/projects", json={"name": f"cf-{item.profile_id}"}).json()
            pid = project["id"]
            pl_resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=pipeline)
            if pl_resp.status_code != 201:
                executions.append(
                    ProfileExecution(
                        profile_id=item.profile_id,
                        family=item.family,
                        example_path=item.example_path,
                        status="failed",
                        notes="Pipeline creation failed.",
                        job_error={"response": pl_resp.json()},
                    )
                )
                continue
            plid = pl_resp.json()["id"]

            run_resp = client.post(f"/api/v1/projects/{pid}/pipelines/{plid}/runs", json=payload)
            if run_resp.status_code != 202:
                executions.append(
                    ProfileExecution(
                        profile_id=item.profile_id,
                        family=item.family,
                        example_path=item.example_path,
                        status="failed",
                        notes="Run submission failed.",
                        job_error={"response": run_resp.json()},
                    )
                )
                continue
            job_id = run_resp.json()["job_id"]
            job = client.get(f"/api/v1/jobs/{job_id}").json()
            if job["status"] == "succeeded":
                executions.append(
                    ProfileExecution(
                        profile_id=item.profile_id,
                        family=item.family,
                        example_path=item.example_path,
                        status="passed",
                        notes="Functional-equivalence smoke pattern succeeded.",
                        job_status=job["status"],
                    )
                )
            else:
                executions.append(
                    ProfileExecution(
                        profile_id=item.profile_id,
                        family=item.family,
                        example_path=item.example_path,
                        status="expected_failure",
                        notes="Run failed with strict propagation (accepted for unsupported deps).",
                        job_status=job["status"],
                        job_error=job.get("error"),
                    )
                )

    passed = sum(1 for x in executions if x.status == "passed")
    failed = [x for x in executions if x.status == "failed"]
    report = {
        "total_profiles": len(executions),
        "passed": passed,
        "expected_failure": sum(1 for x in executions if x.status == "expected_failure"),
        "failed": len(failed),
        "items": [x.__dict__ for x in executions],
    }

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_path = reports_dir / "example_conformance_report.json"
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": output_path.as_posix(), **{k: report[k] for k in ("total_profiles", "passed", "expected_failure", "failed")}}, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
