from __future__ import annotations


def test_capabilities_endpoints(client):
    resp = client.get("/api/v1/capabilities")
    assert resp.status_code == 200
    data = resp.json()
    assert "matrix" in data
    assert "loaders" in data["matrix"]
    assert "splitters" in data["matrix"]

    ex = client.get("/api/v1/capabilities/examples")
    assert ex.status_code == 200
    ex_data = ex.json()
    assert "total_examples" in ex_data
    assert ex_data["covered_examples"] == ex_data["total_examples"]


def test_pipeline_validation_rejects_unknown_loader_params(client):
    pid = client.post("/api/v1/projects", json={"name": "p1"}).json()["id"]
    payload = {
        "name": "bad-pipeline",
        "loader": {"type": "TextLoader", "params": {"unknown_key": 1}},
        "inputs": [],
        "segmentation_stages": [],
    }
    resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=payload)
    assert resp.status_code == 422
    assert "unknown" in resp.json()["message"].lower()


def test_loader_segmentation_run_flow(client):
    pid = client.post("/api/v1/projects", json={"name": "p2"}).json()["id"]
    pipeline_payload = {
        "name": "text-flow",
        "loader": {"type": "TextLoader", "params": {}},
        "inputs": [],
        "segmentation_stages": [
            {
                "stage_name": "s1",
                "splitter_type": "RecursiveCharacterTextSplitter",
                "params": {"chunk_size": 16, "chunk_overlap": 0},
                "input_aliases": ["LOADING"],
                "position": 0,
            }
        ],
    }
    pl_resp = client.post(f"/api/v1/projects/{pid}/pipelines", json=pipeline_payload)
    assert pl_resp.status_code == 201
    plid = pl_resp.json()["id"]

    run_resp = client.post(
        f"/api/v1/projects/{pid}/pipelines/{plid}/runs",
        json={"text": "alpha beta gamma delta epsilon zeta eta theta iota"},
    )
    assert run_resp.status_code == 202
    job_id = run_resp.json()["job_id"]

    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "succeeded"
    assert job["result"]["artifacts_produced"]["document"] == 1
    assert job["result"]["artifacts_produced"]["segment"] > 0

    docs = client.get(f"/api/v1/projects/{pid}/pipelines/{plid}/documents").json()
    segs = client.get(f"/api/v1/projects/{pid}/pipelines/{plid}/segments/s1").json()
    assert docs["total"] == 1
    assert segs["total"] > 0

