"""HTTP API tests through FastAPI TestClient."""

import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "api.db"),
        default_vram_limit_mb=512,
        default_batch_size=4,
        default_delay_ms=0,
        model_resident_mb=128,
        doc_base_mb=48,
        mb_per_100_chars=4,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _wait_terminal(client: TestClient, job_id: str, timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/jobs/{job_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] in (
            "completed",
            "completed_with_failures",
            "error",
        ):
            return body
        time.sleep(0.02)
    raise AssertionError("job did not finish in time")


def test_health_and_redirect(client):
    assert client.get("/health").json()["status"] == "ok"
    r = client.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"] == "/docs"


def test_create_query_order_and_completion(client):
    payload = {
        "job_id": "JOB-API-1",
        "batch_size": 2,
        "vram_limit_mb": 4096,
        "documents": [
            {"id": "A", "text": "银行金融财报投资"},
            {"id": "B", "text": "球队比赛进球体育"},
            {"id": "C", "text": "芯片算法人工智能"},
        ],
    }
    r = client.post("/jobs", json=payload)
    assert r.status_code == 201, r.text
    assert r.json()["job_id"] == "JOB-API-1"

    body = _wait_terminal(client, "JOB-API-1")
    assert body["status"] == "completed"
    assert [d["doc_id"] for d in body["documents"]] == ["A", "B", "C"]
    assert [d["seq"] for d in body["documents"]] == [0, 1, 2]
    assert all(d["status"] == "succeeded" for d in body["documents"])
    assert all(d["category"] for d in body["documents"])
    assert body["counts"]["succeeded"] == 3


def test_demo_endpoint_shrink_and_failure(client):
    r = client.post("/demo/run")
    assert r.status_code == 201
    assert r.json()["job_id"] == "JOB-DEMO-SHRINK"

    body = _wait_terminal(client, "JOB-DEMO-SHRINK")
    assert body["status"] == "completed_with_failures"
    assert body["counts"]["succeeded"] == 7
    assert body["counts"]["failed"] == 1
    assert body["current_batch_size"] == 1

    failed = [d for d in body["documents"] if d["status"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["doc_id"] == "DEMO-05"
    assert failed[0]["error_code"] == "document_exceeds_vram_limit"
    assert failed[0]["error_message"]

    dr = client.get("/jobs/JOB-DEMO-SHRINK/dispatches")
    assert dr.status_code == 200
    assert dr.json()["dispatch_count"] >= 8


def test_404_and_validation(client):
    assert client.get("/jobs/NOPE").status_code == 404
    assert client.post("/jobs/NOPE/resume").status_code == 404

    # duplicate document ids -> 422
    r = client.post(
        "/jobs",
        json={
            "documents": [
                {"id": "X", "text": "a"},
                {"id": "X", "text": "b"},
            ]
        },
    )
    assert r.status_code == 422

    # empty documents -> 422
    r = client.post("/jobs", json={"documents": []})
    assert r.status_code == 422


def test_conflicting_job_id_409(client):
    payload = {"job_id": "DUP", "documents": [{"id": "1", "text": "文本"}]}
    assert client.post("/jobs", json=payload).status_code == 201
    time.sleep(0.05)
    r = client.post("/jobs", json=payload)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "job_id_exists"


def test_resume_finished_job_says_nothing_to_do(client):
    client.post(
        "/jobs",
        json={
            "job_id": "DONE",
            "vram_limit_mb": 4096,
            "documents": [{"id": "1", "text": "短文本"}],
        },
    )
    _wait_terminal(client, "DONE")
    r = client.post("/jobs/DONE/resume")
    assert r.status_code == 200
    body = r.json()
    assert body["started"] is False
    assert "终态" in body["message"]


def test_node_config_get_and_put(client):
    cfg = client.get("/admin/node").json()
    assert cfg["vram_limit_mb"] == 512
    updated = client.put(
        "/admin/node", json={"vram_limit_mb": 256, "per_doc_delay_ms": 5}
    ).json()
    assert updated["vram_limit_mb"] == 256
    assert updated["per_doc_delay_ms"] == 5
    # persisted: a new app against the same DB would read it; at least the
    # getter now returns it.
    assert client.get("/admin/node").json()["vram_limit_mb"] == 256


def test_job_listing(client):
    client.post(
        "/jobs",
        json={"job_id": "LIST-1", "documents": [{"id": "1", "text": "文本"}]},
    )
    r = client.get("/jobs")
    assert r.status_code == 200
    ids = [j["job_id"] for j in r.json()["jobs"]]
    assert "LIST-1" in ids
