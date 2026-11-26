"""Runner: OOM shrinking, single-doc failure, restart resume semantics."""

import asyncio

from app.db import Database
from app.demo import build_demo_job
from app.node import SimulatedClassificationNode, estimate_vram_mb
from app.runner import RunManager

RESIDENT = 128
DOC_BASE = 48
MB_PER_100 = 4


def _build(tmp_path, name="r.db"):
    db = Database(str(tmp_path / name))
    db.seed_node_settings(512, 0)
    node = SimulatedClassificationNode(db, model_resident_mb=RESIDENT)
    manager = RunManager(db, node)
    return db, node, manager


def _create_demo(db, job_id="JOB-DEMO"):
    payload = build_demo_job()
    docs = [
        {
            "doc_id": d.id,
            "content": d.text,
            "vram_estimate_mb": estimate_vram_mb(
                d.text, doc_base_mb=DOC_BASE, mb_per_100_chars=MB_PER_100
            ),
        }
        for d in payload.documents
    ]
    return db.create_job(job_id, docs, 4, 512)


def test_demo_shrink_and_single_doc_failure(tmp_path):
    db, _, manager = _build(tmp_path)
    _create_demo(db)

    asyncio.run(manager.run_job("JOB-DEMO"))

    job = db.get_job("JOB-DEMO")
    assert job["status"] == "completed_with_failures"
    assert job["current_batch_size"] == 1
    counts = db.status_counts("JOB-DEMO")
    assert counts["succeeded"] == 7
    assert counts["failed"] == 1
    assert counts["pending"] == 0 and counts["processing"] == 0

    docs = db.list_documents("JOB-DEMO")
    # submission order preserved
    assert [d["doc_id"] for d in docs] == [f"DEMO-{i:02d}" for i in range(1, 9)]
    failed = docs[4]
    assert failed["doc_id"] == "DEMO-05"
    assert failed["status"] == "failed"
    assert failed["error_code"] == "document_exceeds_vram_limit"
    assert "超过当前上限 512MB" in failed["error_message"]
    assert all(d["status"] == "succeeded" for i, d in enumerate(docs) if i != 4)

    # The first dispatch was a batch of 4; later DEMO-03/04 share a batch
    # of 2 after shrinking.
    dispatches = db.list_dispatches("JOB-DEMO")
    first_batch = {d["seq"] for d in dispatches if d["batch_id"] == dispatches[0]["batch_id"]}
    assert first_batch == {0, 1, 2, 3}
    db.close()


def test_restart_resume_does_not_redispatch_terminal_docs(tmp_path):
    db, _, manager = _build(tmp_path, "restart.db")
    _create_demo(db, "JOB-R")

    # History from a previous process: seq0 classified, seq4 failed, and a
    # batch containing seq1..3 was claimed when the process died.
    db.mark_succeeded("JOB-R", 0, "finance", 0.91)
    db.mark_failed("JOB-R", 4, "document_exceeds_vram_limit", "old failure")
    db.claim_next_batch("JOB-R", 3, "dead-process-owner", 512)  # seq 1,2,3

    # ---- simulated restart ----
    reset_docs, interrupted = db.reset_stale_work()
    assert reset_docs == 3
    assert interrupted == 0  # job row was never marked running in this setup

    asyncio.run(manager.run_job("JOB-R"))

    job = db.get_job("JOB-R")
    assert job["status"] == "completed_with_failures"
    docs = {d["seq"]: d for d in db.list_documents("JOB-R")}
    assert docs[0]["category"] == "finance"  # untouched result
    assert docs[4]["error_message"] == "old failure"  # untouched failure

    # Terminal seq 0 / 4 were never claimed, hence have no dispatch rows at
    # all: the post-restart run must not create any either.
    dispatches_after = db.list_dispatches("JOB-R")
    for seq in (0, 4):
        assert [d for d in dispatches_after if d["seq"] == seq] == [], (
            f"terminal seq {seq} was re-dispatched"
        )

    # Every non-terminal doc reached a terminal state.
    assert all(
        docs[s]["status"] in ("succeeded", "failed") for s in range(8)
    )
    db.close()


def test_concurrent_runs_never_double_dispatch(tmp_path):
    db, _, manager = _build(tmp_path, "conc.db")
    docs = [
        {"doc_id": f"C{i:02d}", "content": "普通短文本",
         "vram_estimate_mb": 50}
        for i in range(12)
    ]
    db.create_job("JOB-C", docs, 4, 4096)  # huge limit: no OOM

    async def scenario():
        await asyncio.gather(
            manager.run_job("JOB-C"),
            manager.run_job("JOB-C"),
            manager.run_job("JOB-C"),
        )

    asyncio.run(scenario())

    dispatches = db.list_dispatches("JOB-C")
    per_seq: dict[int, int] = {}
    for d in dispatches:
        per_seq[d["seq"]] = per_seq.get(d["seq"], 0) + 1
    assert per_seq == {i: 1 for i in range(12)}
    assert db.status_counts("JOB-C")["succeeded"] == 12
    db.close()


def test_request_start_dedups_live_task(tmp_path):
    db, _, manager = _build(tmp_path, "dedup.db")
    docs = [
        {"doc_id": f"D{i}", "content": "短文本", "vram_estimate_mb": 50}
        for i in range(6)
    ]
    db.create_job("JOB-D", docs, 2, 4096)

    async def scenario():
        assert manager.request_start("JOB-D") is True
        assert manager.request_start("JOB-D") is False
        assert manager.is_running("JOB-D") is True
        await manager.wait_for("JOB-D")

    asyncio.run(scenario())
    assert manager.is_running("JOB-D") is False
    assert db.status_counts("JOB-D")["succeeded"] == 6
    db.close()
