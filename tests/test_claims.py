"""Atomic claim / no-duplicate-dispatch guarantees."""

import threading

from app.db import Database


def _make_job(db: Database, job_id="J1", n=20):
    docs = [
        {"doc_id": f"D{i:03d}", "content": "x" * 10,
         "vram_estimate_mb": 50}
        for i in range(n)
    ]
    db.create_job(job_id, docs, initial_batch_size=4, vram_limit_mb=512)


def test_claim_returns_ordered_pending_batch(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    _make_job(db)
    batch = db.claim_next_batch("J1", 4, "owner-a", 512)
    assert [d["seq"] for d in batch] == [0, 1, 2, 3]
    # rows are now processing, so the next claim skips them
    batch2 = db.claim_next_batch("J1", 4, "owner-a", 512)
    assert [d["seq"] for d in batch2] == [4, 5, 6, 7]
    db.close()


def test_concurrent_claims_never_overlap(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    _make_job(db, n=20)

    claimed: list[list[int]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker(idx):
        barrier.wait()
        batch = db.claim_next_batch("J1", 3, f"owner-{idx}", 512)
        with lock:
            claimed.append([d["seq"] for d in batch])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    all_seqs = [s for batch in claimed for s in batch]
    # 8 workers * 3 > 20 docs: every document claimed exactly once.
    assert len(all_seqs) == 20
    assert len(set(all_seqs)) == 20
    assert sorted(all_seqs) == list(range(20))
    # one dispatch_log row per claimed doc
    dispatches = db.list_dispatches("J1")
    assert len(dispatches) == 20
    assert len({(d["seq"], d["id"]) for d in dispatches}) == 20
    db.close()


def test_terminal_documents_are_never_claimed(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    _make_job(db, n=6)
    db.mark_succeeded("J1", 0, "finance", 0.9)
    db.mark_failed("J1", 1, "document_exceeds_vram_limit", "too big")

    batch = db.claim_next_batch("J1", 10, "owner-z", 512)
    assert [d["seq"] for d in batch] == [2, 3, 4, 5]

    # and dispatch log only contains the new claim, not terminal docs
    seqs = {d["seq"] for d in db.list_dispatches("J1")}
    assert seqs == {2, 3, 4, 5}
    db.close()


def test_reset_stale_processing_only(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    _make_job(db, n=4)
    batch = db.claim_next_batch("J1", 4, "dead-owner", 512)
    assert len(batch) == 4
    db.mark_succeeded("J1", 0, "tech", 0.95)  # one finished before crash

    reset_docs, _ = db.reset_stale_work()
    assert reset_docs == 3  # the other three were still processing

    docs = {d["seq"]: d["status"] for d in db.list_documents("J1")}
    assert docs[0] == "succeeded"
    assert [s for s, st in docs.items() if st == "pending"] == [1, 2, 3]
    db.close()
