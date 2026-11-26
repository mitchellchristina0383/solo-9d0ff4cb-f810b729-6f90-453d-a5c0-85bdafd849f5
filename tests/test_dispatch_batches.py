"""Batch dispatch ledger: one physical row per batch, global ids, migration.

Covers the refactor where new dispatches store a single
``dispatch_batch_log`` row per claim instead of one ``dispatch_log`` row
per document, while the query API still expands to the original
per-document shape and old data volumes remain readable.
"""

from __future__ import annotations

import json
import sqlite3
import threading

from app.db import Database


def _make_job(db: Database, job_id: str = "J1", n: int = 20, est: int = 50):
    docs = [
        {"doc_id": f"D{i:03d}", "content": "x" * 10, "vram_estimate_mb": est}
        for i in range(n)
    ]
    db.create_job(job_id, docs, initial_batch_size=4, vram_limit_mb=512)


# ------------------------------------------------------- one row per batch


def test_one_physical_ledger_row_per_batch(tmp_path):
    db = Database(str(tmp_path / "b.db"))
    _make_job(db, n=10)
    db.claim_next_batch("J1", 4, "o", 512)
    db.claim_next_batch("J1", 4, "o", 512)
    db.claim_next_batch("J1", 4, "o", 512)  # only two rows remain

    assert db.count_dispatch_batches("J1") == 3
    physical = db._conn.execute(
        "SELECT COUNT(*) AS n FROM dispatch_batch_log"
    ).fetchone()["n"]
    assert physical == 3  # not one row per document (10)
    # new claims never touch the legacy per-document table
    assert db._conn.execute(
        "SELECT COUNT(*) AS n FROM dispatch_log"
    ).fetchone()["n"] == 0

    batches = db.list_dispatch_batches("J1")
    assert [b["doc_count"] for b in batches] == [4, 4, 2]
    first = json.loads(batches[0]["doc_seqs_json"])
    assert [e["seq"] for e in first] == [0, 1, 2, 3]
    assert all("estimate_mb" in e for e in first)
    db.close()


def test_expanded_query_keeps_per_document_fields_and_order(tmp_path):
    db = Database(str(tmp_path / "b.db"))
    _make_job(db, n=10)
    b1 = db.claim_next_batch("J1", 4, "o", 512)
    db.claim_next_batch("J1", 4, "o", 512)
    db.claim_next_batch("J1", 4, "o", 512)

    rows = db.list_dispatches("J1")
    assert len(rows) == 10  # one entry per dispatched document
    assert set(rows[0].keys()) == {
        "id", "seq", "batch_id", "vram_limit_mb", "estimate_mb", "dispatched_at"
    }
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids)
    # every document of the first batch shares one batch-ledger id
    first_group = {r["id"] for r in rows if r["batch_id"] == b1[0]["batch_id"]}
    assert len(first_group) == 1
    assert [r["seq"] for r in rows if r["id"] in first_group] == [0, 1, 2, 3]
    db.close()


# ------------------------------------------------- gaps and re-dispatch


def test_gapping_seqs_and_redispatch_after_shrink(tmp_path):
    db = Database(str(tmp_path / "b.db"))
    _make_job(db, n=6)
    db.mark_succeeded("J1", 0, "finance", 0.9)
    db.mark_failed("J1", 1, "document_exceeds_vram_limit", "big")

    batch = db.claim_next_batch("J1", 10, "o", 512)  # terminal 0,1 skipped
    assert [d["seq"] for d in batch] == [2, 3, 4, 5]

    db.release_to_pending("J1", [3, 4, 5], "o")
    again = db.claim_next_batch("J1", 3, "o", 512)
    assert [d["seq"] for d in again] == [3, 4, 5]

    assert db.count_dispatch_batches("J1") == 2
    view = db.list_dispatches("J1")
    seq3 = [r for r in view if r["seq"] == 3]
    assert len(seq3) == 2
    assert seq3[0]["batch_id"] != seq3[1]["batch_id"]
    assert seq3[0]["id"] != seq3[1]["id"]
    db.close()


# --------------------------------------------- global monotonic + restart


def test_ids_increase_globally_and_continue_after_restart(tmp_path):
    path = tmp_path / "c.db"
    db = Database(str(path))
    _make_job(db, "JA", n=4)
    _make_job(db, "JB", n=4)
    db.claim_next_batch("JA", 4, "o", 512)
    db.claim_next_batch("JB", 4, "o", 512)
    ids = [b["id"] for b in db.list_dispatch_batches("JA")] + [
        b["id"] for b in db.list_dispatch_batches("JB")
    ]
    assert ids == [1, 2]
    db.close()

    db2 = Database(str(path))  # restart against the same file
    _make_job(db2, "JC", n=4)
    db2.claim_next_batch("JC", 4, "o", 512)
    assert db2.list_dispatch_batches("JC")[0]["id"] == 3
    db2.close()


# ---------------------------------------------------- legacy volume migration


def _build_legacy_db(path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE jobs (
          job_id TEXT PRIMARY KEY, status TEXT NOT NULL, total INTEGER NOT NULL,
          initial_batch_size INTEGER NOT NULL, current_batch_size INTEGER NOT NULL,
          vram_limit_mb INTEGER, last_error TEXT, created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT);
        CREATE TABLE documents (
          job_id TEXT NOT NULL, seq INTEGER NOT NULL, doc_id TEXT NOT NULL,
          content TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          category TEXT, confidence REAL, vram_estimate_mb INTEGER NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, claimed_by TEXT, claimed_at TEXT,
          error_code TEXT, error_message TEXT, created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL, finished_at TEXT,
          PRIMARY KEY (job_id, seq), UNIQUE (job_id, doc_id));
        CREATE INDEX idx_documents_claim ON documents(job_id, status, seq);
        CREATE TABLE dispatch_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
          seq INTEGER NOT NULL, batch_id TEXT NOT NULL, vram_limit_mb INTEGER NOT NULL,
          estimate_mb INTEGER NOT NULL, dispatched_at TEXT NOT NULL);
        CREATE INDEX idx_dispatch_job ON dispatch_log(job_id, seq);
        CREATE TABLE node_settings (
          id INTEGER PRIMARY KEY CHECK (id = 1), vram_limit_mb INTEGER NOT NULL,
          per_doc_delay_ms INTEGER NOT NULL, updated_at TEXT NOT NULL);
        """
    )
    conn.execute(
        "INSERT INTO jobs VALUES "
        "('JL','pending',2,2,2,512,NULL,'t','t',NULL,NULL)"
    )
    conn.execute(
        "INSERT INTO documents VALUES "
        "('JL',0,'L0','x','pending',NULL,NULL,50,0,NULL,NULL,NULL,NULL,'t','t',NULL),"
        "('JL',1,'L1','x','pending',NULL,NULL,50,0,NULL,NULL,NULL,NULL,'t','t',NULL)"
    )
    conn.execute(
        "INSERT INTO dispatch_log VALUES "
        "(10,'JL',0,'b_old_1',512,50,'t'),(11,'JL',1,'b_old_1',512,50,'t')"
    )
    conn.execute(
        "INSERT INTO sqlite_sequence(name,seq) VALUES ('dispatch_log',11)"
    )
    conn.commit()
    conn.close()


def test_legacy_volume_old_ids_preserved_and_new_ids_continue(tmp_path):
    path = tmp_path / "legacy.db"
    _build_legacy_db(path)

    db = Database(str(path))  # triggers schema migration
    legacy = db.list_dispatches("JL")
    assert [(r["id"], r["seq"]) for r in legacy] == [(10, 0), (11, 1)]
    assert db.count_dispatch_batches("JL") == 0
    seeded = db._conn.execute(
        "SELECT seq FROM sqlite_sequence WHERE name='dispatch_batch_log'"
    ).fetchone()["seq"]
    assert seeded == 11

    db.claim_next_batch("JL", 2, "o", 512)  # both still pending
    new_id = db.list_dispatch_batches("JL")[0]["id"]
    assert new_id == 12  # legacy max + 1, no collision

    merged = db.list_dispatches("JL")
    assert [(r["id"], r["seq"]) for r in merged] == [
        (10, 0), (11, 1), (12, 0), (12, 1)
    ]
    db.close()


def test_migration_is_idempotent_across_restarts(tmp_path):
    path = tmp_path / "legacy2.db"
    _build_legacy_db(path)

    db = Database(str(path))
    db.claim_next_batch("JL", 2, "o", 512)
    assert db.list_dispatch_batches("JL")[0]["id"] == 12
    db.close()

    db2 = Database(str(path))  # second restart must not reseed/rewind
    db2.create_job(
        "JNEW",
        [{"doc_id": "N1", "content": "x", "vram_estimate_mb": 50}],
        1,
        512,
    )
    db2.claim_next_batch("JNEW", 1, "o", 512)
    assert db2.list_dispatch_batches("JNEW")[0]["id"] == 13
    db2.close()


# --------------------------------------------- concurrency + atomic rollback


def test_concurrent_claims_one_batch_row_each_no_overlap(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    _make_job(db, n=20)
    claimed: list[list[int]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker(idx):
        barrier.wait()
        batch = db.claim_next_batch("J1", 3, f"o{idx}", 512)
        with lock:
            claimed.append([d["seq"] for d in batch])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    all_seqs = [s for batch in claimed for s in batch]
    assert sorted(all_seqs) == list(range(20))
    assert len(set(all_seqs)) == 20
    # 8 workers * 3 > 20 -> seven non-empty claims, seven physical rows
    assert db.count_dispatch_batches("J1") == 7
    assert all(d["attempts"] == 1 for d in db.list_documents("J1"))
    db.close()


class _FailError(sqlite3.Error):
    pass


class _ConnProxy:
    """Swap-in connection wrapper that can force a write to fail."""

    def __init__(self, real):
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_fail", None)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_real"), name)

    def __setattr__(self, name, value):
        if name == "_fail":
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, "_real"), name, value)

    def execute(self, sql, *params):
        predicate = object.__getattribute__(self, "_fail")
        if predicate is not None and predicate(sql):
            raise _FailError(sql)
        return object.__getattribute__(self, "_real").execute(sql, *params)


def test_ledger_write_failure_rolls_back_claim_and_attempts(tmp_path):
    db = Database(str(tmp_path / "r.db"))
    _make_job(db, n=6)
    proxy = _ConnProxy(db._conn)
    db._conn = proxy

    proxy._fail = lambda sql: sql.lstrip().upper().startswith(
        "INSERT INTO DISPATCH_BATCH_LOG"
    )
    try:
        db.claim_next_batch("J1", 4, "o1", 512)
    except _FailError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected ledger failure to propagate")
    proxy._fail = None

    assert all(d["status"] == "pending" for d in db.list_documents("J1"))
    assert all(d["attempts"] == 0 for d in db.list_documents("J1"))
    assert db.count_dispatch_batches("J1") == 0

    # connection still healthy after rollback
    batch = db.claim_next_batch("J1", 4, "o2", 512)
    assert [d["seq"] for d in batch] == [0, 1, 2, 3]
    assert db.count_dispatch_batches("J1") == 1
    db.close()
