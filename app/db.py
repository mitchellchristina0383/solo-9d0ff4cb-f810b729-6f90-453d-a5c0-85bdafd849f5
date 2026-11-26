"""SQLite persistence layer.

Two hard guarantees live here:

* ``claim_next_batch`` runs in a ``BEGIN IMMEDIATE`` transaction and only
  selects rows whose status is ``pending``.  Concurrent resumes (threads,
  requests, or even processes against the same file) therefore can never
  claim the same document twice at the same time.
* Documents that already reached the terminal states (``succeeded`` /
  ``failed``) are never selected by a claim, so restart-and-resume never
  re-dispatches them.

Dispatch flows are stored in two ledgers:

* ``dispatch_log`` -- the legacy per-document table.  It is kept intact so
  an existing data volume stays fully queryable; new claims never write to
  it.
* ``dispatch_batch_log`` -- the current table with exactly **one row per
  batch** (per claim).  Per-document seq / estimate detail is carried in an
  ordered JSON array, so gapping seqs and re-dispatch after shrinking are
  both represented without extra rows.  Its AUTOINCREMENT id increases over
  the whole database in claim-commit order and continues after a restart;
  schema migration seeds it past the largest legacy id so old and new ids
  never collide.

``list_dispatches`` merges both ledgers back into the original per-
document response shape (ordered by ``(id, seq)``), so the query API keeps
returning the old fields one entry per dispatched document.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id              TEXT PRIMARY KEY,
    status              TEXT NOT NULL,
    total               INTEGER NOT NULL,
    initial_batch_size  INTEGER NOT NULL,
    current_batch_size  INTEGER NOT NULL,
    vram_limit_mb       INTEGER,
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    started_at          TEXT,
    finished_at         TEXT
);

CREATE TABLE IF NOT EXISTS documents (
    job_id          TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    doc_id          TEXT NOT NULL,
    content         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    category        TEXT,
    confidence      REAL,
    vram_estimate_mb INTEGER NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    claimed_at      TEXT,
    error_code      TEXT,
    error_message   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    finished_at     TEXT,
    PRIMARY KEY (job_id, seq),
    UNIQUE (job_id, doc_id)
);

CREATE INDEX IF NOT EXISTS idx_documents_claim
    ON documents(job_id, status, seq);

-- Legacy append-only ledger (pre-batch-refactor): one row every time a
-- document was dispatched.  Kept verbatim so an existing data volume keeps
-- every historical flow queryable; new dispatches no longer write here.
CREATE TABLE IF NOT EXISTS dispatch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    batch_id        TEXT NOT NULL,
    vram_limit_mb   INTEGER NOT NULL,
    estimate_mb     INTEGER NOT NULL,
    dispatched_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dispatch_job ON dispatch_log(job_id, seq);

-- New batch ledger: exactly ONE row per dispatch batch (one claim), no
-- matter how many documents the batch contains.  Per-document seq / VRAM
-- estimates live in ``doc_seqs_json`` (submission order; seqs may skip and
-- a document re-dispatched after shrinking gets another row under a new
-- batch id).  AUTOINCREMENT (not plain INTEGER PRIMARY KEY) guarantees ids
-- are strictly increasing over the whole database and never reused after a
-- restart; a migration step seeds the sequence past every legacy id so new
-- ids never collide with old ones.
CREATE TABLE IF NOT EXISTS dispatch_batch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    doc_count       INTEGER NOT NULL,
    doc_seqs_json   TEXT NOT NULL,
    vram_limit_mb   INTEGER NOT NULL,
    dispatched_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dispatch_batch_job
    ON dispatch_batch_log(job_id, id);

CREATE TABLE IF NOT EXISTS node_settings (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    vram_limit_mb       INTEGER NOT NULL,
    per_doc_delay_ms    INTEGER NOT NULL,
    updated_at          TEXT NOT NULL
);
"""


class JobExistsError(Exception):
    """A job with the caller-supplied id already exists."""


class Database:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        # One connection, guarded by the lock.  The workload is tiny and the
        # explicit BEGIN IMMEDIATE transactions make claims atomic.
        self._conn = sqlite3.connect(
            path, timeout=30, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.execute("PRAGMA foreign_keys = ON")
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = NORMAL")
        self._lock = threading.RLock()
        self.init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ schema

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._migrate_dispatch_sequence()

    def _migrate_dispatch_sequence(self) -> None:
        """Hand an existing data volume over to the batch ledger.

        The legacy per-document ``dispatch_log`` stays readable.  When it
        already holds rows but the new ``dispatch_batch_log`` does not, seed
        the new table's AUTOINCREMENT sequence past the largest legacy id so
        new batch ids keep increasing over the whole database and can never
        collide with an old id.  Runs inside the schema-setup lock at
        startup, before any claim can insert a batch row.
        """
        legacy_max = self._conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM dispatch_log"
        ).fetchone()["m"]
        if legacy_max <= 0:
            return
        new_count = self._conn.execute(
            "SELECT COUNT(*) AS n FROM dispatch_batch_log"
        ).fetchone()["n"]
        if new_count > 0:
            return  # migration already done (or a fresh DB already using it)
        self._conn.execute(
            "INSERT OR REPLACE INTO sqlite_sequence(name, seq) "
            "VALUES ('dispatch_batch_log', ?)",
            (legacy_max,),
        )

    def seed_node_settings(self, vram_limit_mb: int, delay_ms: int) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM node_settings WHERE id = 1"
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO node_settings"
                    "(id, vram_limit_mb, per_doc_delay_ms, updated_at) "
                    "VALUES (1, ?, ?, ?)",
                    (vram_limit_mb, delay_ms, utc_now_iso()),
                )

    def get_node_settings(self) -> dict[str, int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT vram_limit_mb, per_doc_delay_ms "
                "FROM node_settings WHERE id = 1"
            ).fetchone()
        if row is None:  # defensive; lifespan always seeds
            return {"vram_limit_mb": 512, "per_doc_delay_ms": 80}
        return {
            "vram_limit_mb": row["vram_limit_mb"],
            "per_doc_delay_ms": row["per_doc_delay_ms"],
        }

    def update_node_settings(
        self, vram_limit_mb: int, per_doc_delay_ms: int
    ) -> dict[str, int]:
        with self._lock:
            self._conn.execute(
                "UPDATE node_settings SET vram_limit_mb = ?, "
                "per_doc_delay_ms = ?, updated_at = ? WHERE id = 1",
                (vram_limit_mb, per_doc_delay_ms, utc_now_iso()),
            )
        return self.get_node_settings()

    # -------------------------------------------------------------------- jobs

    def create_job(
        self,
        job_id: str,
        documents: list[dict[str, Any]],
        initial_batch_size: int,
        vram_limit_mb: int | None,
    ) -> dict[str, Any]:
        """Insert job + documents atomically.

        ``documents`` items need doc_id, content and vram_estimate_mb.
        Raises JobExistsError if job_id is taken.
        """
        now = utc_now_iso()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                existing = self._conn.execute(
                    "SELECT 1 FROM jobs WHERE job_id = ?", (job_id,)
                ).fetchone()
                if existing is not None:
                    self._conn.execute("ROLLBACK")
                    raise JobExistsError(job_id)
                self._conn.execute(
                    "INSERT INTO jobs (job_id, status, total, "
                    "initial_batch_size, current_batch_size, vram_limit_mb, "
                    "created_at, updated_at) "
                    "VALUES (?, 'pending', ?, ?, ?, ?, ?, ?)",
                    (
                        job_id,
                        len(documents),
                        initial_batch_size,
                        initial_batch_size,
                        vram_limit_mb,
                        now,
                        now,
                    ),
                )
                self._conn.executemany(
                    "INSERT INTO documents (job_id, seq, doc_id, content, "
                    "status, vram_estimate_mb, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)",
                    [
                        (
                            job_id,
                            seq,
                            d["doc_id"],
                            d["content"],
                            d["vram_estimate_mb"],
                            now,
                            now,
                        )
                        for seq, d in enumerate(documents)
                    ],
                )
                self._conn.execute("COMMIT")
            except JobExistsError:
                raise
            except sqlite3.IntegrityError as exc:
                self._conn.execute("ROLLBACK")
                raise JobExistsError(job_id) from exc
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get_job(job_id)  # type: ignore[return-value]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC, job_id DESC "
                "LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def set_job_status(
        self,
        job_id: str,
        status: str,
        *,
        set_started: bool = False,
        set_finished: bool = False,
        last_error: str | None = None,
        clear_error: bool = False,
    ) -> None:
        fields = ["status = ?", "updated_at = ?"]
        params: list[Any] = [status, utc_now_iso()]
        if set_started:
            fields.append("started_at = COALESCE(started_at, ?)")
            params.append(utc_now_iso())
        if set_finished:
            fields.append("finished_at = ?")
            params.append(utc_now_iso())
        if last_error is not None:
            fields.append("last_error = ?")
            params.append(last_error)
        if clear_error:
            fields.append("last_error = NULL")
        params.append(job_id)
        with self._lock:
            self._conn.execute(
                f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ?",
                params,
            )

    def update_current_batch_size(self, job_id: str, size: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET current_batch_size = ?, updated_at = ? "
                "WHERE job_id = ?",
                (size, utc_now_iso(), job_id),
            )

    # --------------------------------------------------------------- documents

    def list_documents(self, job_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, doc_id, status, category, confidence, "
                "vram_estimate_mb, attempts, error_code, error_message, "
                "claimed_by, claimed_at, finished_at, updated_at "
                "FROM documents WHERE job_id = ? ORDER BY seq ASC",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def status_counts(self, job_id: str) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM documents "
                "WHERE job_id = ? GROUP BY status",
                (job_id,),
            ).fetchall()
        counts = {"pending": 0, "processing": 0, "succeeded": 0, "failed": 0}
        for r in rows:
            counts[r["status"]] = r["n"]
        counts["total"] = sum(counts[k] for k in
                              ("pending", "processing", "succeeded", "failed"))
        return counts

    def claim_next_batch(
        self,
        job_id: str,
        max_docs: int,
        owner: str,
        vram_limit_mb: int,
    ) -> list[dict[str, Any]]:
        """Atomically claim up to ``max_docs`` pending documents.

        Returns the claimed rows in submission order and records exactly one
        row in the batch ledger (``dispatch_batch_log``) for the whole
        claim, all in one immediate transaction: document status flips to
        ``processing``, per-document ``attempts`` increments and the batch
        flow commit together, so a write failure rolls everything back and
        no document is left half-claimed.  An empty list means there is
        nothing left to dispatch.

        Batch-internal seqs may skip (terminal rows are excluded) and a
        document re-dispatched after shrinking is simply another batch row
        carrying its seq again.
        """
        batch_id = f"b_{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                rows = self._conn.execute(
                    "SELECT seq FROM documents "
                    "WHERE job_id = ? AND status = 'pending' "
                    "ORDER BY seq ASC LIMIT ?",
                    (job_id, max_docs),
                ).fetchall()
                if not rows:
                    self._conn.execute("COMMIT")
                    return []
                seqs = [r["seq"] for r in rows]
                self._conn.executemany(
                    "UPDATE documents SET status = 'processing', "
                    "attempts = attempts + 1, claimed_by = ?, claimed_at = ?, "
                    "updated_at = ? WHERE job_id = ? AND seq = ? "
                    "AND status = 'pending'",
                    [(owner, now, now, job_id, s) for s in seqs],
                )
                docs = self._conn.execute(
                    "SELECT seq, doc_id, content, vram_estimate_mb, attempts "
                    "FROM documents WHERE job_id = ? AND seq IN "
                    f"({','.join('?' for _ in seqs)}) ORDER BY seq ASC",
                    (job_id, *seqs),
                ).fetchall()
                # One ledger row for the entire batch; per-document detail
                # rides along as an ordered JSON array.
                doc_entries = [
                    {"seq": int(d["seq"]),
                     "estimate_mb": int(d["vram_estimate_mb"])}
                    for d in docs
                ]
                self._conn.execute(
                    "INSERT INTO dispatch_batch_log (job_id, batch_id, "
                    "doc_count, doc_seqs_json, vram_limit_mb, dispatched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        job_id,
                        batch_id,
                        len(doc_entries),
                        json.dumps(doc_entries, ensure_ascii=False),
                        vram_limit_mb,
                        now,
                    ),
                )
                self._conn.execute("COMMIT")
                result = [dict(d) for d in docs]
                for d in result:
                    d["batch_id"] = batch_id
                return result
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_succeeded(
        self, job_id: str, seq: int, category: str, confidence: float
    ) -> None:
        now = utc_now_iso()
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET status = 'succeeded', category = ?, "
                "confidence = ?, error_code = NULL, error_message = NULL, "
                "claimed_by = NULL, finished_at = ?, updated_at = ? "
                "WHERE job_id = ? AND seq = ?",
                (category, confidence, now, now, job_id, seq),
            )

    def mark_failed(
        self, job_id: str, seq: int, error_code: str, error_message: str
    ) -> None:
        now = utc_now_iso()
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET status = 'failed', error_code = ?, "
                "error_message = ?, claimed_by = NULL, finished_at = ?, "
                "updated_at = ? WHERE job_id = ? AND seq = ?",
                (error_code, error_message, now, now, job_id, seq),
            )

    def release_to_pending(
        self, job_id: str, seqs: Iterable[int], owner: str
    ) -> int:
        """Give back claimed-but-unprocessed documents (OOM remainder)."""
        seqs = list(seqs)
        if not seqs:
            return 0
        now = utc_now_iso()
        with self._lock:
            cur = self._conn.execute(
                "UPDATE documents SET status = 'pending', claimed_by = NULL, "
                "claimed_at = NULL, updated_at = ? "
                "WHERE job_id = ? AND claimed_by = ? AND status = 'processing'"
                f" AND seq IN ({','.join('?' for _ in seqs)})",
                (now, job_id, owner, *seqs),
            )
            return cur.rowcount

    def release_owner_processing(self, job_id: str, owner: str) -> int:
        """Rollback hook for an unexpected runner error."""
        now = utc_now_iso()
        with self._lock:
            cur = self._conn.execute(
                "UPDATE documents SET status = 'pending', claimed_by = NULL, "
                "claimed_at = NULL, updated_at = ? "
                "WHERE job_id = ? AND claimed_by = ? "
                "AND status = 'processing'",
                (now, job_id, owner),
            )
            return cur.rowcount

    # ------------------------------------------------------------- restart etc.

    def reset_stale_work(self) -> tuple[int, int]:
        """After a process restart, 'processing' rows are orphaned.

        Terminal rows (succeeded/failed) are intentionally untouched.
        Returns (reset_documents, interrupted_jobs).
        """
        now = utc_now_iso()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                cur = self._conn.execute(
                    "UPDATE documents SET status = 'pending', "
                    "claimed_by = NULL, claimed_at = NULL, updated_at = ? "
                    "WHERE status = 'processing'",
                    (now,),
                )
                reset_docs = cur.rowcount
                cur2 = self._conn.execute(
                    "UPDATE jobs SET status = 'interrupted', updated_at = ? "
                    "WHERE status = 'running'",
                    (now,),
                )
                interrupted_jobs = cur2.rowcount
                self._conn.execute("COMMIT")
                return reset_docs, interrupted_jobs
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def list_dispatches(self, job_id: str) -> list[dict[str, Any]]:
        """Per-document dispatch view, merging legacy and batch ledgers.

        Legacy ``dispatch_log`` rows are returned with their original ids
        and fields untouched.  Each new ``dispatch_batch_log`` row (one per
        batch) is expanded into one entry per document it dispatched, all
        sharing that batch's id.  The merged result is ordered by
        ``(id, seq)`` -- i.e. claim-commit order over the whole database,
        then submission order inside a batch -- which preserves the old
        "one entry per dispatched document, id order" contract.
        """
        with self._lock:
            legacy_rows = self._conn.execute(
                "SELECT id, seq, batch_id, vram_limit_mb, estimate_mb, "
                "dispatched_at FROM dispatch_log "
                "WHERE job_id = ? ORDER BY id ASC, seq ASC",
                (job_id,),
            ).fetchall()
            batch_rows = self._conn.execute(
                "SELECT id, batch_id, doc_count, doc_seqs_json, "
                "vram_limit_mb, dispatched_at FROM dispatch_batch_log "
                "WHERE job_id = ? ORDER BY id ASC",
                (job_id,),
            ).fetchall()

        entries: list[dict[str, Any]] = [dict(r) for r in legacy_rows]
        for row in batch_rows:
            try:
                doc_entries = json.loads(row["doc_seqs_json"])
            except (ValueError, TypeError):  # defensive: never hide the row
                doc_entries = []
            for entry in doc_entries:
                entries.append(
                    {
                        "id": row["id"],
                        "seq": int(entry["seq"]),
                        "batch_id": row["batch_id"],
                        "vram_limit_mb": row["vram_limit_mb"],
                        "estimate_mb": int(entry["estimate_mb"]),
                        "dispatched_at": row["dispatched_at"],
                    }
                )
        entries.sort(key=lambda e: (int(e["id"]), int(e["seq"])))
        return entries

    def list_dispatch_batches(self, job_id: str) -> list[dict[str, Any]]:
        """The physical batch ledger: one row per dispatch batch."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, job_id, batch_id, doc_count, doc_seqs_json, "
                "vram_limit_mb, dispatched_at FROM dispatch_batch_log "
                "WHERE job_id = ? ORDER BY id ASC",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def count_dispatch_batches(self, job_id: str | None = None) -> int:
        """Number of physical batch-ledger rows (whole DB or one job)."""
        with self._lock:
            if job_id is None:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM dispatch_batch_log"
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM dispatch_batch_log "
                    "WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
        return int(row["n"])

    def new_owner(self) -> str:
        return f"run-{uuid.uuid4().hex[:12]}"
