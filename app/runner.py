"""Job orchestration: dispatch batches, shrink on OOM, track one run per job."""

from __future__ import annotations

import asyncio
import logging

from .db import Database
from .node import SimulatedClassificationNode

logger = logging.getLogger("classifier.runner")

ERR_DOCUMENT_TOO_LARGE = "document_exceeds_vram_limit"
ERR_JOB_FAILED = "runner_error"


class RunManager:
    """Owns the asyncio run task for each job id (single API instance).

    The in-process task map prevents two event-loop runs for one job.
    Correctness against duplicate dispatch does *not* rely on this map
    alone: ``Database.claim_next_batch`` is an atomic SQLite transaction
    that only claims pending rows.
    """

    def __init__(self, db: Database, node: SimulatedClassificationNode):
        self.db = db
        self.node = node
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def is_running(self, job_id: str) -> bool:
        task = self._tasks.get(job_id)
        return task is not None and not task.done()

    def active_jobs(self) -> list[str]:
        return [jid for jid, t in self._tasks.items() if not t.done()]

    def request_start(self, job_id: str) -> bool:
        """Start a background run unless one is already active.

        Returns True when a new task was created, False when a live task
        already exists.  Must be called from the event loop thread; the
        check/create section contains no ``await`` so concurrent resume
        requests cannot interleave here.
        """
        existing = self._tasks.get(job_id)
        if existing is not None and not existing.done():
            return False
        task = asyncio.create_task(self.run_job(job_id))
        self._tasks[job_id] = task
        task.add_done_callback(lambda _t, jid=job_id: self._tasks.pop(jid, None))
        return True

    async def run_job(self, job_id: str) -> None:
        owner = self.db.new_owner()
        job = self.db.get_job(job_id)
        if job is None:
            logger.warning("run requested for unknown job %s", job_id)
            return

        self.db.set_job_status(
            job_id, "running", set_started=True, clear_error=True
        )
        batch_size = max(1, int(job["current_batch_size"]))

        try:
            while True:
                job = self.db.get_job(job_id)
                if job is None:
                    return
                settings = self.db.get_node_settings()
                limit_mb = (
                    int(job["vram_limit_mb"])
                    if job["vram_limit_mb"] is not None
                    else int(settings["vram_limit_mb"])
                )

                batch = self.db.claim_next_batch(
                    job_id, batch_size, owner, limit_mb
                )
                if not batch:
                    break

                result = await self.node.classify_batch(batch, limit_mb)

                for item in result.succeeded:
                    self.db.mark_succeeded(
                        job_id,
                        int(item["seq"]),
                        str(item["category"]),
                        float(item["confidence"]),
                    )

                if not result.oom_occurred:
                    # Whole batch classified.  Batch size stays stable; we
                    # never need to grow it for correctness.
                    continue

                oom = result.oom
                assert oom is not None
                succeeded_seqs = {int(i["seq"]) for i in result.succeeded}
                unfinished = [
                    d for d in batch if int(d["seq"]) not in succeeded_seqs
                ]

                if batch_size <= 1:
                    # A single document still does not fit: terminal failure
                    # for this document only.  Nothing to release because
                    # the batch contained exactly this one row.
                    target = unfinished[0] if unfinished else batch[0]
                    seq = int(target["seq"])
                    resident_mb = (
                        oom.required_total_mb
                        - int(target["vram_estimate_mb"])
                    )
                    message = (
                        f"单篇文档 {target['doc_id']} 预估需要 "
                        f"{oom.required_total_mb}MB 显存"
                        f"（节点常驻约 {resident_mb}MB"
                        f" + 本文 {target['vram_estimate_mb']}MB），"
                        f"超过当前上限 {oom.limit_mb}MB；缩批到 1 后仍 OOM，"
                        f"记录为失败，其余文档继续处理。"
                    )
                    self.db.mark_failed(
                        job_id, seq, ERR_DOCUMENT_TOO_LARGE, message
                    )
                    logger.info(
                        "job %s doc seq=%s too large: required=%s limit=%s",
                        job_id, seq, oom.required_total_mb, oom.limit_mb,
                    )
                    continue

                # Give the OOM remainder back and retry with a smaller
                # batch (halved, at least 1).  Partial results are already
                # durable and will never be dispatched again.
                self.db.release_to_pending(
                    job_id, [int(d["seq"]) for d in unfinished], owner
                )
                batch_size = max(1, batch_size // 2)
                self.db.update_current_batch_size(job_id, batch_size)
                logger.info(
                    "job %s OOM at seq=%s; %d partial ok, %d released; "
                    "batch_size shrunk to %d",
                    job_id,
                    oom.next_seq,
                    len(result.succeeded),
                    len(unfinished),
                    batch_size,
                )

            counts = self.db.status_counts(job_id)
            final_status = (
                "completed_with_failures"
                if counts["failed"] > 0
                else "completed"
            )
            self.db.set_job_status(job_id, final_status, set_finished=True)
            logger.info(
                "job %s finished: %s (%s)", job_id, final_status, counts
            )
        except asyncio.CancelledError:
            # Shutting down: orphaned claims are reset to pending on next
            # startup; re-raise so the task actually stops.
            raise
        except Exception as exc:  # persist and allow resume
            logger.exception("job %s runner error", job_id)
            try:
                self.db.release_owner_processing(job_id, owner)
                self.db.set_job_status(
                    job_id,
                    "error",
                    last_error=f"{ERR_JOB_FAILED}: {type(exc).__name__}: {exc}",
                )
            except Exception:  # pragma: no cover
                logger.exception("failed to persist error state for %s", job_id)

    async def wait_for(self, job_id: str, timeout: float | None = None) -> None:
        task = self._tasks.get(job_id)
        if task is None:
            return
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
