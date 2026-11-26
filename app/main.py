"""FastAPI single-instance service: numbered batch classification with
OOM-driven batch shrinking, SQLite durability and restart resume.

Operate from the Swagger UI at ``/docs``.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

from .config import Settings
from .db import Database, JobExistsError
from .demo import build_demo_job
from .models import (
    DispatchList,
    Health,
    JobCreate,
    JobList,
    JobOut,
    NodeConfig,
    NodeConfigOut,
    ResumeResponse,
)
from .node import SimulatedClassificationNode, estimate_vram_mb
from .runner import RunManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("classifier.api")


def _serialize_job(
    db: Database, manager: RunManager | None, job: dict[str, Any]
) -> dict[str, Any]:
    documents = db.list_documents(job["job_id"])
    counts = db.status_counts(job["job_id"])
    settings = db.get_node_settings()
    effective_limit = (
        int(job["vram_limit_mb"])
        if job["vram_limit_mb"] is not None
        else int(settings["vram_limit_mb"])
    )
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "total": job["total"],
        "initial_batch_size": job["initial_batch_size"],
        "current_batch_size": job["current_batch_size"],
        "vram_limit_mb": job["vram_limit_mb"],
        "effective_vram_limit_mb": effective_limit,
        "running": manager.is_running(job["job_id"]) if manager else False,
        "last_error": job["last_error"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"],
        "started_at": job["started_at"],
        "finished_at": job["finished_at"],
        "counts": counts,
        "documents": documents,
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.ensure_data_dir()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(settings.db_path)
        db.seed_node_settings(
            settings.default_vram_limit_mb, settings.default_delay_ms
        )
        # Restart recovery: orphaned processing rows become pending again;
        # terminal rows are left untouched.
        reset_docs, interrupted_jobs = db.reset_stale_work()
        if reset_docs or interrupted_jobs:
            logger.info(
                "startup recovery: %s stale document(s) -> pending, "
                "%s job(s) -> interrupted",
                reset_docs,
                interrupted_jobs,
            )
        node = SimulatedClassificationNode(
            db, model_resident_mb=settings.model_resident_mb
        )
        manager = RunManager(db, node)
        app.state.settings = settings
        app.state.db = db
        app.state.node = node
        app.state.manager = manager
        app.state.startup_reset = {
            "reset_documents": reset_docs,
            "interrupted_jobs": interrupted_jobs,
        }
        try:
            yield
        finally:
            db.close()

    app = FastAPI(
        title="归档文档分类与显存缩批续跑服务",
        description=(
            "接收带编号的文本批次，由本地模拟分类节点推理；节点可能先返回部分"
            "结果再报显存不足(OOM)，服务会把未完成文档缩成更小批次重试，单篇"
            "仍超限则只记录该篇失败。SQLite 持久化作业与逐篇结果，重启后可用"
            "原作业号调用 `/jobs/{job_id}/resume` 续跑。\n\n"
            "**快速体验**：依次调用 `POST /demo/run` → "
            "`GET /jobs/{job_id}`（job_id 用 `JOB-DEMO-SHRINK`）→ "
            "`GET /jobs/{job_id}/dispatches`。"
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    # ------------------------------------------------------------ meta routes

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/health", response_model=Health, tags=["meta"])
    async def health(request: Request) -> dict[str, Any]:
        return {
            "status": "ok",
            "db_path": settings.db_path,
            "stale_reset_on_startup": request.app.state.startup_reset,
        }

    # ------------------------------------------------------------ node admin

    @app.get(
        "/admin/node",
        response_model=NodeConfigOut,
        tags=["simulated-node"],
        summary="查看模拟分类节点的显存上限 / 延迟与显存公式",
    )
    async def get_node_config(request: Request) -> dict[str, Any]:
        cfg = request.app.state.db.get_node_settings()
        return {
            **cfg,
            "model_resident_mb": settings.model_resident_mb,
            "doc_base_mb": settings.doc_base_mb,
            "mb_per_100_chars": settings.mb_per_100_chars,
            "cost_formula": (
                "batch = model_resident_mb + "
                "sum(doc_base_mb + ceil(len(text) * mb_per_100_chars / 100))"
            ),
        }

    @app.put(
        "/admin/node",
        response_model=NodeConfigOut,
        tags=["simulated-node"],
        summary="调整模拟节点显存上限（运行中也立即生效）和每篇推理延迟",
    )
    async def put_node_config(
        body: NodeConfig, request: Request
    ) -> dict[str, Any]:
        cfg = request.app.state.db.update_node_settings(
            body.vram_limit_mb, body.per_doc_delay_ms
        )
        return {
            **cfg,
            "model_resident_mb": settings.model_resident_mb,
            "doc_base_mb": settings.doc_base_mb,
            "mb_per_100_chars": settings.mb_per_100_chars,
            "cost_formula": (
                "batch = model_resident_mb + "
                "sum(doc_base_mb + ceil(len(text) * mb_per_100_chars / 100))"
            ),
        }

    # ------------------------------------------------------------------ jobs

    def _create_and_start(
        request: Request, payload: JobCreate, job_id: str
    ) -> dict[str, Any]:
        db: Database = request.app.state.db
        manager: RunManager = request.app.state.manager

        seen: set[str] = set()
        for d in payload.documents:
            if d.id in seen:
                raise HTTPException(
                    status_code=422,
                    detail=f"文档编号重复: {d.id}（同一作业内 id 必须唯一）",
                )
            seen.add(d.id)

        documents = [
            {
                "doc_id": d.id,
                "content": d.text,
                "vram_estimate_mb": estimate_vram_mb(
                    d.text,
                    doc_base_mb=settings.doc_base_mb,
                    mb_per_100_chars=settings.mb_per_100_chars,
                ),
            }
            for d in payload.documents
        ]
        try:
            job = db.create_job(
                job_id=job_id,
                documents=documents,
                initial_batch_size=payload.batch_size,
                vram_limit_mb=payload.vram_limit_mb,
            )
        except JobExistsError:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "job_id_exists",
                    "job_id": job_id,
                    "hint": "可直接调用 GET /jobs/"
                    f"{job_id} 查询，或 POST /jobs/{job_id}/resume 续跑",
                },
            )

        manager.request_start(job_id)
        return _serialize_job(db, manager, job)

    @app.post(
        "/jobs",
        response_model=JobOut,
        status_code=201,
        tags=["jobs"],
        summary="提交带编号的文本批次（创建后自动开始推理）",
    )
    async def create_job(payload: JobCreate, request: Request) -> dict[str, Any]:
        job_id = payload.job_id or f"JOB-{uuid.uuid4().hex[:16]}"
        return _create_and_start(request, payload, job_id)

    @app.post(
        "/demo/run",
        response_model=JobOut,
        status_code=201,
        tags=["demo"],
        summary="一键生成演示作业：部分成功 → 缩批 → 单篇超限失败 → 其余完成",
        description=(
            "生成固定的 8 篇演示文档（job_id=JOB-DEMO-SHRINK）。"
            "若该作业号已存在则直接续跑并返回当前状态。"
        ),
    )
    async def run_demo(request: Request) -> dict[str, Any]:
        payload = build_demo_job()
        db: Database = request.app.state.db
        existing = db.get_job("JOB-DEMO-SHRINK")
        if existing is not None:
            request.app.state.manager.request_start("JOB-DEMO-SHRINK")
            return _serialize_job(db, request.app.state.manager, existing)
        return _create_and_start(request, payload, "JOB-DEMO-SHRINK")

    @app.get(
        "/jobs",
        response_model=JobList,
        tags=["jobs"],
        summary="列出最近的作业及其计数",
    )
    async def list_jobs(request: Request) -> dict[str, Any]:
        db: Database = request.app.state.db
        manager: RunManager = request.app.state.manager
        jobs = db.list_jobs()
        return {
            "jobs": [
                {
                    "job_id": j["job_id"],
                    "status": j["status"],
                    "total": j["total"],
                    "current_batch_size": j["current_batch_size"],
                    "created_at": j["created_at"],
                    "finished_at": j["finished_at"],
                    "counts": db.status_counts(j["job_id"]),
                    "running": manager.is_running(j["job_id"]),
                }
                for j in jobs
            ]
        }

    @app.get(
        "/jobs/{job_id}",
        response_model=JobOut,
        tags=["jobs"],
        summary="按提交顺序查询每篇文档的进度、分类结果或失败原因",
    )
    async def get_job(job_id: str, request: Request) -> dict[str, Any]:
        db: Database = request.app.state.db
        job = db.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"作业不存在: {job_id}")
        return _serialize_job(db, request.app.state.manager, job)

    @app.post(
        "/jobs/{job_id}/resume",
        response_model=ResumeResponse,
        tags=["jobs"],
        summary="按原作业号续跑：已有结果/失败的文档不会重新派发，并发调用安全",
    )
    async def resume_job(job_id: str, request: Request) -> dict[str, Any]:
        db: Database = request.app.state.db
        manager: RunManager = request.app.state.manager
        job = db.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"作业不存在: {job_id}")

        already_running = manager.is_running(job_id)
        if already_running:
            return {
                "job_id": job_id,
                "started": False,
                "running": True,
                "status": job["status"],
                "message": "该作业已有续跑任务在执行；未重复派发。",
            }

        counts = db.status_counts(job_id)
        if counts["pending"] == 0 and counts["processing"] == 0:
            return {
                "job_id": job_id,
                "started": False,
                "running": False,
                "status": job["status"],
                "message": (
                    "所有文档均已到达终态"
                    f"（成功 {counts['succeeded']} / 失败 {counts['failed']}），"
                    "没有需要续跑的文档。"
                ),
            }

        started = manager.request_start(job_id)
        return {
            "job_id": job_id,
            "started": started,
            "running": started or manager.is_running(job_id),
            "status": "running",
            "message": "已按原作业号启动续跑；终态文档不会重新派发。",
        }

    @app.get(
        "/jobs/{job_id}/dispatches",
        response_model=DispatchList,
        tags=["jobs"],
        summary="查看逐篇派发流水（核对缩批与重复派发的证据）",
    )
    async def list_dispatches(job_id: str, request: Request) -> dict[str, Any]:
        db: Database = request.app.state.db
        if db.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail=f"作业不存在: {job_id}")
        rows = db.list_dispatches(job_id)
        return {
            "job_id": job_id,
            "dispatch_count": len(rows),
            "dispatches": rows,
        }

    return app


app = create_app()
