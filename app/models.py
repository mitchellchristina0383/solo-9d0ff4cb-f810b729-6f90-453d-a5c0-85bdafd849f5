"""Pydantic request/response schemas (also drive the /docs UI)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

JobStatus = Literal[
    "pending",
    "running",
    "interrupted",
    "completed",
    "completed_with_failures",
    "error",
]
DocStatus = Literal["pending", "processing", "succeeded", "failed"]


class DocumentIn(BaseModel):
    id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="文档编号（同一作业内唯一）",
        examples=["DOC-001"],
    )
    text: str = Field(
        ...,
        min_length=1,
        description="文档正文；长度决定模拟显存占用，越长越容易触发 OOM",
        examples=["央行公布最新货币政策，银行板块财报预期向好，金融市场反应积极。"],
    )


class JobCreate(BaseModel):
    job_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9_.\-]+$",
        description="可选的原作业号；不传则由服务端生成。重启后用它续跑。",
        examples=["JOB-2026-ARCHIVE-0001"],
    )
    documents: list[DocumentIn] = Field(
        ...,
        min_length=1,
        description="带编号的文本批次；数组顺序即提交顺序，查询结果按此顺序返回",
    )
    batch_size: int = Field(
        default=4,
        ge=1,
        le=64,
        description="初始派发批大小；发生 OOM 后会自动减半重试",
    )
    vram_limit_mb: int | None = Field(
        default=None,
        ge=64,
        le=65536,
        description="可选：本作业专用的显存上限(MB)；不传则使用节点当前的全局可调上限",
    )

    model_config = {
        "json_schema_extra": {
            # singular "example" is what Swagger UI prefills in Request body
            "example": {
                "job_id": "JOB-DEMO-1",
                "batch_size": 4,
                "vram_limit_mb": 4096,
                "documents": [
                    {"id": "DOC-01", "text": "银行发布财报，金融板块上涨，投资预期向好。"},
                    {"id": "DOC-02", "text": "足球联赛决赛今晚开赛，球队进球大战引发体育迷热议。"},
                    {"id": "DOC-03", "text": "芯片算法与人工智能模型快速迭代，软件和数据中心需求增长。"},
                    {"id": "DOC-04", "text": "医院新药物通过疫苗临床试验，医学专家关注疾病防控进展。"},
                ],
            }
        }
    }


class DocumentOut(BaseModel):
    seq: int = Field(..., description="提交顺序下标，从 0 开始")
    doc_id: str = Field(..., description="文档编号")
    status: DocStatus
    category: str | None = Field(None, description="分类结果（成功时）")
    confidence: float | None = None
    vram_estimate_mb: int
    attempts: int = Field(..., description="累计派发次数（缩批重试会增加）")
    error_code: str | None = Field(
        None, description="失败码，例如 document_exceeds_vram_limit"
    )
    error_message: str | None = Field(None, description="失败原因")
    claimed_by: str | None = None
    claimed_at: str | None = None
    finished_at: str | None = None
    updated_at: str | None = None


class Counts(BaseModel):
    total: int
    pending: int
    processing: int
    succeeded: int
    failed: int


class JobOut(BaseModel):
    job_id: str
    status: JobStatus
    total: int
    initial_batch_size: int
    current_batch_size: int = Field(..., description="缩批后的当前批大小")
    vram_limit_mb: int | None = Field(
        None, description="作业级覆盖值；为空时使用节点全局设置"
    )
    effective_vram_limit_mb: int = Field(
        ..., description="本次查询实际生效的显存上限"
    )
    running: bool = Field(..., description="当前 API 进程内是否有续跑任务")
    last_error: str | None = None
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    counts: Counts
    documents: list[DocumentOut]


class JobSummary(BaseModel):
    job_id: str
    status: str
    total: int
    current_batch_size: int
    created_at: str
    finished_at: str | None = None
    counts: Counts
    running: bool = False


class JobList(BaseModel):
    jobs: list[JobSummary]


class ResumeResponse(BaseModel):
    job_id: str
    started: bool = Field(
        ..., description="True=本次调用启动了新的续跑；False=已有运行中的任务"
    )
    running: bool
    status: str
    message: str


class NodeConfig(BaseModel):
    vram_limit_mb: int = Field(..., ge=64, le=65536)
    per_doc_delay_ms: int = Field(..., ge=0, le=60000)

    model_config = {
        "json_schema_extra": {
            "example": {"vram_limit_mb": 512, "per_doc_delay_ms": 80}
        }
    }


class NodeConfigOut(NodeConfig):
    model_resident_mb: int
    doc_base_mb: int
    mb_per_100_chars: int
    cost_formula: str


class DispatchOut(BaseModel):
    id: int
    seq: int
    batch_id: str
    vram_limit_mb: int
    estimate_mb: int
    dispatched_at: str


class DispatchList(BaseModel):
    job_id: str
    dispatch_count: int
    dispatches: list[DispatchOut]


class Health(BaseModel):
    status: str
    db_path: str
    stale_reset_on_startup: dict[str, int] | None = None
