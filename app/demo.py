"""Deterministic showcase payload for the one-click /demo/run endpoint."""

from __future__ import annotations

from .models import DocumentIn, JobCreate

# Paddings are deterministic CJK-heavy strings so length -> VRAM estimate is
# stable across runs.  The lengths are chosen against the default 512MB
# limit / 128MB resident model to exercise:
#   batch partial success -> OOM -> shrink -> success,
#   zero-partial OOM -> shrink to 1 -> single-doc permanent failure,
#   remaining documents still processed.

_PAD = "归档分类压力测试文本，包含金融科技医学体育等主题词汇。"


def _long_text(length: int, lead: str) -> str:
    body = (_PAD * (length // len(_PAD) + 1))[:length]
    return f"{lead}。{body}"


def build_demo_job() -> JobCreate:
    docs = [
        DocumentIn(
            id="DEMO-01",
            text="央行下调存款准备金率，银行财报与股票投资板块应声上涨，金融市场预期改善。",
        ),
        DocumentIn(
            id="DEMO-02",
            text="足球联赛决赛夜，主队三球逆转，前锋梅开二度，奥运级体育解说热议比赛。",
        ),
        # ~6000 chars: too heavy after two resident docs in a batch of 4,
        # but fits once the batch is shrunk to 2.
        DocumentIn(id="DEMO-03", text=_long_text(6000, "芯片算法与人工智能模型")),
        DocumentIn(
            id="DEMO-04",
            text="医院与医学研究所发布健康指南，提醒公众规律作息，药物使用需遵医嘱。",
        ),
        # ~11000 chars: estimate + model resident > 512MB even alone,
        # so it becomes a permanent per-document failure.
        DocumentIn(id="DEMO-05", text=_long_text(11000, "超长归档文献无法载入显存")),
        DocumentIn(
            id="DEMO-06",
            text="市政府公布新的选举与政策日程，总统外交讲话聚焦区域合作，议会展开辩论。",
        ),
        DocumentIn(id="DEMO-07", text=_long_text(2500, "数据中心软件负载与算力调度")),
        DocumentIn(
            id="DEMO-08",
            text="新能源汽车与电池工厂投资扩产，产业链财报显示研发投入持续增加。",
        ),
    ]
    return JobCreate(
        job_id="JOB-DEMO-SHRINK",
        documents=docs,
        batch_size=4,
        vram_limit_mb=512,
    )
