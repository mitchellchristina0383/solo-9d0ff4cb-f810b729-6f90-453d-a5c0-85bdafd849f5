"""Local simulated classification node.

VRAM cost model (deliberately simple and fully observable)::

    batch_vram_mb = model_resident_mb
                    + sum(estimate_vram_mb(text_i))
    estimate_vram_mb(text) = doc_base_mb
                             + ceil(len(text) * mb_per_100_chars / 100)

The node processes a batch strictly in submission order.  Before processing
each document it checks whether the document fits on top of the model
resident memory and the already-processed documents.  If it does not fit,
the node returns the partial classifications produced so far together with
an ``oom`` descriptor -- exactly the "partial result, then out of memory"
behaviour the retry logic has to handle.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import dataclass, field
from typing import Any

# Tiny deterministic keyword classifier.  The point is the batching /
# recovery machinery, not model quality.
KEYWORD_CATEGORIES: list[tuple[str, tuple[str, ...]]] = [
    (
        "finance",
        ("股票", "基金", "财报", "银行", "投资", "金融", "经济",
         "stock", "finance", "bank", "earnings"),
    ),
    (
        "technology",
        ("芯片", "算法", "软件", "人工智能", "模型", "代码", "数据",
         "tech", "software", "algorithm", "chip", "ai"),
    ),
    (
        "health",
        ("医学", "健康", "医院", "药物", "疫苗", "疾病",
         "health", "medical", "hospital", "drug"),
    ),
    (
        "sports",
        ("比赛", "球队", "进球", "奥运", "联赛", "体育",
         "sport", "match", "goal", "olympic"),
    ),
    (
        "politics",
        ("政府", "选举", "政策", "总统", "议会", "外交",
         "election", "government", "president", "policy"),
    ),
]
FALLBACK_CATEGORIES = [c for c, _ in KEYWORD_CATEGORIES]


def estimate_vram_mb(
    text: str, *, doc_base_mb: int, mb_per_100_chars: int
) -> int:
    return doc_base_mb + math.ceil(
        len(text) * mb_per_100_chars / 100.0
    )


def classify_text(text: str, doc_id: str) -> tuple[str, float]:
    lowered = text.lower()
    best_category: str | None = None
    best_hits = 0
    for category, keywords in KEYWORD_CATEGORIES:
        hits = sum(1 for kw in keywords if kw.lower() in lowered)
        if hits > best_hits:
            best_hits = hits
            best_category = category
    if best_category is None:
        digest = hashlib.sha256(doc_id.encode("utf-8")).digest()
        idx = digest[0] % len(FALLBACK_CATEGORIES)
        best_category = FALLBACK_CATEGORIES[idx]
        confidence = 0.70 + (digest[1] % 25) / 100.0
    else:
        digest = hashlib.sha256(
            (doc_id + best_category).encode("utf-8")
        ).digest()
        confidence = 0.82 + (digest[0] % 17) / 100.0  # 0.82 .. 0.98
    return best_category, round(min(confidence, 0.99), 4)


@dataclass
class OOMInfo:
    error_code: str = "node_out_of_memory"
    limit_mb: int = 0
    used_before_mb: int = 0
    required_total_mb: int = 0
    next_seq: int = -1
    next_doc_id: str = ""
    remaining_seqs: list[int] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "limit_mb": self.limit_mb,
            "used_before_mb": self.used_before_mb,
            "required_total_mb": self.required_total_mb,
            "next_seq": self.next_seq,
            "next_doc_id": self.next_doc_id,
            "remaining_seqs": self.remaining_seqs,
        }


@dataclass
class NodeBatchResult:
    """Return value of one node dispatch.

    ``succeeded`` contains one dict per classified document; ``oom`` is set
    when the node ran out of memory at ``oom.next_seq``.  Documents from
    that seq onwards are absent from ``succeeded``.
    """

    succeeded: list[dict[str, Any]]
    oom: OOMInfo | None = None

    @property
    def oom_occurred(self) -> bool:
        return self.oom is not None


class SimulatedClassificationNode:
    """The fake GPU classification node.

    Memory limit and per-document delay are read from the database-backed
    settings on every dispatch, so ``PUT /admin/node`` adjusts behaviour
    immediately without a restart.
    """

    def __init__(self, db: Any, *, model_resident_mb: int):
        self.db = db
        self.model_resident_mb = model_resident_mb

    async def classify_batch(
        self, docs: list[dict[str, Any]], vram_limit_mb: int
    ) -> NodeBatchResult:
        settings = self.db.get_node_settings()
        delay_s = max(0, settings["per_doc_delay_ms"]) / 1000.0

        used = self.model_resident_mb
        done: list[dict[str, Any]] = []

        for index, doc in enumerate(docs):
            estimate = int(doc["vram_estimate_mb"])
            required_total = used + estimate
            if required_total > vram_limit_mb:
                oom = OOMInfo(
                    limit_mb=vram_limit_mb,
                    used_before_mb=used,
                    required_total_mb=required_total,
                    next_seq=int(doc["seq"]),
                    next_doc_id=str(doc["doc_id"]),
                    remaining_seqs=[int(d["seq"]) for d in docs[index:]],
                )
                return NodeBatchResult(succeeded=done, oom=oom)

            # Simulate inference time, but only for documents that actually
            # fit and therefore get processed.
            if delay_s:
                await asyncio.sleep(delay_s)

            category, confidence = classify_text(
                doc["content"], str(doc["doc_id"])
            )
            done.append(
                {
                    "seq": int(doc["seq"]),
                    "doc_id": str(doc["doc_id"]),
                    "category": category,
                    "confidence": confidence,
                }
            )
            used += estimate

        return NodeBatchResult(succeeded=done, oom=None)
