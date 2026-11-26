"""Application configuration.

All settings can be overridden through environment variables so the same
image works in Docker Compose and in local self-tests.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    db_path: str
    default_vram_limit_mb: int
    default_batch_size: int
    default_delay_ms: int
    # Toy VRAM cost model (documented in README):
    #   batch_total = model_resident_mb
    #                 + sum(doc_base_mb + ceil(len(text) * mb_per_100_chars / 100))
    model_resident_mb: int
    doc_base_mb: int
    mb_per_100_chars: int

    @staticmethod
    def from_env() -> Settings:
        return Settings(
            db_path=os.environ.get("DB_PATH", "data/classifier.db"),
            default_vram_limit_mb=_get_int("VRAM_LIMIT_MB", 512),
            default_batch_size=max(1, _get_int("BATCH_SIZE", 4)),
            default_delay_ms=max(0, _get_int("NODE_DELAY_MS", 80)),
            model_resident_mb=_get_int("MODEL_RESIDENT_MB", 128),
            doc_base_mb=_get_int("DOC_BASE_MB", 48),
            mb_per_100_chars=_get_int("MB_PER_100_CHARS", 4),
        )

    def ensure_data_dir(self) -> None:
        parent = Path(self.db_path).expanduser().parent
        parent.mkdir(parents=True, exist_ok=True)
