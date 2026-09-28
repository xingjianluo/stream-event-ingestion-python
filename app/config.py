"""运行配置。所有阈值集中在这里，方便本地验证与测试注入。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path("data")
    window_size_seconds: int = 10
    allowed_lateness_seconds: int = 30
    max_body_bytes: int = 65536
    max_event_id_length: int = 128
    max_events_per_batch: int = 100
    max_total_events: int = 10000
    max_consumers: int = 100
    max_inflight_requests: int = 128


def load_settings() -> Settings:
    """从环境变量读取配置（DATA_DIR / 各阈值），缺省使用默认值。"""
    int_env = lambda name, default: int(os.environ.get(name, default))  # noqa: E731
    return Settings(
        data_dir=Path(os.environ.get("DATA_DIR", "data")),
        window_size_seconds=int_env("WINDOW_SIZE_SECONDS", 10),
        allowed_lateness_seconds=int_env("ALLOWED_LATENESS_SECONDS", 30),
        max_body_bytes=int_env("MAX_BODY_BYTES", 65536),
        max_event_id_length=int_env("MAX_EVENT_ID_LENGTH", 128),
        max_events_per_batch=int_env("MAX_EVENTS_PER_BATCH", 100),
        max_total_events=int_env("MAX_TOTAL_EVENTS", 10000),
        max_consumers=int_env("MAX_CONSUMERS", 100),
        max_inflight_requests=int_env("MAX_INFLIGHT_REQUESTS", 128),
    )
