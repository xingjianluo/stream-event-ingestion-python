"""公共 fixture 与测试日志工具。

每个判定都打印输入与依据，方便在单测日志里解释结果。
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.clock import FakeClock
from app.config import Settings
from app.service import IngestionService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("tests")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(now_ms=1_000_000)


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        window_size_seconds=10,
        allowed_lateness_seconds=30,
        max_total_events=1000,
        max_events_per_batch=50,
        max_inflight_requests=4,
    )


@pytest.fixture
def service(settings, clock) -> IngestionService:
    return IngestionService(settings=settings, clock=clock)


@pytest.fixture
def client(service) -> TestClient:
    return TestClient(create_app(service))


def log_decision(case: str, payload: object, decision: object) -> None:
    """打印“输入 + 判定依据”，用于单测日志解释结果。"""
    logger.info("用例=%s 输入=%s 判定=%s", case, payload, decision)
