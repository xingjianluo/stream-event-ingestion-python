"""Shared fixtures and a test logger that prints inputs and verdict basis."""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.ingest import IngestService
from app.offsets import OffsetService
from app.storage import Storage
from app.windows import WindowEngine

WINDOW_SIZE_MS = 60_000
ALLOWED_LATENESS_MS = 10_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    force=True,
)
log = logging.getLogger("sei.test")


def log_case(case: str, inputs: dict, basis: str, observed: dict) -> None:
    """Print the case inputs and the judgment basis into the test log."""
    log.info("CASE %s | inputs=%s | basis=%s | observed=%s", case, inputs, basis, observed)


@pytest.fixture()
def storage():
    s = Storage(":memory:")
    s.open()
    yield s
    s.close()


@pytest.fixture()
def client(storage):
    app = create_app(
        storage,
        IngestService(storage, WindowEngine(storage, WINDOW_SIZE_MS, ALLOWED_LATENESS_MS), 1000),
        OffsetService(storage),
    )
    return TestClient(app)


def make_event(event_id: str, event_time: int, source: str = "s", partition: int = 0,
               payload: str = "x") -> dict:
    return {
        "event_id": event_id,
        "source": source,
        "partition": partition,
        "event_time": event_time,
        "payload": payload,
    }
