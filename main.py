"""Service entrypoint: build components and run the HTTP server."""
from __future__ import annotations

import os

import uvicorn

from app.api import create_app
from app.ingest import IngestService
from app.offsets import OffsetService
from app.storage import Storage
from app.windows import WindowEngine

DB_PATH = os.environ.get("SEI_DB_PATH", ":memory:")
WINDOW_SIZE_MS = int(os.environ.get("SEI_WINDOW_SIZE_MS", "60000"))
ALLOWED_LATENESS_MS = int(os.environ.get("SEI_ALLOWED_LATENESS_MS", "10000"))
MAX_EVENTS = int(os.environ.get("SEI_MAX_EVENTS", "100000"))


def build() -> "object":
    storage = Storage(DB_PATH)
    storage.open()
    windows = WindowEngine(storage, WINDOW_SIZE_MS, ALLOWED_LATENESS_MS)
    ingest = IngestService(storage, windows, MAX_EVENTS)
    offsets = OffsetService(storage)
    return create_app(storage, ingest, offsets)


app = build()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
