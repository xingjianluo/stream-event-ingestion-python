"""本地启动入口：uvicorn main:app，默认 127.0.0.1:8000。"""

from __future__ import annotations

import os

import uvicorn

from app.api import create_app
from app.config import load_settings
from app.service import IngestionService

settings = load_settings()
service = IngestionService(settings=settings)
app = create_app(service)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
    )
