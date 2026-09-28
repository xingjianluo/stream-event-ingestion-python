"""HTTP API wiring ingestion, consumption, offsets and window queries."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from .ingest import IngestService
from .models import IngestResult, IngestStatus, RejectReason
from .offsets import OffsetError, OffsetService
from .storage import Storage

#: HTTP status per ingest outcome, so clients can distinguish them
#: without parsing the body.
_STATUS_BY_RESULT = {
    IngestStatus.ACCEPTED: 201,
    IngestStatus.DUPLICATE: 200,
}


def _ingest_http_status(result: IngestResult) -> int:
    if result.status in _STATUS_BY_RESULT:
        return _STATUS_BY_RESULT[result.status]
    assert result.reason is not None
    return {
        RejectReason.PAYLOAD_TOO_LARGE: 413,
        RejectReason.BACKPRESSURE: 503,
    }.get(result.reason, 400)


def _result_body(result: IngestResult) -> dict[str, Any]:
    body: dict[str, Any] = {"status": result.status.value}
    if result.seq is not None:
        body["seq"] = result.seq
    if result.reason is not None:
        body["reason"] = result.reason.value
    if result.detail:
        body["detail"] = result.detail
    return body


def create_app(
    storage: Storage,
    ingest: IngestService,
    offsets: OffsetService,
) -> FastAPI:
    app = FastAPI(title="stream-event-ingestion")

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/v1/events")
    async def post_event(request: Request) -> JSONResponse:
        raw = await request.body()
        parsed = ingest.validate(raw)
        if isinstance(parsed, IngestResult):
            return JSONResponse(_result_body(parsed), status_code=_ingest_http_status(parsed))
        result = ingest.ingest(parsed)
        return JSONResponse(_result_body(result), status_code=_ingest_http_status(result))

    @app.get("/v1/events")
    def get_events(
        after_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        """Consume from the simulated channel: entries with seq > after_seq."""
        return {"events": storage.read_events(after_seq, limit)}

    @app.post("/v1/offsets")
    def post_offset(body: dict[str, Any]) -> JSONResponse:
        consumer, source = body.get("consumer"), body.get("source")
        partition, offset = body.get("partition"), body.get("offset")
        if not isinstance(consumer, str) or not consumer:
            return JSONResponse(
                {"reason": RejectReason.INVALID_FIELD.value,
                 "detail": "consumer must be a non-empty string"}, status_code=400)
        if not isinstance(source, str) or not source:
            return JSONResponse(
                {"reason": RejectReason.INVALID_FIELD.value,
                 "detail": "source must be a non-empty string"}, status_code=400)
        if not isinstance(partition, int) or isinstance(partition, bool) or partition < 0:
            return JSONResponse(
                {"reason": RejectReason.INVALID_FIELD.value,
                 "detail": "partition must be an integer >= 0"}, status_code=400)
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            return JSONResponse(
                {"reason": RejectReason.INVALID_FIELD.value,
                 "detail": "offset must be an integer >= 0"}, status_code=400)
        try:
            committed = offsets.commit(consumer, source, partition, offset)
        except OffsetError as exc:
            return JSONResponse(
                {"reason": exc.reason.value, "detail": exc.detail}, status_code=409)
        return JSONResponse({"committed": committed}, status_code=200)

    @app.get("/v1/offsets")
    def get_offset(consumer: str, source: str, partition: int = Query(ge=0)) -> JSONResponse:
        value = offsets.get(consumer, source, partition)
        if value is None:
            return JSONResponse({"detail": "no offset committed"}, status_code=404)
        return JSONResponse({"offset": value}, status_code=200)

    @app.get("/v1/windows")
    def get_windows(source: str | None = None) -> dict:
        windows = storage.list_windows(source)
        return {
            "windows": [
                {
                    "source": w.source,
                    "window_start": w.window_start,
                    "window_end": w.window_end,
                    "count": w.count,
                    "late_count": w.late_count,
                    "closed": w.closed,
                }
                for w in windows
            ]
        }

    return app
