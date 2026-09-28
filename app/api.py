"""FastAPI 入口：单条写入、批量写入、拉取、提交位点、窗口查询。

所有写入入口（单条 / 批量）走同一套 service 逻辑，保证不同入口判定一致；
背压通过 try_enter/leave 包裹，容量吃紧时返回 503 BACKPRESSURE。
"""

from __future__ import annotations

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from app.errors import MALFORMED_JSON, OFFSET_JUMP, ServiceError, error_body
from app.service import IngestionService
from app.validation import parse_body, parse_events


def create_app(service: IngestionService | None = None) -> FastAPI:
    app = FastAPI(title="stream-event-ingestion", version="0.1.0")
    svc = service or IngestionService()

    @app.exception_handler(ServiceError)
    async def _service_error_handler(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.error.http_status,
            content=error_body(exc.error, exc.detail),
        )

    async def _read_body(request: Request) -> bytes:
        raw = await request.body()
        if raw == b"" or raw is None:
            raise ServiceError(MALFORMED_JSON, "请求体为空")
        return raw

    def _ingest_payload(raw: bytes, batch_entry: bool) -> dict:
        # 在途名额（背压）包住解析+写入
        svc.try_enter()
        try:
            payload = parse_body(raw, svc.settings)
            events = parse_events(payload, svc.settings, svc.clock.now_ms())
            result = svc.ingest_batch(events)
        finally:
            svc.leave()
        if batch_entry:
            return {
                "accepted": result.accepted,
                "duplicate": result.duplicate,
                "rejected": result.rejected,
                "results": [_event_result_dict(r) for r in result.results],
            }
        first = result.results[0]
        return _event_result_dict(first)

    @app.get("/v1/health")
    async def health() -> dict:
        return {"status": "ok", **svc.stats()}

    @app.post("/v1/events")
    async def ingest_one(request: Request) -> dict:
        return _ingest_payload(await _read_body(request), batch_entry=False)

    @app.post("/v1/events/batch")
    async def ingest_batch(request: Request) -> dict:
        return _ingest_payload(await _read_body(request), batch_entry=True)

    @app.get("/v1/sources/{source}/partitions/{partition}/events")
    async def fetch_events(
        source: str,
        partition: int,
        consumer_id: str = Query(..., min_length=1),
        max_events: int = Query(100, ge=1, le=1000),
    ) -> dict:
        result = svc.fetch(source, partition, consumer_id, max_events)
        return {
            "source": result.source,
            "partition": result.partition,
            "consumer_id": result.consumer_id,
            "from_offset": result.from_offset,
            "events": [
                {
                    "offset": e.offset,
                    "event_id": e.event_id,
                    "event_time": e.event_time,
                    "value": e.value,
                    "ingress_time": e.ingress_time,
                }
                for e in result.events
            ],
        }

    @app.post("/v1/sources/{source}/partitions/{partition}/commit")
    async def commit_offset(
        source: str,
        partition: int,
        request: Request,
        consumer_id: str = Query(..., min_length=1),
    ) -> dict:
        raw = await _read_body(request)
        payload = parse_body(raw, svc.settings)
        if not isinstance(payload, dict) or "offset" not in payload:
            raise ServiceError(OFFSET_JUMP, "提交体必须是包含 offset 字段的对象，例如 {'offset': 0}")
        result = svc.commit(source, partition, consumer_id, payload["offset"])
        return {
            "source": result.source,
            "partition": result.partition,
            "consumer_id": result.consumer_id,
            "previous_offset": result.previous_offset,
            "committed_offset": result.committed_offset,
            "status": result.status,
        }

    @app.get("/v1/sources/{source}/windows")
    async def get_windows(source: str) -> dict:
        views = svc.window_views(source)
        return {
            "source": source,
            "window_size_seconds": svc.settings.window_size_seconds,
            "allowed_lateness_seconds": svc.settings.allowed_lateness_seconds,
            "now_ms": svc.clock.now_ms(),
            "windows": [
                {
                    "window_start": w.window_start,
                    "window_end": w.window_end,
                    "state": w.state,
                    "count": w.count,
                    "total_value": w.total_value,
                    "late_events": w.late_events,
                    "distinct_event_keys": w.distinct_event_keys,
                }
                for w in views
            ],
        }

    return app


def _event_result_dict(r) -> dict:
    return {
        "status": r.status,
        "offset": r.offset,
        "reject_code": r.reject_code,
        "reason": r.reason,
        "window_start": r.window_start,
        "lateness": r.lateness,
    }
