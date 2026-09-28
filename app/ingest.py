"""Event validation and idempotent ingestion."""
from __future__ import annotations

import json
import threading

from .models import MAX_EVENT_BYTES, Event, IngestResult, IngestStatus, RejectReason
from .storage import Storage
from .windows import WindowEngine

REQUIRED_FIELDS = ("event_id", "source", "partition", "event_time", "payload")


class BackpressureError(Exception):
    """Raised inside the ingest transaction to roll back an overflow append."""


def _reject(reason: RejectReason, detail: str) -> IngestResult:
    return IngestResult(status=IngestStatus.REJECTED, reason=reason, detail=detail)


class IngestService:
    def __init__(self, storage: Storage, windows: WindowEngine, max_events: int) -> None:
        self._storage = storage
        self._windows = windows
        self._max_events = max_events
        # Serializes the capacity check + append so concurrent writers
        # cannot both pass the check and overflow the log.
        self._ingest_lock = threading.Lock()

    def validate(self, raw_body: bytes) -> Event | IngestResult:
        """Parse and validate a raw request body into an Event, or a rejection."""
        if len(raw_body) > MAX_EVENT_BYTES:
            return _reject(
                RejectReason.PAYLOAD_TOO_LARGE,
                f"body is {len(raw_body)} bytes, limit is {MAX_EVENT_BYTES}",
            )
        try:
            data = json.loads(raw_body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _reject(RejectReason.MALFORMED_JSON, "body is not valid JSON")
        if not isinstance(data, dict):
            return _reject(RejectReason.MALFORMED_JSON, "body must be a JSON object")

        for field in REQUIRED_FIELDS:
            if field not in data:
                return _reject(RejectReason.MISSING_FIELD, f"missing field: {field}")

        event_id, source, payload = data["event_id"], data["source"], data["payload"]
        partition, event_time = data["partition"], data["event_time"]
        if not isinstance(event_id, str) or not event_id:
            return _reject(RejectReason.INVALID_FIELD, "event_id must be a non-empty string")
        if not isinstance(source, str) or not source:
            return _reject(RejectReason.INVALID_FIELD, "source must be a non-empty string")
        # bool is a subclass of int; exclude it explicitly.
        if not isinstance(partition, int) or isinstance(partition, bool) or partition < 0:
            return _reject(RejectReason.INVALID_FIELD, "partition must be an integer >= 0")
        if not isinstance(event_time, int) or isinstance(event_time, bool) or event_time < 0:
            return _reject(RejectReason.INVALID_FIELD, "event_time must be epoch ms >= 0")
        if not isinstance(payload, str):
            return _reject(RejectReason.INVALID_FIELD, "payload must be a string")

        return Event(
            event_id=event_id,
            source=source,
            partition=partition,
            event_time=event_time,
            payload=payload,
        )

    def ingest(self, event: Event) -> IngestResult:
        """Append to the log (deduplicated) and update window stats atomically.

        The log append, the dedup check and the window update commit in one
        transaction: a duplicate delivery is a no-op, and a crash mid-write
        cannot leave the log and the aggregates diverged.
        """
        with self._ingest_lock:
            try:
                with self._storage.transaction():
                    inserted, seq = self._storage.append_event(event)
                    if not inserted:
                        # Idempotent replay: safe even when the log is full.
                        return IngestResult(status=IngestStatus.DUPLICATE, seq=seq)
                    if self._storage.event_count() > self._max_events:
                        raise BackpressureError
                    self._windows.on_event(event)
                    return IngestResult(status=IngestStatus.ACCEPTED, seq=seq)
            except BackpressureError:
                return _reject(
                    RejectReason.BACKPRESSURE,
                    f"event log is full ({self._max_events} events); retry later",
                )
