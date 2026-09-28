"""Core type definitions shared by ingestion, offsets and windowing."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

#: Maximum accepted size of a single event payload (bytes, UTF-8 encoded).
MAX_EVENT_BYTES = 16 * 1024


class RejectReason(str, Enum):
    """Machine-distinguishable reasons for rejecting a request."""

    MALFORMED_JSON = "malformed_json"
    MISSING_FIELD = "missing_field"
    INVALID_FIELD = "invalid_field"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    BACKPRESSURE = "backpressure"
    OFFSET_REGRESSION = "offset_regression"
    OFFSET_GAP = "offset_gap"


class IngestStatus(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass(frozen=True)
class Event:
    """A validated event ready to be appended to the local log."""

    event_id: str
    source: str
    partition: int
    event_time: int  # event-time, epoch milliseconds
    payload: str

    @property
    def dedup_key(self) -> tuple[str, int, str]:
        """Identity of an event: same id from different sources/partitions differs."""
        return (self.source, self.partition, self.event_id)


@dataclass(frozen=True)
class IngestResult:
    status: IngestStatus
    seq: int | None = None        # log sequence number when accepted/duplicate
    reason: RejectReason | None = None
    detail: str = ""


@dataclass(frozen=True)
class WindowStat:
    """Aggregated counters for one tumbling event-time window."""

    source: str
    window_start: int
    window_end: int
    count: int
    late_count: int  # events that arrived after the window closed
    closed: bool
