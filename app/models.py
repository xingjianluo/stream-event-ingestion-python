"""跨模块共享的数据类型（请求/记录/响应视图）。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IngestEvent:
    """一次写入请求里的单条事件（已通过基础字段校验）。"""

    source: str
    partition: int
    event_id: str
    event_time: int
    value: int


@dataclass(frozen=True)
class EventRecord:
    """写入成功后落入本地分区日志的记录。"""

    source: str
    partition: int
    event_id: str
    event_time: int
    value: int
    offset: int
    ingress_time: int
    duplicate: bool = False


@dataclass(frozen=True)
class EventResult:
    status: str  # "ACCEPTED" | "DUPLICATE" | "REJECTED"
    offset: int | None
    reject_code: str | None = None
    reason: str | None = None
    window_start: int | None = None
    lateness: bool | None = None


@dataclass(frozen=True)
class BatchResult:
    accepted: int
    duplicate: int
    rejected: int
    results: list[EventResult]


@dataclass(frozen=True)
class CommitResult:
    source: str
    partition: int
    consumer_id: str
    previous_offset: int | None
    committed_offset: int
    status: str  # "COMMITTED" | "ALREADY_COMMITTED"


@dataclass(frozen=True)
class FetchResult:
    source: str
    partition: int
    consumer_id: str
    from_offset: int
    events: list[EventRecord]


@dataclass(frozen=True)
class WindowView:
    source: str
    window_start: int
    window_end: int
    state: str  # "OPEN" | "SEALED"
    count: int
    total_value: int
    late_events: int
    distinct_event_keys: int
