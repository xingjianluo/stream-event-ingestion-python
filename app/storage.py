"""基于本地文件 JSON 快照的存储，模拟消息通道（分区日志）与状态库。

- events[source][partition] 是只追加的分区日志，每条记录带 offset；
- seen 保存已计入的事件键（source|partition|event_id），用于幂等；
- windows 保存按事件时间的窗口聚合（含已折叠键集合，保证重放不重算）；
- offsets[source][partition][consumer_id] 保存消费位点；
- 每次变更后原子写盘（临时文件 + os.replace），保证重启后状态不丢、不重。
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.models import EventRecord

_STATE_VERSION = 1


@dataclass
class WindowState:
    source: str
    window_start: int
    window_end: int
    count: int = 0
    total_value: int = 0
    late_events: int = 0
    event_keys: set[str] = field(default_factory=set)
    folded: set[str] = field(default_factory=set)  # 已折叠键，保证内部重放也只计一次

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "count": self.count,
            "total_value": self.total_value,
            "late_events": self.late_events,
            "event_keys": sorted(self.event_keys),
            "folded": sorted(self.folded),
        }

    @classmethod
    def from_jsonable(cls, data: dict[str, Any]) -> "WindowState":
        return cls(
            source=data["source"],
            window_start=data["window_start"],
            window_end=data["window_end"],
            count=data["count"],
            total_value=data["total_value"],
            late_events=data.get("late_events", 0),
            event_keys=set(data.get("event_keys", [])),
            folded=set(data.get("folded", [])),
        )


class Store:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.path = self.data_dir / "state.json"
        self.lock = threading.RLock()
        self.events: dict[str, dict[int, list[EventRecord]]] = {}
        self.seen: set[str] = set()
        self.windows: dict[str, dict[int, WindowState]] = {}
        self.offsets: dict[str, dict[int, dict[str, int]]] = {}

    # ---- 生命周期 -------------------------------------------------------
    def load(self) -> None:
        """从磁盘恢复状态；文件不存在时初始化为空。"""
        with self.lock:
            if not self.path.exists():
                return
            with self.path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            self.from_jsonable(data)

    def persist(self) -> None:
        """把当前状态原子写盘（临时文件 + replace）。调用方须持有锁。"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".json.tmp")
        payload = json.dumps(self.to_jsonable(), ensure_ascii=False, separators=(",", ":"))
        with tmp_path.open("w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.path)

    # ---- 分区日志 -------------------------------------------------------
    def partitions(self, source: str) -> dict[int, list[EventRecord]]:
        return self.events.setdefault(source, {})

    def has_partition(self, source: str, partition: int) -> bool:
        return source in self.events and partition in self.events[source]

    def append(self, source: str, partition: int, record: EventRecord) -> None:
        self.partitions(source).setdefault(partition, []).append(record)

    def next_offset(self, source: str, partition: int) -> int:
        return len(self.partitions(source).get(partition, []))

    def total_events(self) -> int:
        return sum(len(log) for parts in self.events.values() for log in parts.values())

    def read(self, source: str, partition: int, start: int, end: int | None = None) -> list[EventRecord]:
        """读 [start, end) 范围的日志，越界自动收敛到合法区间。"""
        log = self.events.get(source, {}).get(partition, [])
        lo = max(0, start)
        hi = len(log) if end is None else min(len(log), end)
        if lo > hi:
            lo = hi
        return log[lo:hi]

    # ---- 窗口状态 -------------------------------------------------------
    def window_states(self, source: str) -> dict[int, WindowState]:
        return self.windows.setdefault(source, {})

    def get_or_create_window(self, source: str, window_start: int, window_end: int) -> WindowState:
        states = self.window_states(source)
        state = states.get(window_start)
        if state is None:
            state = WindowState(source=source, window_start=window_start, window_end=window_end)
            states[window_start] = state
        return state

    # ---- 序列化 ---------------------------------------------------------
    def to_jsonable(self) -> dict[str, Any]:
        return {
            "version": _STATE_VERSION,
            "events": {
                source: {
                    str(partition): [record.__dict__ for record in log]
                    for partition, log in parts.items()
                }
                for source, parts in self.events.items()
            },
            "seen": sorted(self.seen),
            "windows": {
                source: {str(ws): state.to_jsonable() for ws, state in states.items()}
                for source, states in self.windows.items()
            },
            "offsets": {
                source: {
                    str(partition): dict(consumers)
                    for partition, consumers in parts.items()
                }
                for source, parts in self.offsets.items()
            },
        }

    def from_jsonable(self, data: dict[str, Any]) -> None:
        self.events = {}
        for source, parts in data.get("events", {}).items():
            self.events[source] = {}
            for partition_str, records in parts.items():
                self.events[source][int(partition_str)] = [
                    EventRecord(**record) for record in records
                ]
        self.seen = set(data.get("seen", []))
        self.windows = {}
        for source, states in data.get("windows", {}).items():
            self.windows[source] = {
                int(ws): WindowState.from_jsonable(state) for ws, state in states.items()
            }
        self.offsets = {}
        for source, parts in data.get("offsets", {}).items():
            self.offsets[source] = {int(p): dict(consumers) for p, consumers in parts.items()}
