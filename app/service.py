"""核心服务层：统一编排写入、窗口折叠、位点、背压与持久化。

关键一致性约定：
- 所有状态变更都在 store.lock 内完成，并发写入串行化，结果确定；
- 批量写入先整批校验（重复键探测、容量、窗口闭合），全部通过才落状态，
  任一事件被硬性拒绝则整批拒绝、不留半截结果；
- 窗口折叠按事件键幂等，乱序、重复投递、内部重放都只计一次；
- 每次成功变更后立即原子持久化，重启后不丢不重；
- 在途名额用非阻塞信号量模拟容量吃紧，拿不到名额 -> 明确背压（BACKPRESSURE）。
"""

from __future__ import annotations

import threading

from app.clock import Clock, SystemClock
from app.config import Settings
from app.errors import (
    BACKPRESSURE,
    CAPACITY_EXCEEDED,
    PARTITION_NOT_FOUND,
    WINDOW_CLOSED,
    ServiceError,
)
from app.idempotency import IdempotencyManager, make_event_key
from app.models import (
    BatchResult,
    CommitResult,
    EventRecord,
    EventResult,
    FetchResult,
    IngestEvent,
    WindowView,
)
from app.offsets import OffsetManager
from app.storage import Store
from app.windows import WindowManager


class IngestionService:
    def __init__(
        self,
        settings: Settings | None = None,
        clock: Clock | None = None,
        store: Store | None = None,
    ) -> None:
        self.settings = settings or Settings()
        self.clock = clock or SystemClock()
        self.store = store or Store(self.settings.data_dir)
        self.store.load()
        self.idempotency = IdempotencyManager(self.store)
        self.offsets = OffsetManager(self.store)
        self.windows = WindowManager(self.store, self.clock, self.settings)
        self._inflight = threading.Semaphore(self.settings.max_inflight_requests)

    # ---- 写入 -----------------------------------------------------------
    def ingest_batch(self, events: list[IngestEvent]) -> BatchResult:
        """原子地写入一批事件。返回每条事件的判定（ACCEPTED/DUPLICATE）。"""
        with self.store.lock:
            # 第一阶段：整批预校验。任何硬性拒绝都不产生状态变更。
            planned: list[tuple[IngestEvent, str, int, bool, str]] = []
            candidate_keys: set[str] = set()
            # 容量按“将要新增”的唯一事件数预估（重复事件不占日志位）
            would_add = 0
            for event in events:
                key = make_event_key(event.source, event.partition, event.event_id)
                if key in self.store.seen or key in candidate_keys:
                    planned.append((event, key, -1, False, "DUPLICATE"))
                    continue
                candidate_keys.add(key)
                verdict, ws, is_late = self.windows.classify(event.event_time)
                if verdict == "WINDOW_CLOSED":
                    raise ServiceError(
                        WINDOW_CLOSED,
                        f"事件 {key} 所属窗口 [{ws}, {ws + self.windows.size_ms}) 已闭合，"
                        f"超过允许迟到 {self.settings.allowed_lateness_seconds}s，整批拒绝",
                    )
                would_add += 1
                planned.append((event, key, ws, is_late, "ACCEPTED"))

            if self.store.total_events() + would_add > self.settings.max_total_events:
                raise ServiceError(
                    CAPACITY_EXCEEDED,
                    f"本批新增 {would_add} 条后将超过总容量 {self.settings.max_total_events}，整批拒绝",
                )

            # 第二阶段：全部通过，依次落状态
            results: list[EventResult] = []
            accepted = duplicate = 0
            for event, key, ws, is_late, status in planned:
                if status == "DUPLICATE":
                    duplicate += 1
                    results.append(
                        EventResult(
                            status="DUPLICATE",
                            offset=None,
                            reason="同一 (source, partition, event_id) 已计入，幂等忽略",
                            window_start=self.windows.window_start_for(event.event_time),
                            lateness=None,
                        )
                    )
                    continue
                offset = self.store.next_offset(event.source, event.partition)
                record = EventRecord(
                    source=event.source,
                    partition=event.partition,
                    event_id=event.event_id,
                    event_time=event.event_time,
                    value=event.value,
                    offset=offset,
                    ingress_time=self.clock.now_ms(),
                )
                self.store.append(event.source, event.partition, record)
                self.idempotency.remember(key)
                self.windows.fold(record, is_late)
                accepted += 1
                results.append(
                    EventResult(
                        status="ACCEPTED",
                        offset=offset,
                        window_start=ws,
                        lateness=is_late,
                    )
                )
            self.store.persist()
            return BatchResult(
                accepted=accepted, duplicate=duplicate, rejected=0, results=results
            )

    # ---- 消费 -----------------------------------------------------------
    def fetch(
        self, source: str, partition: int, consumer_id: str, max_events: int = 100
    ) -> FetchResult:
        """按消费者已提交位点之后续读日志；新消费者从 offset 0 开始。"""
        with self.store.lock:
            if not self.store.has_partition(source, partition):
                raise ServiceError(
                    PARTITION_NOT_FOUND, f"分区 {source}/{partition} 不存在，没有可消费事件"
                )
            committed = self.offsets.get(source, partition, consumer_id)
            start = 0 if committed is None else committed + 1
            end = start + max(1, max_events)
            events = self.store.read(source, partition, start, end)
            return FetchResult(
                source=source,
                partition=partition,
                consumer_id=consumer_id,
                from_offset=start,
                events=events,
            )

    def commit(self, source: str, partition: int, consumer_id: str, offset: int) -> CommitResult:
        with self.store.lock:
            result = self.offsets.commit(source, partition, consumer_id, offset)
            self.store.persist()
            return result

    # ---- 查询 -----------------------------------------------------------
    def window_views(self, source: str) -> list[WindowView]:
        with self.store.lock:
            return self.windows.views(source)

    def stats(self) -> dict:
        with self.store.lock:
            return {
                "total_events": self.store.total_events(),
                "capacity": self.settings.max_total_events,
                "distinct_keys": len(self.store.seen),
                "inflight_capacity": self.settings.max_inflight_requests,
            }

    # ---- 背压 -----------------------------------------------------------
    def check_capacity(self) -> None:
        if self.store.total_events() >= self.settings.max_total_events:
            raise ServiceError(
                CAPACITY_EXCEEDED,
                f"已达总容量 {self.settings.max_total_events}，拒绝写入",
            )

    def try_enter(self) -> None:
        """非阻塞申请一个在途名额，失败表示触发背压。"""
        if not self._inflight.acquire(blocking=False):
            raise ServiceError(
                BACKPRESSURE,
                f"在途请求已达 {self.settings.max_inflight_requests}，请稍后重试",
            )

    def leave(self) -> None:
        self._inflight.release()
