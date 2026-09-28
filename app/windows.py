"""按事件时间的固定窗口统计与迟到判定。

时间均为毫秒。规则（确定性，只依赖注入的当前时间）：
- 窗口为 epoch 对齐的固定窗口，大小 settings.window_size_seconds；
- watermark = now - allowed_lateness；
- window_end        <= watermark        -> 窗口 SEALED，事件永久拒绝（WINDOW_CLOSED）；
- watermark < window_end <= now          -> 窗口开放但事件迟到，接受并计入 late_events（LATE_ACCEPTED）；
- window_end > now                       -> 正常事件（ON_TIME）。
迟到事件不会静默丢弃：能补的补进窗口，不能补的以 WINDOW_CLOSED 明确拒绝。
fold 以事件键去重，乱序、重复、内部重放都不会把结果算乱。
"""

from __future__ import annotations

from app.clock import Clock
from app.config import Settings
from app.idempotency import make_event_key
from app.models import EventRecord, WindowView
from app.storage import Store, WindowState


class WindowManager:
    def __init__(self, store: Store, clock: Clock, settings: Settings) -> None:
        self.store = store
        self.clock = clock
        self.settings = settings

    @property
    def size_ms(self) -> int:
        return self.settings.window_size_seconds * 1000

    @property
    def allowed_lateness_ms(self) -> int:
        return self.settings.allowed_lateness_seconds * 1000

    def window_start_for(self, event_time_ms: int) -> int:
        return (event_time_ms // self.size_ms) * self.size_ms

    def watermark(self, now_ms: int) -> int:
        return now_ms - self.allowed_lateness_ms

    def classify(self, event_time_ms: int) -> tuple[str, int, bool]:
        """返回 (判定, window_start, is_late)。

        判定取值：ON_TIME / LATE_ACCEPTED / WINDOW_CLOSED。
        未来时间由 validation 层提前拒绝，不会进入这里。
        """
        now = self.clock.now_ms()
        ws = self.window_start_for(event_time_ms)
        we = ws + self.size_ms
        watermark = self.watermark(now)
        if we <= watermark:
            return "WINDOW_CLOSED", ws, True
        if we <= now:
            return "LATE_ACCEPTED", ws, True
        return "ON_TIME", ws, False

    def fold(self, record: EventRecord, is_late: bool) -> WindowState:
        """把事件幂等折叠进对应窗口；重复键不改变任何聚合值。"""
        ws = self.window_start_for(record.event_time)
        we = ws + self.size_ms
        state = self.store.get_or_create_window(record.source, ws, we)
        key = make_event_key(record.source, record.partition, record.event_id)
        if key in state.folded:
            # 同一事件（可能来自内部重放）只折叠一次
            return state
        state.folded.add(key)
        state.event_keys.add(key)
        state.count += 1
        state.total_value += record.value
        if is_late:
            state.late_events += 1
        return state

    def views(self, source: str, now_ms: int | None = None) -> list[WindowView]:
        """返回某来源下全部窗口视图，按窗口起点升序，并标注 OPEN/SEALED。"""
        now = self.clock.now_ms() if now_ms is None else now_ms
        states = self.store.window_states(source)
        return [self._view(state, now) for _, state in sorted(states.items())]

    def _view(self, state: WindowState, now_ms: int) -> WindowView:
        sealed = state.window_end <= self.watermark(now_ms)
        return WindowView(
            source=state.source,
            window_start=state.window_start,
            window_end=state.window_end,
            state="SEALED" if sealed else "OPEN",
            count=state.count,
            total_value=state.total_value,
            late_events=state.late_events,
            distinct_event_keys=len(state.event_keys),
        )
