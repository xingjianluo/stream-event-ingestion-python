"""时钟抽象，便于测试里控制“当前时间”来模拟迟到与窗口闭合。"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


class FakeClock:
    def __init__(self, now_ms: int = 0) -> None:
        self._now = now_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, delta_ms: int) -> int:
        self._now += delta_ms
        return self._now

    def set(self, now_ms: int) -> None:
        self._now = now_ms
