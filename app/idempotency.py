"""幂等键管理：同一 (来源, 分区, 事件编号) 全局只计入一次。"""

from __future__ import annotations

from app.storage import Store


def make_event_key(source: str, partition: int, event_id: str) -> str:
    return f"{source}|{partition}|{event_id}"


class IdempotencyManager:
    def __init__(self, store: Store) -> None:
        self.store = store

    def is_duplicate(self, key: str) -> bool:
        return key in self.store.seen

    def remember(self, key: str) -> None:
        self.store.seen.add(key)
