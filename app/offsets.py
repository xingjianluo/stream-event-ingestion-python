"""消费位点管理：只允许从当前位点连续、单调地提交。

语义约定（每个消费者独立位点，互不影响）：
- 位点表示“已连续处理完成的最后一条 offset”，下一条从 committed+1 开始；
- 首次提交必须是 0；
- 之后每次只能提交 current+1（逐条推进）；
- 提交更小的值 -> OFFSET_ROLLBACK；
- 跳过中间值 -> OFFSET_JUMP；
- 超过分区日志末端 -> OFFSET_JUMP（不能对尚不存在的事件提交位点）；
- 重复提交同一个值 -> 幂等成功，状态 ALREADY_COMMITTED。
"""

from __future__ import annotations

from app.errors import (
    OFFSET_JUMP,
    OFFSET_ROLLBACK,
    PARTITION_NOT_FOUND,
    ServiceError,
)
from app.models import CommitResult
from app.storage import Store


class OffsetManager:
    def __init__(self, store: Store) -> None:
        self.store = store

    def get(self, source: str, partition: int, consumer_id: str) -> int | None:
        return self.store.offsets.get(source, {}).get(partition, {}).get(consumer_id)

    def _consumer_map(self, source: str, partition: int) -> dict[str, int]:
        return self.store.offsets.setdefault(source, {}).setdefault(partition, {})

    def commit(
        self, source: str, partition: int, consumer_id: str, new_offset: int,
    ) -> CommitResult:
        """校验位点语义后提交。调用方须持有存储锁。"""
        if not self.store.has_partition(source, partition):
            raise ServiceError(
                PARTITION_NOT_FOUND, f"分区 {source}/{partition} 不存在，无法提交位点"
            )
        if not isinstance(new_offset, int) or isinstance(new_offset, bool) or new_offset < 0:
            raise ServiceError(OFFSET_JUMP, f"offset 必须为非负整数，实际为 {new_offset!r}")

        log_end = self.store.next_offset(source, partition)  # 下一条将分配的 offset
        if new_offset >= log_end:
            raise ServiceError(
                OFFSET_JUMP,
                f"offset={new_offset} 超出日志末端 {log_end - 1}，不能提交尚未存在的进度",
            )

        consumers = self._consumer_map(source, partition)
        previous = consumers.get(consumer_id)

        if previous is not None:
            if new_offset < previous:
                raise ServiceError(
                    OFFSET_ROLLBACK,
                    f"消费者 {consumer_id} 当前位点 {previous}，拒绝回退到 {new_offset}",
                )
            if new_offset == previous:
                return CommitResult(
                    source=source,
                    partition=partition,
                    consumer_id=consumer_id,
                    previous_offset=previous,
                    committed_offset=previous,
                    status="ALREADY_COMMITTED",
                )
            if new_offset != previous + 1:
                raise ServiceError(
                    OFFSET_JUMP,
                    f"消费者 {consumer_id} 当前位点 {previous}，"
                    f"只能提交 {previous + 1}，跳跃到 {new_offset} 被拒绝",
                )
        elif new_offset != 0:
            raise ServiceError(
                OFFSET_JUMP,
                f"消费者 {consumer_id} 首次提交必须从 0 开始，收到 {new_offset}",
            )

        consumers[consumer_id] = new_offset
        return CommitResult(
            source=source,
            partition=partition,
            consumer_id=consumer_id,
            previous_offset=previous,
            committed_offset=new_offset,
            status="COMMITTED",
        )

    def all_committed(self) -> list[tuple[str, int, str, int]]:
        """返回 (source, partition, consumer_id, offset) 列表，供管理接口查看。"""
        out: list[tuple[str, int, str, int]] = []
        for source, parts in self.store.offsets.items():
            for partition, consumers in parts.items():
                for consumer_id, offset in consumers.items():
                    out.append((source, partition, consumer_id, offset))
        return sorted(out)
