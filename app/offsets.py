"""Consumer offset commits with regression/gap protection.

Offset semantics (Kafka-style): the committed value is the NEXT log seq the
consumer will read, i.e. "everything with seq < offset has been processed".

Enforced rules (violations are rejected, never silently continued):
- first commit for a (consumer, source, partition) must be 0 — a consumer
  may not start from the middle of the log;
- a commit lower than the stored offset is a regression -> rejected;
- a commit beyond the current log end (max_seq + 1) is a jump into the
  future -> rejected;
- re-committing the stored offset is an idempotent no-op.
"""
from __future__ import annotations

from .models import RejectReason
from .storage import Storage


class OffsetError(Exception):
    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class OffsetService:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def commit(self, consumer: str, source: str, partition: int, offset: int) -> int:
        """Commit next expected offset; raises OffsetError on regression/gap.

        Returns the stored offset after the commit.
        """
        key = f"consumer={consumer!r} source={source!r} partition={partition}"
        if offset < 0:
            raise OffsetError(RejectReason.INVALID_FIELD, "offset must be >= 0")
        with self._storage.transaction():
            stored = self._storage.get_offset(consumer, source, partition)
            log_end = self._storage.max_seq() + 1
            if stored is None:
                if offset != 0:
                    raise OffsetError(
                        RejectReason.OFFSET_GAP,
                        f"first commit for {key} must be 0, got {offset}",
                    )
                stored = 0
            if offset < stored:
                raise OffsetError(
                    RejectReason.OFFSET_REGRESSION,
                    f"offset {offset} regresses below committed {stored} for {key}",
                )
            if offset > log_end:
                raise OffsetError(
                    RejectReason.OFFSET_GAP,
                    f"offset {offset} jumps beyond log end {log_end} for {key}",
                )
            if offset >= stored:
                # Upsert is idempotent: re-committing the same value is a no-op.
                self._storage.commit_offset(consumer, source, partition, offset)
            return self._storage.get_offset(consumer, source, partition)  # type: ignore[return-value]

    def get(self, consumer: str, source: str, partition: int) -> int | None:
        return self._storage.get_offset(consumer, source, partition)
