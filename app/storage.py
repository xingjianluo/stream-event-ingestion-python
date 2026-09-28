"""SQLite-backed local storage simulating a message channel.

Holds the append-only event log, consumer offsets and window aggregates.
Compound writes run inside explicit transactions so a restart never leaves
partial state: recovery is simply re-opening the same database file.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from .models import Event, WindowStat

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT    NOT NULL,
    partition   INTEGER NOT NULL,
    event_id    TEXT    NOT NULL,
    event_time  INTEGER NOT NULL,
    payload     TEXT    NOT NULL,
    UNIQUE (source, partition, event_id)
);
CREATE TABLE IF NOT EXISTS offsets (
    consumer    TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    partition   INTEGER NOT NULL,
    next_offset INTEGER NOT NULL,
    PRIMARY KEY (consumer, source, partition)
);
CREATE TABLE IF NOT EXISTS windows (
    source       TEXT    NOT NULL,
    window_start INTEGER NOT NULL,
    window_end   INTEGER NOT NULL,
    count        INTEGER NOT NULL DEFAULT 0,
    late_count   INTEGER NOT NULL DEFAULT 0,
    closed       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, window_start)
);
CREATE TABLE IF NOT EXISTS watermarks (
    source    TEXT    PRIMARY KEY,
    watermark INTEGER NOT NULL
);
"""


class Storage:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def open(self) -> None:
        # autocommit mode; compound writes use explicit BEGIN IMMEDIATE.
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _db(self) -> sqlite3.Connection:
        assert self._conn is not None, "storage not opened"
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Exclusive compound write: single writer, all-or-nothing."""
        with self._lock:
            db = self._db()
            db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                db.execute("ROLLBACK")
                raise
            else:
                db.execute("COMMIT")

    # ---- event log (the simulated message channel) ----

    def append_event(self, event: Event) -> tuple[bool, int]:
        """Append event; returns (inserted, seq). inserted=False means duplicate."""
        cur = self._db().execute(
            "INSERT OR IGNORE INTO events(source, partition, event_id, event_time, payload)"
            " VALUES (?, ?, ?, ?, ?)",
            (event.source, event.partition, event.event_id, event.event_time, event.payload),
        )
        inserted = cur.rowcount > 0
        row = self._db().execute(
            "SELECT seq FROM events WHERE source=? AND partition=? AND event_id=?",
            (event.source, event.partition, event.event_id),
        ).fetchone()
        return inserted, int(row["seq"])

    def read_events(self, after_seq: int, limit: int) -> list[dict]:
        """Read log entries with seq > after_seq, in seq order."""
        rows = self._db().execute(
            "SELECT seq, source, partition, event_id, event_time, payload"
            " FROM events WHERE seq > ? ORDER BY seq LIMIT ?",
            (after_seq, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def event_count(self) -> int:
        row = self._db().execute("SELECT COUNT(*) AS n FROM events").fetchone()
        return int(row["n"])

    def max_seq(self) -> int:
        """Highest committed seq; 0 when the log is empty."""
        row = self._db().execute("SELECT COALESCE(MAX(seq), 0) AS m FROM events").fetchone()
        return int(row["m"])

    # ---- consumer offsets ----

    def get_offset(self, consumer: str, source: str, partition: int) -> int | None:
        row = self._db().execute(
            "SELECT next_offset FROM offsets WHERE consumer=? AND source=? AND partition=?",
            (consumer, source, partition),
        ).fetchone()
        return None if row is None else int(row["next_offset"])

    def commit_offset(self, consumer: str, source: str, partition: int, offset: int) -> None:
        self._db().execute(
            "INSERT INTO offsets(consumer, source, partition, next_offset) VALUES (?, ?, ?, ?)"
            " ON CONFLICT(consumer, source, partition)"
            " DO UPDATE SET next_offset=excluded.next_offset",
            (consumer, source, partition, offset),
        )

    # ---- window aggregates ----

    def add_to_window(self, event: Event, window_start: int, window_end: int, late: bool) -> None:
        col = "late_count" if late else "count"
        self._db().execute(
            f"INSERT INTO windows(source, window_start, window_end, {col}) VALUES (?, ?, ?, 1)"
            f" ON CONFLICT(source, window_start)"
            f" DO UPDATE SET {col} = {col} + 1",
            (event.source, window_start, window_end),
        )

    def get_window(self, source: str, window_start: int) -> WindowStat | None:
        row = self._db().execute(
            "SELECT * FROM windows WHERE source=? AND window_start=?",
            (source, window_start),
        ).fetchone()
        return None if row is None else self._to_window_stat(row)

    def list_windows(self, source: str | None = None) -> list[WindowStat]:
        if source is None:
            rows = self._db().execute(
                "SELECT * FROM windows ORDER BY source, window_start"
            ).fetchall()
        else:
            rows = self._db().execute(
                "SELECT * FROM windows WHERE source=? ORDER BY window_start", (source,)
            ).fetchall()
        return [self._to_window_stat(r) for r in rows]

    @staticmethod
    def _to_window_stat(row: sqlite3.Row) -> WindowStat:
        return WindowStat(
            source=row["source"],
            window_start=int(row["window_start"]),
            window_end=int(row["window_end"]),
            count=int(row["count"]),
            late_count=int(row["late_count"]),
            closed=bool(row["closed"]),
        )

    def close_windows_before(self, source: str, watermark: int) -> None:
        """Mark windows whose end is at/before the watermark as closed."""
        self._db().execute(
            "UPDATE windows SET closed=1 WHERE source=? AND window_end<=? AND closed=0",
            (source, watermark),
        )

    # ---- watermark ----

    def get_watermark(self, source: str) -> int | None:
        row = self._db().execute(
            "SELECT watermark FROM watermarks WHERE source=?", (source,)
        ).fetchone()
        return None if row is None else int(row["watermark"])

    def advance_watermark(self, source: str, watermark: int) -> None:
        """Watermark only moves forward."""
        self._db().execute(
            "INSERT INTO watermarks(source, watermark) VALUES (?, ?)"
            " ON CONFLICT(source) DO UPDATE SET"
            "   watermark = MAX(watermark, excluded.watermark)",
            (source, watermark),
        )
