"""Concurrency: parallel writers, duplicate storms, offset commit races."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from app.ingest import IngestService
from app.models import Event, IngestStatus
from app.offsets import OffsetError, OffsetService
from app.windows import WindowEngine
from tests.conftest import log_case


def _ingest_one(svc: IngestService, event_id: str, event_time: int):
    return svc.ingest(Event(event_id, "s", 0, event_time, "x"))


def test_concurrent_writers_with_duplicate_storm(storage):
    svc = IngestService(storage, WindowEngine(storage, 60_000, 10_000), 10_000)
    unique_ids = [f"e{i}" for i in range(200)]
    # every event delivered 3 times, all deliveries racing across threads
    deliveries = [(eid, t) for eid in unique_ids for t in (1_000, 1_000, 1_000)]
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda d: _ingest_one(svc, *d), deliveries))
    accepted = sum(1 for r in results if r.status == IngestStatus.ACCEPTED)
    duplicates = sum(1 for r in results if r.status == IngestStatus.DUPLICATE)
    window = storage.get_window("s", 0)
    log_case(
        "concurrent_duplicate_storm",
        inputs={"unique_events": len(unique_ids), "deliveries": len(deliveries),
                "threads": 16},
        basis=("each unique (source, partition, event_id) accepted exactly once; "
               "window count equals accepted count"),
        observed={"accepted": accepted, "duplicates": duplicates,
                  "stored": storage.event_count(),
                  "window_count": window.count if window else None},
    )
    assert accepted == len(unique_ids)
    assert duplicates == len(deliveries) - len(unique_ids)
    assert storage.event_count() == len(unique_ids)
    assert window is not None and window.count == len(unique_ids)


def test_concurrent_offset_commits_never_regress(storage):
    svc = OffsetService(storage)
    # fill the log so commits up to its end are legal
    with storage.transaction():
        for i in range(50):
            storage.append_event(Event(f"e{i}", "s", 0, i, "x"))
    log_end = storage.max_seq() + 1

    commits = [0] * 4 + list(range(1, log_end + 1)) * 4  # racy, unsorted per thread
    errors: list[str] = []

    def try_commit(o: int) -> None:
        try:
            svc.commit("c1", "s", 0, o)
        except OffsetError as exc:
            errors.append(exc.reason.value)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(try_commit, commits))
    final = svc.get("c1", "s", 0)
    log_case(
        "concurrent_offset_commits",
        inputs={"commits": "0..log_end x4 across 8 threads", "log_end": log_end},
        basis=("stored offset is monotonic; losing races are rejected as "
               "regressions, the final value is the maximum committed"),
        observed={"final": final, "rejections": sorted(set(errors))},
    )
    assert final == log_end
    assert set(errors) <= {"offset_regression"}


def test_concurrent_writes_respect_capacity(storage):
    svc = IngestService(storage, WindowEngine(storage, 60_000, 10_000), max_events=100)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda i: _ingest_one(svc, f"e{i}", 1_000), range(500)))
    accepted = sum(1 for r in results if r.status == IngestStatus.ACCEPTED)
    backpressure = sum(1 for r in results
                       if r.status == IngestStatus.REJECTED
                       and r.reason is not None and r.reason.value == "backpressure")
    window = storage.get_window("s", 0)
    log_case(
        "concurrent_capacity",
        inputs={"writers": 500, "max_events": 100, "threads": 16},
        basis=("capacity check and append are serialized; overflow is rejected "
               "with backpressure and rolled back, never silently dropped"),
        observed={"accepted": accepted, "backpressure": backpressure,
                  "stored": storage.event_count(),
                  "window_count": window.count if window else None},
    )
    assert accepted == 100
    assert backpressure == 400
    assert storage.event_count() == 100
    assert window is not None and window.count == 100
