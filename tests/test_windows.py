"""Windows: event-time bucketing, watermark, late events across windows."""
from __future__ import annotations

from tests.conftest import ALLOWED_LATENESS_MS, WINDOW_SIZE_MS, log_case, make_event


def _windows(client, source="s"):
    return client.get("/v1/windows", params={"source": source}).json()["windows"]


def test_late_event_before_and_after_window_close(client):
    # t=5s lands in window [0,60s); watermark = 5s - 10s < 0, window open.
    client.post("/v1/events", json=make_event("a", 5_000))
    # t=50s is still within allowed lateness of the open window -> counted.
    client.post("/v1/events", json=make_event("b", 50_000))
    # t=70s pushes the watermark to 60s -> window [0,60s) closes.
    client.post("/v1/events", json=make_event("c", 70_000))
    # t=10s now belongs to a CLOSED window -> recorded as late, count frozen.
    client.post("/v1/events", json=make_event("d", 10_000))
    windows = _windows(client)
    log_case(
        "late_before_after_close",
        inputs={"events": [("a", 5_000), ("b", 50_000), ("c", 70_000), ("d", 10_000)],
                "window_size_ms": WINDOW_SIZE_MS,
                "allowed_lateness_ms": ALLOWED_LATENESS_MS},
        basis=("watermark = max(event_time) - lateness; open window absorbs events, "
               "closed window freezes count and records late_count"),
        observed={"windows": windows},
    )
    w0, w1 = windows
    assert (w0["window_start"], w0["closed"]) == (0, True)
    assert (w0["count"], w0["late_count"]) == (2, 1)   # a,b counted; d late
    assert (w1["window_start"], w1["count"], w1["closed"]) == (60_000, 1, False)


def test_out_of_order_and_duplicate_do_not_corrupt_closed_window(client):
    client.post("/v1/events", json=make_event("a", 5_000))
    client.post("/v1/events", json=make_event("c", 70_000))   # closes [0,60s)
    # duplicate of an on-time event after close: dedup wins, nothing changes.
    client.post("/v1/events", json=make_event("a", 5_000))
    # out-of-order late event, delivered twice: counted once, as late.
    client.post("/v1/events", json=make_event("d", 10_000))
    client.post("/v1/events", json=make_event("d", 10_000))
    windows = _windows(client)
    log_case(
        "out_of_order_duplicate_closed_window",
        inputs={"events": [("a", 5_000), ("c", 70_000), ("a", 5_000, "dup"),
                           ("d", 10_000), ("d", 10_000, "dup")]},
        basis=("dedup happens before windowing; a closed window's count is frozen "
               "and late deliveries land in late_count exactly once"),
        observed={"windows": windows},
    )
    w0 = windows[0]
    assert (w0["count"], w0["late_count"]) == (1, 1)


def test_very_late_event_creates_closed_window_record(client):
    client.post("/v1/events", json=make_event("a", 200_000))  # wm = 190s
    # belongs to [120s,180s), far behind the watermark; no row exists yet.
    client.post("/v1/events", json=make_event("x", 130_000))
    windows = _windows(client)
    log_case(
        "very_late_event",
        inputs={"events": [("a", 200_000), ("x", 130_000)]},
        basis=("event behind the watermark is never dropped: it is recorded as "
               "late_count in its own (immediately closed) window"),
        observed={"windows": windows},
    )
    w_late = next(w for w in windows if w["window_start"] == 120_000)
    assert (w_late["count"], w_late["late_count"], w_late["closed"]) == (0, 1, True)


def test_watermark_is_per_source(client):
    client.post("/v1/events", json=make_event("a", 200_000, source="fast"))
    # slow source has no watermark yet; its old event is still on time.
    client.post("/v1/events", json=make_event("b", 1_000, source="slow"))
    fast = _windows(client, "fast")
    slow = _windows(client, "slow")
    log_case(
        "per_source_watermark",
        inputs={"events": [("fast", 200_000), ("slow", 1_000)]},
        basis="watermarks are tracked per source; one source cannot close another's windows",
        observed={"fast": fast, "slow": slow},
    )
    assert slow[0]["count"] == 1 and slow[0]["late_count"] == 0
