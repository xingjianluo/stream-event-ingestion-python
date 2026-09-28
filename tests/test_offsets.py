"""Offsets: monotonic contiguous commits, regression/gap rejection, recovery."""
from __future__ import annotations

from tests.conftest import log_case, make_event


def _commit(client, offset, consumer="c1", source="s", partition=0):
    return client.post("/v1/offsets", json={
        "consumer": consumer, "source": source,
        "partition": partition, "offset": offset,
    })


def test_offset_commit_progression(client):
    for i in range(3):
        client.post("/v1/events", json=make_event(f"e{i}", i * 1_000))
    results = [_commit(client, o) for o in (0, 2, 4)]
    log_case(
        "offset_progression",
        inputs={"commits": [0, 2, 4], "log_end": 4},
        basis="monotonic commits within [stored, log_end] are accepted",
        observed={"statuses": [r.status_code for r in results],
                  "bodies": [r.json() for r in results]},
    )
    assert [r.status_code for r in results] == [200, 200, 200]
    assert [r.json()["committed"] for r in results] == [0, 2, 4]


def test_offset_regression_rejected(client):
    for i in range(3):
        client.post("/v1/events", json=make_event(f"e{i}", i * 1_000))
    _commit(client, 0)
    _commit(client, 3)
    r = _commit(client, 1)
    log_case(
        "offset_regression",
        inputs={"stored": 3, "commit": 1},
        basis="commit below stored offset must be rejected, not resumed from the middle",
        observed={"status": r.status_code, "body": r.json()},
    )
    assert r.status_code == 409
    assert r.json()["reason"] == "offset_regression"
    assert client.get("/v1/offsets", params={
        "consumer": "c1", "source": "s", "partition": 0}).json()["offset"] == 3


def test_offset_gap_rejected(client):
    for i in range(3):
        client.post("/v1/events", json=make_event(f"e{i}", i * 1_000))
    r_first = _commit(client, 2)          # first commit must be 0
    _commit(client, 0)
    r_beyond = _commit(client, 99)        # beyond log end (max_seq 3 -> end 4)
    log_case(
        "offset_gap",
        inputs={"first_commit": 2, "beyond_log_end": 99, "log_end": 4},
        basis="consumer must start at 0 and may not jump past the log end",
        observed={"first": {"status": r_first.status_code, "body": r_first.json()},
                  "beyond": {"status": r_beyond.status_code, "body": r_beyond.json()}},
    )
    assert r_first.status_code == 409 and r_first.json()["reason"] == "offset_gap"
    assert r_beyond.status_code == 409 and r_beyond.json()["reason"] == "offset_gap"


def test_offset_recommit_is_idempotent(client):
    client.post("/v1/events", json=make_event("e0", 0))
    _commit(client, 0)
    _commit(client, 1)
    r = _commit(client, 1)
    log_case(
        "offset_recommit",
        inputs={"commits": [0, 1, 1]},
        basis="re-committing the stored offset is a no-op, not an error",
        observed={"status": r.status_code, "body": r.json()},
    )
    assert r.status_code == 200 and r.json()["committed"] == 1


def test_restart_recovers_offsets_and_stats(tmp_path):
    """Restart on the same db file: no double counting, no lost progress."""
    from fastapi.testclient import TestClient

    from app.api import create_app
    from app.ingest import IngestService
    from app.offsets import OffsetService
    from app.storage import Storage
    from app.windows import WindowEngine

    db = str(tmp_path / "sei.db")

    def boot():
        s = Storage(db)
        s.open()
        app = create_app(s, IngestService(s, WindowEngine(s, 60_000, 10_000), 1000),
                         OffsetService(s))
        return s, TestClient(app)

    s1, c1 = boot()
    c1.post("/v1/events", json=make_event("a", 1_000))
    c1.post("/v1/events", json=make_event("b", 2_000))
    _commit(c1, 0)
    _commit(c1, 2)
    s1.close()

    s2, c2 = boot()  # simulated restart
    replay = [c2.post("/v1/events", json=make_event(e, t))
              for e, t in (("a", 1_000), ("b", 2_000))]
    offset = c2.get("/v1/offsets", params={"consumer": "c1", "source": "s", "partition": 0})
    windows = c2.get("/v1/windows").json()["windows"]
    log_case(
        "restart_recovery",
        inputs={"pre_restart_events": ["a", "b"], "committed": 2,
                "post_restart_replay": ["a", "b"]},
        basis="state lives in the db file; replays dedup, offsets and windows survive",
        observed={"replay_statuses": [r.json()["status"] for r in replay],
                  "offset": offset.json(), "windows": windows,
                  "event_count": s2.event_count()},
    )
    assert all(r.json()["status"] == "duplicate" for r in replay)
    assert offset.json()["offset"] == 2
    assert s2.event_count() == 2
    assert windows[0]["count"] == 2 and windows[0]["late_count"] == 0
    s2.close()
