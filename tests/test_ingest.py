"""Ingestion: idempotent dedup, validation rejections, backpressure."""
from __future__ import annotations

import json

from app.models import MAX_EVENT_BYTES
from tests.conftest import log_case, make_event


def test_duplicate_delivery_counted_once(client, storage):
    event = make_event("e1", 1_000)
    r1 = client.post("/v1/events", json=event)
    r2 = client.post("/v1/events", json=event)
    r3 = client.post("/v1/events", json=event)
    log_case(
        "duplicate_delivery",
        inputs={"event": event, "deliveries": 3},
        basis="dedup key (source, partition, event_id); only first delivery counted",
        observed={"statuses": [r1.status_code, r2.status_code, r3.status_code],
                  "bodies": [r1.json(), r2.json(), r3.json()]},
    )
    assert r1.status_code == 201 and r1.json()["status"] == "accepted"
    for r in (r2, r3):
        assert r.status_code == 200 and r.json()["status"] == "duplicate"
        assert r.json()["seq"] == r1.json()["seq"]
    assert storage.event_count() == 1
    window = storage.get_window("s", 0)
    assert window is not None and window.count == 1 and window.late_count == 0


def test_same_event_id_distinguished_by_source_and_partition(client, storage):
    base = make_event("e1", 1_000)
    other_partition = make_event("e1", 1_000, partition=1)
    other_source = make_event("e1", 1_000, source="other")
    results = [client.post("/v1/events", json=e) for e in (base, other_partition, other_source)]
    log_case(
        "dedup_key_scope",
        inputs={"events": [base, other_partition, other_source]},
        basis="identity is (source, partition, event_id); same id elsewhere is a new event",
        observed={"statuses": [r.status_code for r in results]},
    )
    assert all(r.status_code == 201 for r in results)
    assert storage.event_count() == 3


def test_malformed_json_rejected(client):
    r = client.post("/v1/events", content=b"{not-json")
    log_case("malformed_json", inputs={"body": b"{not-json"},
             basis="body must parse as a JSON object",
             observed={"status": r.status_code, "body": r.json()})
    assert r.status_code == 400
    assert r.json()["reason"] == "malformed_json"


def test_missing_field_rejected(client):
    event = make_event("e1", 1_000)
    del event["event_time"]
    r = client.post("/v1/events", json=event)
    log_case("missing_field", inputs={"body": event},
             basis="all of event_id/source/partition/event_time/payload required",
             observed={"status": r.status_code, "body": r.json()})
    assert r.status_code == 400
    assert r.json()["reason"] == "missing_field"
    assert "event_time" in r.json()["detail"]


def test_invalid_field_type_rejected(client, storage):
    event = make_event("e1", 1_000)
    event["partition"] = -1
    r = client.post("/v1/events", json=event)
    log_case("invalid_field", inputs={"body": event},
             basis="partition must be an integer >= 0",
             observed={"status": r.status_code, "body": r.json()})
    assert r.status_code == 400
    assert r.json()["reason"] == "invalid_field"
    assert storage.event_count() == 0


def test_payload_too_large_rejected(client, storage):
    event = make_event("e1", 1_000, payload="y" * MAX_EVENT_BYTES)
    body = json.dumps(event).encode()
    r = client.post("/v1/events", content=body,
                    headers={"Content-Type": "application/json"})
    log_case("payload_too_large", inputs={"body_bytes": len(body)},
             basis=f"limit is {MAX_EVENT_BYTES} bytes",
             observed={"status": r.status_code, "body": r.json()})
    assert r.status_code == 413
    assert r.json()["reason"] == "payload_too_large"
    assert storage.event_count() == 0


def test_backpressure_when_log_full(storage):
    from fastapi.testclient import TestClient

    from app.api import create_app
    from app.ingest import IngestService
    from app.offsets import OffsetService
    from app.windows import WindowEngine

    app = create_app(storage, IngestService(storage, WindowEngine(storage, 60_000, 10_000), 2),
                     OffsetService(storage))
    client = TestClient(app)
    r1 = client.post("/v1/events", json=make_event("a", 1))
    r2 = client.post("/v1/events", json=make_event("b", 2))
    r3 = client.post("/v1/events", json=make_event("c", 3))
    r4 = client.post("/v1/events", json=make_event("a", 1))  # replay while full
    log_case(
        "backpressure",
        inputs={"max_events": 2, "events": ["a", "b", "c", "a(replay)"]},
        basis="log full -> 503 backpressure, overflow rolled back; replay stays idempotent",
        observed={"statuses": [r.status_code for r in (r1, r2, r3, r4)],
                  "r3": r3.json(), "r4": r4.json(),
                  "stored": storage.event_count()},
    )
    assert (r1.status_code, r2.status_code) == (201, 201)
    assert r3.status_code == 503 and r3.json()["reason"] == "backpressure"
    assert r4.status_code == 200 and r4.json()["status"] == "duplicate"
    assert storage.event_count() == 2
    window = storage.get_window("s", 0)
    assert window is not None and window.count == 2  # overflow did not leak into stats
