"""非法请求拒绝原因、批量原子性、并发写入一致性与背压测试。"""

from __future__ import annotations

import concurrent.futures
import threading

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.service import IngestionService
from tests.conftest import log_decision


def _event(eid: str = "e", **over) -> dict:
    base = {"source": "shop", "partition": 0, "event_id": eid, "event_time": 999_000, "value": 1}
    base.update(over)
    return base


# ----------------------------------------------------------------------
# 非法形态 -> 可区分的拒绝原因
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "case,raw,expected",
    [
        ("空请求体", b"", "MALFORMED_JSON"),
        ("非法 JSON", b"{not-json", "MALFORMED_JSON"),
    ],
)
def test_malformed_body(client, case, raw, expected):
    r = client.post("/v1/events", content=raw, headers={"content-type": "application/json"})
    log_decision(case, raw, r.json())
    assert r.status_code == 400 and r.json()["error"] == expected


def test_body_too_large_is_distinct(client, settings):
    big = b"x" * (settings.max_body_bytes + 1)
    r = client.post("/v1/events", content=big, headers={"content-type": "application/json"})
    log_decision("超大请求体", {"size": len(big), "limit": settings.max_body_bytes}, r.json())
    assert r.status_code == 413 and r.json()["error"] == "BODY_TOO_LARGE"


def test_missing_field_is_distinct(client):
    r = client.post("/v1/events", json={"source": "shop", "partition": 0, "event_id": "e"})
    log_decision("缺少 event_time", {"keys": ["source", "partition", "event_id"]}, r.json())
    assert r.status_code == 400 and r.json()["error"] == "MISSING_FIELD"
    assert "event_time" in r.json()["detail"]


def test_bad_type_and_invalid_partition_distinct(client):
    r1 = client.post("/v1/events", json=_event(partition="0"))
    r2 = client.post("/v1/events", json=_event(partition=-3))
    r3 = client.post("/v1/events", json=_event(event_id=""))
    r4 = client.post("/v1/events", json=_event(value="x"))
    log_decision(
        "可区分的非法字段",
        ["partition='0'", "partition=-3", "event_id=''", "value='x'"],
        [r1.json()["error"], r2.json()["error"], r3.json()["error"], r4.json()["error"]],
    )
    assert r1.status_code == 422 and r1.json()["error"] == "INVALID_PARTITION"
    assert r2.status_code == 422 and r2.json()["error"] == "INVALID_PARTITION"
    assert r3.status_code == 400 and r3.json()["error"] == "BAD_FIELD_TYPE"
    assert r4.status_code == 400 and r4.json()["error"] == "BAD_FIELD_TYPE"


def test_event_id_too_long_distinct(client, settings):
    r = client.post("/v1/events", json=_event(event_id="z" * (settings.max_event_id_length + 1)))
    log_decision("event_id 超长", {"length": settings.max_event_id_length + 1}, r.json())
    assert r.status_code == 422 and r.json()["error"] == "EVENT_ID_TOO_LONG"


def test_future_event_time_rejected(client):
    r = client.post("/v1/events", json=_event(event_time=5_000_000))
    log_decision("未来时间", {"event_time": 5_000_000}, r.json())
    assert r.status_code == 422 and r.json()["error"] == "INVALID_EVENT_TIME"


def test_batch_payload_shape_errors(client, settings):
    r1 = client.post("/v1/events/batch", json={"events": "not-list"})
    r2 = client.post(
        "/v1/events/batch",
        json={"events": [_event(f"e{i}") for i in range(settings.max_events_per_batch + 1)]},
    )
    log_decision("批量形态错误", ["events 非数组", "超过单批上限"], [r1.json()["error"], r2.json()["error"]])
    assert r1.status_code == 400 and r1.json()["error"] == "INVALID_BATCH"
    assert r2.status_code == 413 and r2.json()["error"] == "BATCH_TOO_LARGE"


# ----------------------------------------------------------------------
# 批量原子性：一条硬性拒绝 -> 整批不落状态
# ----------------------------------------------------------------------
def test_batch_is_atomic_when_one_event_invalid(client):
    payload = {"events": [_event("ok-1"), _event(event_time=1000), _event("ok-2")]}  # 中间事件窗口已闭合
    r = client.post("/v1/events/batch", json=payload)
    log_decision("批量中一条窗口闭合", [e["event_id"] if e["event_id"] else "?" for e in payload["events"]], r.json())
    assert r.status_code == 422 and r.json()["error"] == "WINDOW_CLOSED"
    health = client.get("/v1/health").json()
    assert health["total_events"] == 0  # 整批回滚，没有半截写入
    windows = client.get("/v1/sources/shop/windows").json()["windows"]
    assert windows == []


def test_batch_duplicates_accepted_but_invalid_rolls_back(client):
    # 先写一条
    client.post("/v1/events", json=_event("exists"))
    payload = {"events": [_event("exists"), _event(event_time=1000)]}  # 一条重复 + 一条非法
    r = client.post("/v1/events/batch", json=payload)
    assert r.status_code == 422 and r.json()["error"] == "WINDOW_CLOSED"
    # 非法导致整批拒绝；原先那一条仍在，且只有它
    assert client.get("/v1/health").json()["total_events"] == 1


# ----------------------------------------------------------------------
# 并发写入：同一事件键被多线程同时投递，只有一条计入
# ----------------------------------------------------------------------
def test_concurrent_duplicate_writes_counted_once(settings, clock, tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        window_size_seconds=10,
        allowed_lateness_seconds=30,
        max_total_events=10000,
        max_inflight_requests=64,
    )
    svc = IngestionService(settings=settings, clock=clock)
    client = TestClient(create_app(svc))

    payload = _event("hot-key")

    def post_once(_):
        return client.post("/v1/events", json=payload)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        responses = list(pool.map(post_once, range(32)))

    statuses = sorted(r.json()["status"] for r in responses)
    accepted = sum(1 for r in responses if r.json()["status"] == "ACCEPTED")
    duplicate = sum(1 for r in responses if r.json()["status"] == "DUPLICATE")
    log_decision("32 并发重复投递同一事件", payload, {"ACCEPTED": accepted, "DUPLICATE": duplicate})
    assert accepted == 1 and duplicate == 31
    health = client.get("/v1/health").json()
    assert health["total_events"] == 1 and health["distinct_keys"] == 1
    windows = client.get("/v1/sources/shop/windows").json()["windows"]
    assert sum(w["count"] for w in windows) == 1


def test_concurrent_distinct_writes_all_persisted(settings, clock, tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data2",
        window_size_seconds=10,
        allowed_lateness_seconds=30,
        max_total_events=10000,
        max_inflight_requests=64,
    )
    svc = IngestionService(settings=settings, clock=clock)
    client = TestClient(create_app(svc))

    def post_one(i):
        return client.post("/v1/events", json=_event(f"k-{i}"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        responses = list(pool.map(post_one, range(200)))

    assert all(r.status_code == 200 for r in responses)
    accepted = sum(1 for r in responses if r.json()["status"] == "ACCEPTED")
    log_decision("200 并发不同事件", {"n": 200}, {"accepted": accepted})
    health = client.get("/v1/health").json()
    assert accepted == 200
    assert health["total_events"] == 200 and health["distinct_keys"] == 200
    # offset 连续无缺号
    events = client.get(
        "/v1/sources/shop/partitions/0/events?consumer_id=audit&max_events=1000"
    ).json()["events"]
    offsets = [e["offset"] for e in events]
    assert offsets == list(range(200))


# ----------------------------------------------------------------------
# 背压：在途名额耗尽时返回可区分的 503，而不是悄悄排队/丢数据
# ----------------------------------------------------------------------
def test_backpressure_returns_distinct_503(settings, clock, tmp_path):
    # 在途名额只有 1：第一个请求占住名额期间，第二个请求必须拿到 503 BACKPRESSURE
    settings = Settings(
        data_dir=tmp_path / "data3",
        window_size_seconds=10,
        allowed_lateness_seconds=30,
        max_total_events=10000,
        max_inflight_requests=1,
    )
    svc = IngestionService(settings=settings, clock=clock)
    from app.models import IngestEvent

    started = threading.Event()
    gate = threading.Event()
    real_ingest = svc.ingest_batch

    def slow_ingest(events):
        started.set()
        gate.wait(timeout=2)  # 占住唯一在途名额，直到测试放行
        return real_ingest(events)

    svc.ingest_batch = slow_ingest
    outcome = {}

    def first():
        # 完整走 try_enter -> ingest -> leave 流程
        svc.try_enter()
        try:
            outcome["first"] = slow_ingest([IngestEvent("shop", 0, "first", 999_000, 1)]).results[0].status
        finally:
            svc.leave()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(first)
        assert started.wait(timeout=2), "第一个请求未能进入处理"
        # 此时唯一名额被占，再申请名额应立即触发背压
        try:
            svc.try_enter()
            outcome["second"] = "NO_BACKPRESSURE"
        except Exception as exc:  # noqa: BLE001
            outcome["second"] = exc.error.code
        gate.set()
        f1.result(timeout=2)

    log_decision("在途名额耗尽", {"max_inflight": 1}, outcome)
    assert outcome["first"] == "ACCEPTED"
    assert outcome["second"] == "BACKPRESSURE"

    # 名额释放后可以再次进入，证明是背压而非永久拒绝
    svc.try_enter()
    svc.leave()


def test_capacity_full_is_distinct_from_backpressure(settings, clock, tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data4",
        window_size_seconds=10,
        allowed_lateness_seconds=30,
        max_total_events=3,
        max_inflight_requests=10,
    )
    svc = IngestionService(settings=settings, clock=clock)
    client = TestClient(create_app(svc))
    for i in range(3):
        assert client.post("/v1/events", json=_event(f"k{i}")).status_code == 200
    r = client.post("/v1/events", json=_event("overflow"))
    log_decision("容量耗尽（区别于背压）", {"max_total": 3}, r.json())
    assert r.status_code == 409 and r.json()["error"] == "CAPACITY_EXCEEDED"
