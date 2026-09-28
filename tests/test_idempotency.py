"""重复投递幂等去重测试。

覆盖：
1. 同一事件经“单条入口”重复投递只计入一次；
2. 同一事件经“批量入口 / 批量内重复”也只计入一次（各入口判定一致）；
3. 相同 event_id 但来源或分区不同，必须被区分成不同事件；
4. 重复投递不改变窗口聚合与总容量计数；
5. 重启（用同一份磁盘状态重新构造服务）后重复投递依旧幂等。
"""

from __future__ import annotations

from pathlib import Path

from app.api import create_app
from app.config import Settings
from app.service import IngestionService
from tests.conftest import log_decision


def _event(eid: str, *, source: str = "shop", partition: int = 0, et: int = 1_000_500, value: int = 1) -> dict:
    return {"source": source, "partition": partition, "event_id": eid, "event_time": et, "value": value}


def test_duplicate_via_single_entry_is_counted_once(client):
    payload = _event("evt-1", value=4)
    r1 = client.post("/v1/events", json=payload)
    r2 = client.post("/v1/events", json=payload)  # 重复投递
    log_decision("单条入口重复投递", payload, {"first": r1.json()["status"], "second": r2.json()["status"]})

    assert r1.status_code == 200 and r1.json()["status"] == "ACCEPTED"
    assert r2.status_code == 200 and r2.json()["status"] == "DUPLICATE"

    windows = client.get("/v1/sources/shop/windows").json()["windows"]
    w = next(w for w in windows if w["window_start"] == 1_000_000)
    assert w["count"] == 1
    assert w["total_value"] == 4
    assert w["distinct_event_keys"] == 1


def test_duplicate_across_entries_has_consistent_result(client):
    payload = _event("evt-x", value=2)
    single = client.post("/v1/events", json=payload)
    batch = client.post("/v1/events/batch", json={"events": [payload]})
    inner_dup = client.post("/v1/events/batch", json={"events": [payload, payload]})
    log_decision(
        "不同入口重复投递",
        payload,
        {
            "single": single.json()["status"],
            "batch": (batch.json()["accepted"], batch.json()["duplicate"]),
            "inner_dup": (inner_dup.json()["accepted"], inner_dup.json()["duplicate"]),
        },
    )
    assert single.json()["status"] == "ACCEPTED"
    assert (batch.json()["accepted"], batch.json()["duplicate"]) == (0, 1)
    # evt-x 此前已在单条入口写入，因此批量里两条全部判重
    assert (inner_dup.json()["accepted"], inner_dup.json()["duplicate"]) == (0, 2)
    assert [r["status"] for r in inner_dup.json()["results"]] == ["DUPLICATE", "DUPLICATE"]

    # 再验证“仅批量内部重复”：新 id，第一条 ACCEPTED、第二条 DUPLICATE
    only_inner = client.post(
        "/v1/events/batch",
        json={"events": [_event("evt-inner"), _event("evt-inner")]},
    )
    assert (only_inner.json()["accepted"], only_inner.json()["duplicate"]) == (1, 1)
    assert [r["status"] for r in only_inner.json()["results"]] == ["ACCEPTED", "DUPLICATE"]

    windows = client.get("/v1/sources/shop/windows").json()["windows"]
    total = sum(w["count"] for w in windows)
    # evt-x 一条 + evt-inner 一条；任何重复投递都不再增加计数
    assert total == 2


def test_same_event_id_different_source_or_partition_are_distinct(client):
    p1 = _event("same-id", source="shop-a", partition=0)
    p2 = _event("same-id", source="shop-b", partition=0)
    p3 = _event("same-id", source="shop-a", partition=7)
    for p in (p1, p2, p3):
        assert client.post("/v1/events", json=p).json()["status"] == "ACCEPTED"

    # 再次投递三者，均应判为重复（证明三者各自独立被记住）
    decisions = [client.post("/v1/events", json=p).json()["status"] for p in (p1, p2, p3)]
    log_decision("同编号不同来源/分区", [p1, p2, p3], decisions)
    assert decisions == ["DUPLICATE", "DUPLICATE", "DUPLICATE"]

    health = client.get("/v1/health").json()
    assert health["total_events"] == 3
    assert health["distinct_keys"] == 3


def test_duplicate_does_not_consume_capacity(client, settings, service):
    for i in range(settings.max_total_events):
        r = client.post("/v1/events", json=_event(f"cap-{i}", et=1_000_100))
        assert r.json()["status"] == "ACCEPTED"
    # 容量满后：新事件被拒，旧事件重复投递仍然幂等成功且不占容量
    rejected = client.post("/v1/events", json=_event("brand-new"))
    duplicate = client.post("/v1/events", json=_event("cap-0", et=1_000_100))
    log_decision(
        "容量满时重复 vs 新增",
        {"capacity": settings.max_total_events},
        {"new": rejected.json()["error"], "dup": duplicate.json()["status"]},
    )
    assert rejected.status_code == 409 and rejected.json()["error"] == "CAPACITY_EXCEEDED"
    assert duplicate.status_code == 200 and duplicate.json()["status"] == "DUPLICATE"


def test_idempotency_survives_restart(settings: Settings, clock, tmp_path: Path):
    svc1 = IngestionService(settings=settings, clock=clock)
    app1 = create_app(svc1)
    from fastapi.testclient import TestClient

    c1 = TestClient(app1)
    payload = _event("persist-1", value=6)
    assert c1.post("/v1/events", json=payload).json()["status"] == "ACCEPTED"

    # 用同一 data_dir 重新构造服务，模拟重启
    svc2 = IngestionService(settings=settings, clock=clock)
    c2 = TestClient(create_app(svc2))
    again = c2.post("/v1/events", json=payload)
    log_decision("重启后重复投递", payload, again.json())
    assert again.status_code == 200 and again.json()["status"] == "DUPLICATE"

    windows = c2.get("/v1/sources/shop/windows").json()["windows"]
    w = next(w for w in windows if w["window_start"] == 1_000_000)
    assert w["count"] == 1 and w["total_value"] == 6
