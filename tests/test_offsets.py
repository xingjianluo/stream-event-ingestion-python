"""消费位点提交与断点恢复测试。

覆盖：
1. 首次提交必须从 0 开始，之后只能逐条连续提交；
2. 位点回退 -> OFFSET_ROLLBACK(409)，跳跃 -> OFFSET_JUMP(409)，原因可区分；
3. 重复提交同一位点幂等成功（ALREADY_COMMITTED），不重复推进；
4. 未提交位点时重复拉取得到同一批数据（at-least-once 重放），提交后从下一条续读；
5. 服务重启后位点从磁盘恢复，统计不重复、不丢失；
6. 不同消费者位点互不影响。
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.service import IngestionService
from tests.conftest import log_decision


def _ingest(client, eid: str, et: int = 1_000_100, partition: int = 0, value: int = 1):
    return client.post(
        "/v1/events",
        json={"source": "orders", "partition": partition, "event_id": eid, "event_time": et, "value": value},
    )


def _commit(client, offset: int, consumer: str = "worker-1", partition: int = 0):
    return client.post(
        f"/v1/sources/orders/partitions/{partition}/commit?consumer_id={consumer}",
        json={"offset": offset},
    )


def _fetch(client, consumer: str = "worker-1", partition: int = 0, max_events: int = 100):
    return client.get(
        f"/v1/sources/orders/partitions/{partition}/events"
        f"?consumer_id={consumer}&max_events={max_events}"
    )


def test_commit_must_be_contiguous_from_zero(client):
    for i in range(3):
        _ingest(client, f"e{i}")

    r_first = _commit(client, 1)
    log_decision("首次提交 offset=1", {"partition": 0}, r_first.json())
    assert r_first.status_code == 409 and r_first.json()["error"] == "OFFSET_JUMP"

    assert _commit(client, 0).json()["status"] == "COMMITTED"
    assert _commit(client, 1).json()["status"] == "COMMITTED"
    assert _commit(client, 2).json()["status"] == "COMMITTED"

    # 已到末端，再提交不存在的 offset=3 属于跳跃越界
    r_end = _commit(client, 3)
    assert r_end.status_code == 409 and r_end.json()["error"] == "OFFSET_JUMP"


def test_offset_rollback_is_rejected(client):
    _ingest(client, "a"); _ingest(client, "b")
    _commit(client, 0); _commit(client, 1)
    r = _commit(client, 0)  # 从 1 回退到 0
    log_decision("位点回退 1->0", {"committed": 1, "attempt": 0}, r.json())
    assert r.status_code == 409 and r.json()["error"] == "OFFSET_ROLLBACK"
    assert "回退" in r.json()["detail"]


def test_offset_jump_is_rejected_and_distinct_from_rollback(client):
    _ingest(client, "a"); _ingest(client, "b"); _ingest(client, "c")
    _commit(client, 0)
    r = _commit(client, 2)  # 跳过 1
    log_decision("位点跳跃 0->2", {"committed": 0, "attempt": 2}, r.json())
    assert r.status_code == 409 and r.json()["error"] == "OFFSET_JUMP"
    assert "跳跃" in r.json()["detail"]
    # 被拒后位点仍是 0，允许按正确顺序补提
    assert _commit(client, 1).json()["status"] == "COMMITTED"


def test_duplicate_commit_is_idempotent(client):
    _ingest(client, "a")
    first = _commit(client, 0)
    second = _commit(client, 0)
    log_decision("重复提交同一 offset", {"offset": 0}, {"first": first.json()["status"], "second": second.json()["status"]})
    assert first.json()["status"] == "COMMITTED"
    assert second.json()["status"] == "ALREADY_COMMITTED"
    assert second.json()["committed_offset"] == 0


def test_repeated_fetch_before_commit_replays_then_resumes(client):
    _ingest(client, "a"); _ingest(client, "b"); _ingest(client, "c")
    # 尚未提交：重复拉取得到同一批，体现 at-least-once
    f1 = _fetch(client)
    f2 = _fetch(client)
    ids1 = [e["event_id"] for e in f1.json()["events"]]
    ids2 = [e["event_id"] for e in f2.json()["events"]]
    log_decision("未提交时重复拉取", {"from_offset": f1.json()["from_offset"]}, {"first": ids1, "second": ids2})
    assert ids1 == ids2 == ["a", "b", "c"]

    # 处理完前两条并连续提交，之后只拿到第三条
    _commit(client, 0)
    _commit(client, 1)
    f3 = _fetch(client)
    log_decision("提交后续读", {"committed": 1}, [e["event_id"] for e in f3.json()["events"]])
    assert f3.json()["from_offset"] == 2
    assert [e["event_id"] for e in f3.json()["events"]] == ["c"]


def test_offset_survives_restart(settings: Settings, clock):
    svc = IngestionService(settings=settings, clock=clock)
    c1 = TestClient(create_app(svc))
    _ingest(c1, "a"); _ingest(c1, "b"); _ingest(c1, "c")
    _commit(c1, 0)
    _commit(c1, 1)  # 断点：worker 处理到 offset 1 后服务重启

    svc2 = IngestionService(settings=settings, clock=clock)  # 重新 load 同一 data_dir
    c2 = TestClient(create_app(svc2))
    tail = _fetch(c2)
    ids = [e["event_id"] for e in tail.json()["events"]]
    log_decision("重启断点恢复", {"restart_committed": 1}, ids)
    assert tail.json()["from_offset"] == 2 and ids == ["c"]

    # 重启后仍然拒绝回退
    rb = _commit(c2, 1)
    assert rb.json()["status"] == "ALREADY_COMMITTED"
    rb2 = _commit(c2, 0)
    assert rb2.status_code == 409 and rb2.json()["error"] == "OFFSET_ROLLBACK"

    # 窗口统计没有因重启/重放翻倍
    windows = c2.get("/v1/sources/orders/windows").json()["windows"]
    w = next(w for w in windows if w["count"])
    assert w["count"] == 3 and w["distinct_event_keys"] == 3
    _commit(c2, 2)


def test_consumers_have_independent_offsets(client):
    _ingest(client, "a"); _ingest(client, "b")
    assert _commit(client, 0, consumer="w1").json()["status"] == "COMMITTED"
    assert _commit(client, 0, consumer="w2").json()["status"] == "COMMITTED"
    # w1 推进到 1，w2 不受影响
    assert _commit(client, 1, consumer="w1").json()["status"] == "COMMITTED"
    rb = _commit(client, 0, consumer="w1")
    assert rb.status_code == 409 and rb.json()["error"] == "OFFSET_ROLLBACK"
    f_w2 = _fetch(client, consumer="w2")
    log_decision("不同消费者位点独立", {"w1": 1, "w2": 0}, [e["event_id"] for e in f_w2.json()["events"]])
    assert [e["event_id"] for e in f_w2.json()["events"]] == ["b"]


def test_commit_against_missing_partition(client):
    r = client.post(
        "/v1/sources/orders/partitions/99/commit?consumer_id=w1", json={"offset": 0}
    )
    log_decision("不存在分区提交位点", {"partition": 99}, r.json())
    assert r.status_code == 404 and r.json()["error"] == "PARTITION_NOT_FOUND"
