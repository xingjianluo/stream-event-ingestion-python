"""按事件时间的窗口统计与迟到处理测试。

固定窗口 10s，允许迟到 30s（见 conftest.settings）。判定只依赖时钟：
- 事件窗口 end <= now - 30s  : WINDOW_CLOSED（422，明确拒绝，不静默丢弃）；
- now - 30s < end <= now      : LATE_ACCEPTED（接受，计入 late_events）；
- end > now                    : ON_TIME。

覆盖：
1. 正常事件按事件时间进入正确窗口；
2. 乱序到达仍归入事件时间所在窗口，顺序不影响结果；
3. 窗口闭合前后的迟到事件分别“补进窗口”与“被明确拒绝”；
4. 闭合窗口状态稳定：迟到被拒后窗口结果不再变化，SEALED 后依旧；
5. 重复投递迟到事件不会二次计数；
6. 重启后窗口结果一致。
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app
from app.service import IngestionService
from tests.conftest import log_decision


def _event(eid: str, et: int, value: int = 1, source: str = "metrics", partition: int = 0) -> dict:
    return {"source": source, "partition": partition, "event_id": eid, "event_time": et, "value": value}


def _post(client, payload: dict, path: str = "/v1/events"):
    return client.post(path, json=payload)


def _window_map(client, source: str = "metrics") -> dict[int, dict]:
    ws = client.get(f"/v1/sources/{source}/windows").json()["windows"]
    return {w["window_start"]: w for w in ws}


def test_on_time_events_land_in_event_time_window(client, clock):
    # 把时钟推进到 1_005_000，则事件时间 1_001_000 的窗口 end=1_010_000 > now，属正常事件
    clock.set(1_005_000)
    r = _post(client, _event("o1", 1_001_000, value=2))
    assert r.json()["status"] == "ACCEPTED" and r.json()["lateness"] is False
    assert r.json()["window_start"] == 1_000_000
    w = _window_map(client)[1_000_000]
    log_decision("正常事件入窗", _event("o1", 1_001_000), w)
    assert w["state"] == "OPEN" and w["count"] == 1 and w["total_value"] == 2
    assert w["late_events"] == 0


def test_out_of_order_events_aggregate_by_event_time(client, clock):
    # 先到一条较晚事件（仍在当前窗口内），再到一条更早的事件，顺序不影响聚合
    _post(client, _event("late-arrival", 999_000, value=5))
    r = _post(client, _event("early-arrival", 995_000, value=3))
    log_decision("乱序到达", {"first_et": 999_000, "second_et": 995_000}, r.json())
    wm = _window_map(client)
    w990 = wm[990_000]
    assert w990["count"] == 2 and w990["total_value"] == 8
    assert w990["distinct_event_keys"] == 2


def test_late_event_before_close_is_accepted(client, clock):
    # now=1_000_000，watermark=970_000。窗口 [990_000,1_000_000) end=1_000_000 > 970_000 -> 迟到可接受
    r = _post(client, _event("late-ok", 995_000, value=4))
    body = r.json()
    log_decision("窗口闭合前的迟到事件", {"event_time": 995_000, "now": clock.now_ms()}, body)
    assert r.status_code == 200 and body["status"] == "ACCEPTED" and body["lateness"] is True
    w = _window_map(client)[990_000]
    assert w["count"] == 1 and w["total_value"] == 4 and w["late_events"] == 1


def test_late_event_after_close_is_explicitly_rejected(client, clock):
    # 窗口 [960_000,970_000) end=970_000 <= watermark=970_000 -> 永久拒绝
    r = _post(client, _event("too-late", 965_000, value=9))
    log_decision("窗口闭合后的迟到事件", {"event_time": 965_000, "now": clock.now_ms()}, r.json())
    assert r.status_code == 422 and r.json()["error"] == "WINDOW_CLOSED"
    assert "已闭合" in r.json()["detail"]
    # 被拒事件不出现在任何窗口，也不占日志/容量
    assert 960_000 not in _window_map(client)
    assert client.get("/v1/health").json()["total_events"] == 0


def test_sealed_window_result_is_stable(client, clock):
    # 在窗口 [990_000,1_000_000) 写两条（事件时间不晚于当前 now=1_000_000）
    _post(client, _event("a", 998_000, value=2))
    _post(client, _event("b", 999_000, value=3))
    wm = _window_map(client)
    assert wm[990_000]["count"] == 2 and wm[990_000]["total_value"] == 5

    # 推进时间到 1_040_000：watermark=1_010_000，窗口 [990_000,1_000_000) SEALED
    clock.set(1_040_000)
    wm = _window_map(client)
    assert wm[990_000]["state"] == "SEALED"

    # 再投该窗口的事件 -> 明确拒绝；窗口结果保持不变
    r = _post(client, _event("c", 995_000, value=100))
    log_decision("SEALED 窗口拒绝迟到", {"now": 1_040_000, "event_time": 995_000}, r.json())
    assert r.status_code == 422 and r.json()["error"] == "WINDOW_CLOSED"
    wm = _window_map(client)
    assert wm[990_000]["count"] == 2 and wm[990_000]["total_value"] == 5
    assert wm[990_000]["late_events"] == 2


def test_duplicate_late_event_counted_once(client, clock):
    payload = _event("dup-late", 995_000, value=4)
    r1 = _post(client, payload)
    r2 = _post(client, payload)
    log_decision("迟到事件重复投递", payload, {"first": r1.json()["status"], "second": r2.json()["status"]})
    assert r1.json()["lateness"] is True and r2.json()["status"] == "DUPLICATE"
    w = _window_map(client)[990_000]
    assert w["count"] == 1 and w["late_events"] == 1 and w["total_value"] == 4


def test_late_updates_change_open_window_but_not_sealed(client, clock):
    # 先写一条迟到可接受的事件，再推进时间让它进入 SEALED，结果保持
    _post(client, _event("x", 995_000, value=7))
    clock.set(1_010_000)  # watermark=980_000：[990_000,1_000_000) 仍 OPEN
    w = _window_map(client)[990_000]
    assert w["state"] == "OPEN"
    # 再来一条同窗口迟到事件（event_time=996_000，end=1_000_000 > 980_000 仍可接受）
    r = _post(client, _event("y", 996_000, value=1))
    log_decision("OPEN 窗口继续接受迟到", {"now": 1_010_000, "et": 996_000}, r.json())
    assert r.status_code == 200 and r.json()["lateness"] is True
    w = _window_map(client)[990_000]
    assert w["count"] == 2 and w["late_events"] == 2 and w["total_value"] == 8

    clock.set(1_040_000)  # watermark=1_010_000：窗口 SEALED
    assert _window_map(client)[990_000]["state"] == "SEALED"


def test_window_results_survive_restart(settings, clock):
    svc = IngestionService(settings=settings, clock=clock)
    c1 = TestClient(create_app(svc))
    _post(c1, _event("a", 999_000, value=2))
    _post(c1, _event("b", 995_000, value=8))
    _post(c1, _event("a", 999_000, value=2))  # 重复

    svc2 = IngestionService(settings=settings, clock=clock)
    c2 = TestClient(create_app(svc2))
    wm = _window_map(c2)
    log_decision("重启后窗口结果", {"windows": sorted(wm)}, {k: (v["count"], v["total_value"], v["late_events"]) for k, v in wm.items()})
    # 两条事件都落在窗口 [990_000,1_000_000)，其中 b(995_000) 迟到、a(999_000) 也迟到
    w = wm[990_000]
    assert w["count"] == 2 and w["total_value"] == 10 and w["late_events"] == 2
