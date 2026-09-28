"""本地端到端演示：不依赖外部服务，直接用 TestClient 走全部 HTTP 入口。

运行：uv run python scripts/local_demo.py
会逐步打印“输入 -> 判定/依据”，覆盖幂等、位点、迟到、背压四类语义。
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

# 允许直接 `uv run python scripts/local_demo.py`：把项目根加入 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient  # noqa: E402

from app.api import create_app  # noqa: E402
from app.clock import FakeClock  # noqa: E402
from app.config import Settings  # noqa: E402
from app.service import IngestionService  # noqa: E402


def show(title: str, resp) -> None:
    body = resp.json()
    print(f"\n### {title}\n-> HTTP {resp.status_code} {body}")


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="stream-demo-"))
    clock = FakeClock(now_ms=1_000_000)  # 固定时钟，演示可重复
    settings = Settings(data_dir=tmp, window_size_seconds=10, allowed_lateness_seconds=30)
    svc = IngestionService(settings=settings, clock=clock)
    c = TestClient(create_app(svc))

    base = "/v1/events"
    e1 = {"source": "shop", "partition": 0, "event_id": "evt-1", "event_time": 999_000, "value": 10}

    show("正常写入（迟到可接受窗口）", c.post(base, json=e1))
    show("重复投递同一事件 -> DUPLICATE", c.post(base, json=e1))
    show(
        "相同 event_id、不同分区 -> 视为不同事件",
        c.post(base, json={**e1, "partition": 1}),
    )
    show(
        "缺少字段 -> MISSING_FIELD(400)",
        c.post(base, json={"source": "shop", "partition": 0, "event_id": "x"}),
    )
    show(
        "窗口闭合后的迟到事件 -> WINDOW_CLOSED(422)",
        c.post(base, json={**e1, "event_id": "old", "event_time": 900_000}),
    )
    show(
        "批量写入：一条新事件 + 一条重复",
        c.post(
            f"{base}/batch",
            json={"events": [{**e1, "event_id": "evt-2"}, e1]},
        ),
    )

    show("消费者拉取（从 offset 0）", c.get("/v1/sources/shop/partitions/0/events?consumer_id=w1"))
    show("提交位点 0 -> COMMITTED", c.post(
        "/v1/sources/shop/partitions/0/commit?consumer_id=w1", json={"offset": 0}))
    show("重复提交 0 -> ALREADY_COMMITTED", c.post(
        "/v1/sources/shop/partitions/0/commit?consumer_id=w1", json={"offset": 0}))
    show("跳跃提交 2 -> OFFSET_JUMP(409)", c.post(
        "/v1/sources/shop/partitions/0/commit?consumer_id=w1", json={"offset": 2}))
    show("回退提交 0 -> 此刻为 ALREADY_COMMITTED（同值幂等）", c.post(
        "/v1/sources/shop/partitions/0/commit?consumer_id=w1", json={"offset": 0}))

    clock.set(1_050_000)  # 推进时间，让旧窗口 SEALED
    show("推进时钟后查看窗口（旧窗口 SEALED）", c.get("/v1/sources/shop/windows"))
    show(
        "SEALED 窗口再补事件 -> WINDOW_CLOSED，不静默丢弃",
        c.post(base, json={**e1, "event_id": "late", "event_time": 995_000}),
    )

    print(f"\n演示数据目录：{tmp}（演示结束，可自行删除）")


if __name__ == "__main__":
    main()
