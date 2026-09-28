"""请求解析与字段校验：把各种非法形态翻译成可区分的拒绝原因。"""

from __future__ import annotations

import json
from typing import Any

from app.config import Settings
from app.errors import (
    BATCH_TOO_LARGE,
    BODY_TOO_LARGE,
    INVALID_BATCH,
    INVALID_EVENT_TIME,
    INVALID_PARTITION,
    MALFORMED_JSON,
    MISSING_FIELD,
    BAD_FIELD_TYPE,
    EVENT_ID_TOO_LONG,
    ServiceError,
)
from app.models import IngestEvent

_REQUIRED_FIELDS = ("source", "partition", "event_id", "event_time")
_ALLOWED_CLOCK_SKEW_MS = 1000


def parse_body(raw: bytes, settings: Settings) -> Any:
    """原始字节 -> JSON 对象；区分超大请求与非法 JSON。"""
    if len(raw) > settings.max_body_bytes:
        raise ServiceError(BODY_TOO_LARGE, f"body 大小 {len(raw)} 字节，超过 {settings.max_body_bytes}")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ServiceError(MALFORMED_JSON, f"无法解析为 JSON: {exc}") from exc


def parse_events(payload: Any, settings: Settings, now_ms: int) -> list[IngestEvent]:
    """解析单条或批量入口的 payload，返回事件列表。

    单条入口：payload 是事件对象；
    批量入口：payload 形如 {"events": [ ... ]}。
    """
    if isinstance(payload, dict) and "events" in payload:
        raw_events = payload["events"]
        if not isinstance(raw_events, list):
            raise ServiceError(INVALID_BATCH, f"events 字段类型为 {type(raw_events).__name__}，应为数组")
        if len(raw_events) > settings.max_events_per_batch:
            raise ServiceError(
                BATCH_TOO_LARGE,
                f"批量 {len(raw_events)} 条，超过单批上限 {settings.max_events_per_batch}",
            )
        items = raw_events
    elif isinstance(payload, dict) and any(field in payload for field in _REQUIRED_FIELDS):
        items = [payload]
    else:
        raise ServiceError(MALFORMED_JSON, "载荷既不是事件对象，也不包含 events 数组")
    return [_parse_one(item, i, settings, now_ms) for i, item in enumerate(items)]


def _where(index: int | None) -> str:
    return "" if index is None else f"第 {index} 条事件: "


def _parse_one(item: Any, index: int | None, settings: Settings, now_ms: int) -> IngestEvent:
    where = _where(index)
    if not isinstance(item, dict):
        raise ServiceError(BAD_FIELD_TYPE, f"{where}事件必须是对象，实际为 {type(item).__name__}")

    # 必填字段存在性
    for name in _REQUIRED_FIELDS:
        if name not in item:
            raise ServiceError(MISSING_FIELD, f"{where}缺少必填字段 '{name}'")
        if item[name] is None:
            raise ServiceError(MISSING_FIELD, f"{where}字段 '{name}' 不能为 null")

    source = item["source"]
    partition = item["partition"]
    event_id = item["event_id"]
    event_time = item["event_time"]
    value = item.get("value", 0)

    # 类型校验：bool 是 int 的子类，显式排除
    if not isinstance(source, str) or not source:
        raise ServiceError(BAD_FIELD_TYPE, f"{where}source 必须是非空字符串")
    if not isinstance(partition, int) or isinstance(partition, bool) or partition < 0:
        raise ServiceError(INVALID_PARTITION, f"{where}partition 必须为非负整数，实际为 {partition!r}")
    if not isinstance(event_id, str) or not event_id:
        raise ServiceError(BAD_FIELD_TYPE, f"{where}event_id 必须是非空字符串")
    if len(event_id) > settings.max_event_id_length:
        raise ServiceError(
            EVENT_ID_TOO_LONG,
            f"{where}event_id 长度 {len(event_id)}，超过上限 {settings.max_event_id_length}",
        )
    if not isinstance(event_time, int) or isinstance(event_time, bool) or event_time < 0:
        raise ServiceError(
            INVALID_EVENT_TIME, f"{where}event_time 必须为非负整数毫秒时间戳，实际为 {event_time!r}"
        )
    # 允许 1 秒时钟偏移；再往未来视为非法
    if event_time > now_ms + _ALLOWED_CLOCK_SKEW_MS:
        raise ServiceError(
            INVALID_EVENT_TIME,
            f"{where}event_time={event_time} 晚于当前时间 {now_ms}（超过 1 秒时钟偏移容忍）",
        )
    if not isinstance(value, int) or isinstance(value, bool):
        raise ServiceError(BAD_FIELD_TYPE, f"{where}value 必须为整数，实际为 {value!r}")

    return IngestEvent(
        source=source,
        partition=partition,
        event_id=event_id,
        event_time=event_time,
        value=value,
    )
