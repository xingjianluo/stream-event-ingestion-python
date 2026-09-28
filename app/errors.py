"""统一的拒绝原因与服务异常。

每个错误码对应一种可区分的失败原因，HTTP 层负责把它映射成状态码，
业务层只抛 ServiceError，保证“失败原因可区分”。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ErrorCode:
    code: str
    http_status: int
    message: str


# 请求形态问题 -> 400
BODY_TOO_LARGE = ErrorCode("BODY_TOO_LARGE", 413, "请求体超过大小限制")
MALFORMED_JSON = ErrorCode("MALFORMED_JSON", 400, "请求体不是合法 JSON")
INVALID_BATCH = ErrorCode("INVALID_BATCH", 400, "批量入口的 events 字段必须是数组")
BATCH_TOO_LARGE = ErrorCode("BATCH_TOO_LARGE", 413, "批量事件条数超过上限")
MISSING_FIELD = ErrorCode("MISSING_FIELD", 400, "事件缺少必填字段")
BAD_FIELD_TYPE = ErrorCode("BAD_FIELD_TYPE", 400, "事件字段类型不正确")
# 语义问题 -> 422
INVALID_PARTITION = ErrorCode("INVALID_PARTITION", 422, "partition 必须为非负整数")
EVENT_ID_TOO_LONG = ErrorCode("EVENT_ID_TOO_LONG", 422, "event_id 超过长度限制")
INVALID_EVENT_TIME = ErrorCode("INVALID_EVENT_TIME", 422, "event_time 非法或来自未来")
WINDOW_CLOSED = ErrorCode("WINDOW_CLOSED", 422, "事件所属窗口已闭合且超过允许迟到时间")
PARTITION_NOT_FOUND = ErrorCode("PARTITION_NOT_FOUND", 404, "指定分区不存在")
OFFSET_ROLLBACK = ErrorCode("OFFSET_ROLLBACK", 409, "位点不能回退")
OFFSET_JUMP = ErrorCode("OFFSET_JUMP", 409, "位点不能跳跃，必须连续提交")
UNKNOWN_CONSUMER = ErrorCode("UNKNOWN_CONSUMER", 404, "消费者不存在")
# 容量与并发 -> 409/503
DUPLICATE_EVENT = ErrorCode("DUPLICATE_EVENT", 200, "重复事件已被幂等忽略")
CAPACITY_EXCEEDED = ErrorCode("CAPACITY_EXCEEDED", 409, "本地存储容量已满")
BACKPRESSURE = ErrorCode("BACKPRESSURE", 503, "系统繁忙，触发背压，请重试")
INTERNAL_ERROR = ErrorCode("INTERNAL_ERROR", 500, "内部错误")


class ServiceError(Exception):
    def __init__(self, error: ErrorCode, detail: str | None = None) -> None:
        super().__init__(detail or error.message)
        self.error = error
        self.detail = detail or error.message


def error_body(error: ErrorCode, detail: str) -> dict:
    return {"error": error.code, "message": error.message, "detail": detail}
