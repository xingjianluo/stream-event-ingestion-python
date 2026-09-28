# 本地事件接入与实时统计服务（Python / FastAPI）

一套可以完全在本地运行的事件接入与按事件时间实时统计的示例服务。它用**本地文件存储模拟消息通道**
（每个 `(source, partition)` 一条只追加的分区日志），并在其上实现了：

- 事件写入 + 幂等去重（重复投递只计一次，不同来源/分区的相同编号互相区分）；
- 消费位点提交 + 断点恢复（位点只能连续、单调推进，回退/跳跃被明确拒绝）；
- 按事件时间的固定窗口统计 + 迟到事件处理（窗口闭合前后规则确定，不乱序、不静默丢弃）；
- 容量与并发保护（容量满 / 在途请求满时返回**可区分**的拒绝，而不是悄悄丢数据）。

所有时间均为 **UTC 毫秒时间戳**。

---

## 1. 快速开始

```bash
# 安装依赖（需要 uv）
uv sync --group dev

# 直接本地启动（默认 127.0.0.1:8000，状态写入 ./data/state.json）
uv run python main.py
# 或：uv run uvicorn main:app --host 127.0.0.1 --port 8000

# 健康检查
curl -s http://127.0.0.1:8000/v1/health
```

不需要启动脚本、用内嵌 TestClient 的端到端演示（固定时钟，结果可重复，会打印“输入 → 判定”）：

```bash
uv run python scripts/local_demo.py
```

运行测试（日志里会打印每个用例的**输入与判定依据**）：

```bash
uv run pytest -s
```

---

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| `GET`  | `/v1/health` | 健康检查 + 当前容量/在途配置 |
| `POST` | `/v1/events` | 写入单条事件（请求体即事件对象） |
| `POST` | `/v1/events/batch` | 批量写入，请求体 `{"events": [ ... ]}`（**整批原子**） |
| `GET`  | `/v1/sources/{source}/partitions/{partition}/events?consumer_id=&max_events=` | 按消费者位点拉取事件 |
| `POST` | `/v1/sources/{source}/partitions/{partition}/commit?consumer_id=` | 提交消费位点，请求体 `{"offset": n}` |
| `GET`  | `/v1/sources/{source}/windows` | 查看该来源各窗口聚合与 OPEN/SEALED 状态 |

事件对象字段：

| 字段 | 类型 | 必填 | 约束 |
| ---- | ---- | ---- | ---- |
| `source` | string | 是 | 非空，事件来源 |
| `partition` | int | 是 | 非负整数（不能是字符串/布尔/负数） |
| `event_id` | string | 是 | 非空，长度 ≤ `MAX_EVENT_ID_LENGTH`（默认 128） |
| `event_time` | int | 是 | 非负毫秒时间戳；允许 1 秒时钟偏移，更远的“未来时间”拒绝 |
| `value` | int | 否 | 默认 0，窗口对其求和 |

写入成功返回：

```json
{ "status": "ACCEPTED", "offset": 0, "window_start": 1000000, "lateness": false }
```

重复事件返回 **HTTP 200** 但状态为 `DUPLICATE`（重复不是错误，客户端无需重试）：

```json
{ "status": "DUPLICATE", "offset": null, "reason": "同一 (source, partition, event_id) 已计入，幂等忽略" }
```

---

## 3. 幂等规则

- 事件的唯一键是复合键：`source | partition | event_id`。
- **同一键无论经过哪个入口（单条 `/v1/events`、批量 `/v1/events/batch`、甚至同一批里出现多次）、
  无论投递多少次，都只计入一次**：第一次 `ACCEPTED`，之后全部 `DUPLICATE`。
- 相同 `event_id` 但 `source` 不同、或 `partition` 不同，是**不同事件**，分别计数。
- 幂等判定在写日志与窗口聚合**之前**完成；重复事件不分配 offset、不占用容量、不改变窗口。
- 已计入的键集合持久化在 `state.json`，**服务重启后重复投递依旧判重**。
- 窗口聚合内部还维护“已折叠事件键集合”，即使内部发生日志重放，也不会把同一事件算两次。

批量写入是**原子**的：先对整批做重复键探测、窗口闭合校验、容量预估，全部通过才落状态；
只要有一条被硬性拒绝（如窗口闭合），整批拒绝并回滚，不会留下“写入一半”的结果。

---

## 4. 位点语义与断点恢复

位点是**每个消费者**（`consumer_id`）独立维护的，语义为“已连续处理完成的最后一条 offset”：

- 新消费者从 offset `0` 开始；`GET .../events` 返回 `committed+1` 之后的事件。
- **首次提交必须是 0**；之后每次只能提交 `current+1`（逐条连续推进）。
- 提交更小的值 → `409 OFFSET_ROLLBACK`（**明确拒绝回退，绝不从中间重算**）。
- 跳过中间值，或提交超出日志末端的值 → `409 OFFSET_JUMP`。
- 重复提交当前位点 → `200 ALREADY_COMMITTED`（幂等，不重复推进）。
- 对不存在的分区提交 → `404 PARTITION_NOT_FOUND`。

拉取不自动推进位点（at-least-once 风格）：客户端“拉取 → 处理 → 提交位点”。
未提交位点前重复拉取会得到同一批事件；提交后续读只返回新事件。

每次提交成功后位点立即原子落盘。服务重启时从 `state.json` 恢复分区日志、已计事件键、
窗口聚合和全部消费者位点，因此**重启或重复消费后统计不重复、不丢失**，回退依旧被拒绝。

---

## 5. 窗口统计与迟到事件

- 固定窗口大小 `WINDOW_SIZE_SECONDS`（默认 10s），窗口按 epoch 对齐：
  `window_start = floor(event_time / size) * size`。
- 聚合维度为 `(source, window_start)`，跨分区合并；输出 `count / total_value /
  late_events / distinct_event_keys`。
- 允许迟到 `ALLOWED_LATENESS_SECONDS`（默认 30s），定义水位线
  `watermark = now - allowed_lateness`。对一条事件，设其窗口右端为 `window_end`：

| 条件 | 判定 | 处理 |
| ---- | ---- | ---- |
| `window_end > now` | `ON_TIME` | 正常计入，`lateness=false` |
| `watermark < window_end <= now` | `LATE_ACCEPTED` | **计入窗口**，并累加到 `late_events`，`lateness=true` |
| `window_end <= watermark` | `WINDOW_CLOSED` | **HTTP 422 明确拒绝**，不写入、不计入、不占容量 |

- 迟到事件**不会被静默丢弃**：还能补的补进窗口并单独计数；不能补的返回 `WINDOW_CLOSED`
  说明具体是哪个窗口、为何拒绝。
- 窗口查询按当前水位线把窗口标成 `OPEN` / `SEALED`。**SEALED 后窗口结果冻结**，
  再来迟到事件一律 `WINDOW_CLOSED`，结果保持稳定。
- 折叠按事件键去重，因此**乱序到达、重复投递、内部重放都不会把窗口结果算乱**。
- 这些判定只依赖“当前时间”，服务内时钟可注入（测试用 `FakeClock`），行为确定可复现。

---

## 6. 背压与容量策略

任何写入请求先申请一个“在途名额”，再做解析与落库：

- **在途请求数** 达到 `MAX_INFLIGHT_REQUESTS`（默认 128）→
  `503 BACKPRESSURE`，提示客户端稍后重试。名额是**非阻塞**获取，不排队、不丢数据。
- **事件总条数** 达到 `MAX_TOTAL_EVENTS`（默认 10000）→
  `409 CAPACITY_EXCEEDED`。重复事件不占容量，容量满时重复旧事件仍返回 `DUPLICATE`。
- **单批条数** 超过 `MAX_EVENTS_PER_BATCH`（默认 100）→ `413 BATCH_TOO_LARGE`。
- **请求体字节数** 超过 `MAX_BODY_BYTES`（默认 65536）→ `413 BODY_TOO_LARGE`。

所有状态变更都在同一把存储锁内完成，并发写入被串行化，结果确定。

### 拒绝原因一览（错误码可区分）

| HTTP | error code | 含义 |
| ---- | ---------- | ---- |
| 400 | `MALFORMED_JSON` | 空体 / 非 JSON |
| 400 | `MISSING_FIELD` | 缺少必填字段（detail 指出字段名与第几条） |
| 400 | `BAD_FIELD_TYPE` | 字段类型错误（source/event_id/value 等） |
| 400 | `INVALID_BATCH` | 批量入口 `events` 不是数组 |
| 413 | `BODY_TOO_LARGE` | 请求体超字节上限 |
| 413 | `BATCH_TOO_LARGE` | 批量条数超上限 |
| 422 | `INVALID_PARTITION` | partition 不是非负整数 |
| 422 | `EVENT_ID_TOO_LONG` | event_id 超长 |
| 422 | `INVALID_EVENT_TIME` | event_time 非法或来自未来 |
| 422 | `WINDOW_CLOSED` | 窗口已闭合、超过允许迟到时间 |
| 404 | `PARTITION_NOT_FOUND` / `UNKNOWN_CONSUMER` | 分区 / 消费者不存在 |
| 409 | `OFFSET_ROLLBACK` | 位点回退 |
| 409 | `OFFSET_JUMP` | 位点跳跃或越界 |
| 409 | `CAPACITY_EXCEEDED` | 本地存储容量已满 |
| 503 | `BACKPRESSURE` | 在途请求过多，触发背压，请重试 |
| 200 | `DUPLICATE_EVENT`（响应 status=`DUPLICATE`） | 重复事件，已幂等忽略 |

---

## 7. 本地验证方法

### 7.1 单元 / 并发测试

```bash
uv run pytest -s
```

测试组织（共 35 个用例）：

- `tests/test_idempotency.py`：单条/批量/批量内重复、跨来源跨分区、容量满时重复、重启后判重；
- `tests/test_offsets.py`：连续提交、回退、跳跃、越界、重复提交幂等、未提交重复拉取、
  重启断点恢复、多消费者独立位点、不存在分区；
- `tests/test_windows_lateness.py`：正常入窗、乱序聚合、闭合前迟到接受、闭合后拒绝、
  SEALED 稳定、迟到重复只计一次、重启窗口一致；
- `tests/test_validation_backpressure.py`：各类非法形态的可区分拒绝、批量原子性、
  **32 并发重复投递只计一次**、200 并发不同事件全部持久化且 offset 连续、在途名额耗尽触发 503、
  容量满 409 与背压 503 可区分。

每个用例通过 `log_decision(...)` 打印“用例 / 输入 / 判定依据”。

### 7.2 curl 手动验证

```bash
uv run python main.py &            # 启动
B=http://127.0.0.1:8000
NOW=$(python3 -c "import time;print(int(time.time()*1000))")

# 写入 -> 再写一遍（DUPLICATE）
curl -s -X POST $B/v1/events -H 'content-type: application/json' \
  -d "{\"source\":\"shop\",\"partition\":0,\"event_id\":\"e1\",\"event_time\":$NOW,\"value\":10}"
curl -s -X POST $B/v1/events -H 'content-type: application/json' \
  -d "{\"source\":\"shop\",\"partition\":0,\"event_id\":\"e1\",\"event_time\":$NOW,\"value\":10}"

# 拉取 / 提交位点 / 尝试回退
curl -s "$B/v1/sources/shop/partitions/0/events?consumer_id=w1"
curl -s -X POST "$B/v1/sources/shop/partitions/0/commit?consumer_id=w1" \
  -H 'content-type: application/json' -d '{"offset":0}'
curl -s -X POST "$B/v1/sources/shop/partitions/0/commit?consumer_id=w1" \
  -H 'content-type: application/json' -d '{"offset":5}'   # 409 OFFSET_JUMP

# 窗口
curl -s "$B/v1/sources/shop/windows"
```

重启服务（`Ctrl+C` 后再次 `uv run python main.py`）后重复上述投递与拉取，
可观察到重复事件仍判重、位点从磁盘恢复、窗口聚合没有翻倍。

---

## 8. 配置（环境变量）

| 变量 | 默认 | 说明 |
| ---- | ---- | ---- |
| `DATA_DIR` | `data` | 本地状态目录（`state.json` 原子写盘） |
| `WINDOW_SIZE_SECONDS` | `10` | 固定窗口大小 |
| `ALLOWED_LATENESS_SECONDS` | `30` | 允许迟到时间 / 水位线滞后 |
| `MAX_BODY_BYTES` | `65536` | 单请求体字节上限 |
| `MAX_EVENT_ID_LENGTH` | `128` | event_id 字符上限 |
| `MAX_EVENTS_PER_BATCH` | `100` | 单批事件条数上限 |
| `MAX_TOTAL_EVENTS` | `10000` | 本地存储事件总容量 |
| `MAX_CONSUMERS` | `100` | 消费者数量上限（预留） |
| `MAX_INFLIGHT_REQUESTS` | `128` | 并发在途请求上限（背压阈值） |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | 监听地址 |

---

## 9. 代码结构

```
app/
  config.py       # 全部阈值配置（环境变量可覆盖）
  models.py       # 请求/记录/响应数据类型
  errors.py       # 错误码 -> HTTP 状态码映射、ServiceError
  clock.py        # Clock 协议 + SystemClock / FakeClock（测试可控时间）
  storage.py      # 本地 JSON 快照存储：分区日志 / seen / windows / offsets
  validation.py   # 请求解析与字段校验（产出可区分错误码）
  idempotency.py  # 复合事件键与幂等集合
  offsets.py      # 连续单调位点校验与提交
  windows.py      # 固定窗口、水位线、迟到判定、幂等折叠
  service.py      # 编排：原子批量、背压、拉取、提交、持久化
  api.py          # FastAPI 路由
main.py           # 本地启动入口
scripts/local_demo.py  # 免启动的端到端演示
tests/             # 幂等 / 位点 / 窗口迟到 / 校验与背压并发 四组测试
```

> 说明：这是一个教学/演示性质的本地实现，状态保存在单个 JSON 快照里，写入串行化。
> 它刻意保证语义清晰、结果可解释，不面向多节点生产部署。
