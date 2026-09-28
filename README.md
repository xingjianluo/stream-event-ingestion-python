# stream-event-ingestion

本地可运行的事件接入与实时统计服务。用 SQLite 模拟消息通道（append-only 日志），
提供幂等写入、消费位点提交与断点恢复、按事件时间的窗口统计与迟到处理。

## 运行

```bash
uv run python main.py          # 默认 :memory:，监听 127.0.0.1:8000
SEI_DB_PATH=/tmp/sei.db uv run python main.py   # 落盘，重启可恢复
```

配置（环境变量）：

| 变量 | 默认 | 含义 |
|---|---|---|
| `SEI_DB_PATH` | `:memory:` | SQLite 路径；所有状态都在这一个文件里 |
| `SEI_WINDOW_SIZE_MS` | `60000` | 滚动窗口大小（事件时间，毫秒） |
| `SEI_ALLOWED_LATENESS_MS` | `10000` | 允许迟到时长，决定水位线 |
| `SEI_MAX_EVENTS` | `100000` | 日志容量上限，超过即背压拒绝 |

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/events` | 写入事件（幂等去重） |
| GET | `/v1/events?after_seq=&limit=` | 从模拟通道按位点顺序消费 |
| POST | `/v1/offsets` | 提交消费位点 |
| GET | `/v1/offsets?consumer=&source=&partition=` | 查询已提交位点 |
| GET | `/v1/windows?source=` | 查询窗口统计 |

事件格式：`{"event_id", "source", "partition", "event_time", "payload"}`，
其中 `event_time` 为事件时间（epoch 毫秒）。

## 幂等规则

- 事件身份 = `(source, partition, event_id)`。同一事件重复投递只会计入一次：
  首次返回 `201 {"status":"accepted","seq":N}`，重放返回 `200 {"status":"duplicate","seq":N}`（同一个 seq）。
- 不同 source 或 partition 下的相同 `event_id` 是**不同**事件，各自计数。
- 去重先于窗口统计，且日志追加 + 去重 + 窗口更新在**同一个事务**里提交：
  重复投递、服务重启都不会造成重复计数或统计丢失。
- 拒绝原因可区分（HTTP 状态码 + `reason` 字段）：

| 场景 | 状态码 | reason |
|---|---|---|
| 非法 JSON / 非对象 | 400 | `malformed_json` |
| 缺字段 | 400 | `missing_field` |
| 字段类型/取值非法 | 400 | `invalid_field` |
| 请求体超过 16 KiB | 413 | `payload_too_large` |
| 日志容量已满 | 503 | `backpressure` |

## 位点语义

- 位点 = 下一条要消费的 `seq`（Kafka 风格），即"seq < offset 的都已处理"。
- 每个 `(consumer, source, partition)` 独立维护一份位点。
- 规则（违反即 `409`，绝不从中间继续算）：
  - 首次提交必须是 `0`——消费者不得从日志中间开始（否则 `offset_gap`）；
  - 提交值低于已存位点属于回退（`offset_regression`）；
  - 提交值超过日志末尾 `max_seq + 1` 属于跳跃（`offset_gap`）；
  - 重复提交相同位点是幂等 no-op，返回 `200`。
- 断点恢复：位点、事件日志、窗口统计都在同一个 SQLite 文件中，
  重启后重放旧事件只会得到 `duplicate`，统计不重不漏。

## 迟到事件与窗口

- 按 source 维护水位线：`watermark = max(event_time) - SEI_ALLOWED_LATENESS_MS`，只前进不后退。
- 窗口 `[start, end)` 在 `end <= watermark` 时闭合（`closed=true`）。
- 稳定规则（不静默丢弃、不算乱已闭合窗口）：
  - 窗口未闭合：事件计入 `count`；
  - 窗口已闭合：事件计入该窗口的 `late_count`，`count` 冻结不变；
  - 极度迟到（对应窗口尚不存在）：新建一行并立即闭合，记入 `late_count`。
- 乱序到达与重复触发不会改变已闭合窗口的 `count`；重复迟到事件经去重后只计一次。

## 背压策略

容量吃紧时宁可明确拒绝也不悄悄丢数据：

- 日志条数达到 `SEI_MAX_EVENTS` 后，新事件返回 `503 backpressure`，
  写入整体回滚（日志与窗口统计都不留残渣）；
- 容量满时重放已存在的事件仍返回 `duplicate`（幂等保证不受背压影响）；
- 容量检查与追加在写锁内串行执行，并发写入不会突破上限。

## 本地验证

```bash
uv run pytest -q                      # 全部测试
uv run pytest -q --log-cli-level=INFO # 查看每个用例的输入与判定依据
```

测试覆盖：重复投递、非法请求的可区分拒绝、位点回退/跳跃、跨窗口迟到、
乱序与重复触发下的窗口稳定性、重启恢复、并发写入与容量背压。

手工验证示例：

```bash
SEI_DB_PATH=/tmp/sei.db uv run python main.py &
curl -X POST localhost:8000/v1/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","source":"s","partition":0,"event_time":1000,"payload":"x"}'
curl -X POST localhost:8000/v1/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","source":"s","partition":0,"event_time":1000,"payload":"x"}'  # duplicate
curl -X POST localhost:8000/v1/offsets -H 'Content-Type: application/json' \
  -d '{"consumer":"c1","source":"s","partition":0,"offset":0}'
curl 'localhost:8000/v1/windows?source=s'
```
