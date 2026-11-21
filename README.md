# 只读查询 API（带守护的 PostgreSQL 连接池）

数据库值班场景：多个应用共用有限的 PostgreSQL 连接。客户端等慢 SQL 超时后，
查询可能仍占着会话；未清理的事务也可能影响下一个请求。本服务用
FastAPI + 自研异步连接池解决这些问题：

- 每个请求可**分别设置**“等待连接的超时”（`acquire_timeout`）和
  “执行查询的超时”（`query_timeout`）；
- 排队中的请求被取消后**立即释放队列名额**（排队不占用连接）；
- 查询超时、客户端断开、SQL 报错后，连接标记为“脏”，**在确认会话
  状态已清理之前绝不再借出**：先做服务端取消（`pg_cancel_backend` 等价的
  CancelRequest）、回滚遗留事务、再用 `SELECT 1` 验证；无法确认的一律
  **关闭并更换新连接**；
- 连接数有可在线调整的目标：每条连接始终属于一个槽位，缩容只关闭多余
  的空闲连接，已借出的查询照常跑完；建连与脏连接清理结束后按最新目标
  决定留用还是关闭；建连失败时可用连接继续服务，恢复后自动补足；
- 会话级只读（`default_transaction_read_only=on`），本 API 只暴露
  固定的 SELECT 查询，不执行任意 SQL。

## 快速开始

```bash
docker compose up --build
```

- API 文档（Swagger UI）：http://localhost:8000/docs
- 连接池指标：http://localhost:8000/pool/stats
- PostgreSQL：`localhost:5432`，用户/密码/库均为 `app`/`app`/`demo`

连接池的**初始**目标连接数用环境变量 `POOL_SIZE` 控制（默认 4，见
`docker-compose.yml`）。运行中可通过 `POST /pool/target` 调整，无需重启。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/query/normal` | 普通查询（`SELECT ... FROM items LIMIT n`） |
| POST | `/query/slow` | 延时查询（`SELECT pg_sleep(n)`），模拟慢 SQL |
| POST | `/query/error` | 报错查询：`autocommit` 单语句报错；`transaction` 在 `BEGIN` 后报错，故意留下失败事务 |
| GET | `/pool/stats` | 连接池指标：`target`（目标）、`opened_connections`（当前打开）、`in_use`、`idle`、`cleaning`、`connecting`、`waiting`、`target_reached` 及各类计数器 |
| POST | `/pool/target` | 设置目标连接数，请求体 `{"target": 2}`（正整数） |
| POST | `/demo/burst` | 后台并发发起 n 个慢查询，便于在 `/pool/stats` 观察排队 |

查询类接口的请求体都支持 `acquire_timeout` / `query_timeout`（秒，`null` 表示不限）。

响应码约定：`200` 成功；`503` 等连接超时；`504` 查询执行超时；
`400` SQL 错误；`502` 连接级故障（如后端被杀）；`499` 客户端中途断开；
`422` `/pool/target` 参数非法（非整数、0、负数）。

## 在线调整连接数

`POST /pool/target {"target": N}`（N 为正整数，校验用 JSON 严格整数，
`4.0` / `"4"` / `true` 都会被拒绝）：

- **缩容**：接口返回前先关闭多余的空闲连接，所以返回体里的
  `opened_connections` 通常已等于新目标；仍被借出的连接等查询照常
  跑完后直接关闭，不打断请求。打开数仍高于目标期间，**暂停新借出和
  补开**，排队请求在收缩完成（或达到 `acquire_timeout`）后才继续。
- **扩容 / 补足**：由后台任务异步建连，串行、一次最多一条在途；
  `target_reached` 变为 `true` 表示已补齐。建连失败按指数退避重试，
  已打开的连接不受影响、继续服务，数据库恢复后自动补足。
- **进行中的建连也占名额**：建连完成时按**最新接受的目标**决定留用
  还是关闭；脏连接清理结束同理。
- 查询超时 / 取消仍按原有逻辑处理，不因调整目标而改变。
- 计数器：`resizes`（调整次数）、`closed_for_resize`（因缩容关闭的
  连接数）、`connect_failures`（后台建连失败次数）、`opened_total`
  （累计打开数）。

`/pool/stats` 的 `size` 字段保留为 `target` 的别名，兼容旧调用方。

## 在 /docs 里演示

1. **排队**：先 `POST /demo/burst`（默认 8 个 3 秒慢查询，池只有 4 条
   连接），立刻反复 `GET /pool/stats`，可看到 `in_use=4, waiting=4`。
2. **等连接超时**：池被 burst 占满时，`POST /query/slow` 设
   `acquire_timeout=1`，约 1 秒后返回 `503`。
3. **查询超时**：`POST /query/slow` 设 `delay_seconds=10, query_timeout=1`，
   约 1 秒后返回 `504`；`/pool/stats` 里 `cleaning` 短暂为 1（服务端
   取消 + 验证会话），随后恢复，`counters.recycled` 或 `replaced` 加一。
4. **SQL 报错**：`POST /query/error`，`mode=transaction` 会留下失败
   事务；连接池回滚并验证后才复用（`counters.recycled` 加一）。
5. **客户端取消**：用 `curl` 发一个慢查询后按 `Ctrl-C` 中断，
   `/pool/stats` 的 `waiting`/`in_use` 会立刻回落，
   `counters.queue_cancelled` 或 `client_disconnects` 加一。
6. **在线缩容**：先 `POST /pool/target {"target": 6}` 等
   `target_reached=true`，再发几个慢查询，马上
   `POST /pool/target {"target": 2}`：空闲连接立即关闭，
   慢查询仍返回 `200`，`/pool/stats` 中 `opened_connections` 随查询
   归还降到 2，期间新请求排队。
7. **扩容 / 建连失败恢复**：`POST /pool/target` 调大目标后观察
   `connecting` 与 `opened_connections` 逐步达到目标；若数据库暂时
   拒绝新连接，`connect_failures` 增长但已有查询照常成功，恢复后
   `target_reached` 自动变为 `true`。

## 自测

```bash
pip install -r tests/requirements.txt
# 可选：提供 psql 连接串后，会额外做服务端核对
# （连接数不超过目标、无残留活跃查询、无失败事务）
export PSQL="postgresql://app:app@127.0.0.1:5432/demo"
# 可选：注入“拒绝新连接、已有会话不受影响”的故障，验证建连失败后恢复；
# 不设置则跳过该用例
export DB_REJECT_CMD='docker compose exec -T db psql -U app -d postgres -c "ALTER DATABASE demo WITH CONNECTION LIMIT 0"'
export DB_RESTORE_CMD='docker compose exec -T db psql -U app -d postgres -c "ALTER DATABASE demo WITH CONNECTION LIMIT -1"'
python tests/selftest.py
```

覆盖：普通查询、目标数参数校验、排队与并发、等连接超时、排队取消
释放名额、查询超时后的连接清理/更换、两类 SQL 报错后的复用、客户端
断开、压力测试，以及**动态目标**：空闲缩容、查询进行中缩容（不打断
请求）、缩容扩容交错、建连失败时继续服务并恢复后补足、建连期间
目标调低后不多开。全程校验
`idle + in_use + cleaning == opened_connections`，且打开数不超过
见过的最大目标。

## 关键设计

- **槽位模型**：每条连接处于 `idle / in_use / cleaning` 三态之一，
  另用 `connecting` 记录正在建立、尚未打开的连接；三者之和恒等于
  `opened_connections`。缩容先关空闲连接，已借出的等归还时关闭，
  因此不会打断查询，也不会把打开数建到目标之上。
- **目标驱动**：`target` 是最新接受的目标。后台管理器串行建连补齐
  缺口；建连 / 清理完成时再读一次 `target`，若期间被调低就关闭新连接。
  `opened > target` 期间 `acquire()` 暂停借出，归还的干净连接直接关闭。
- **排队不持名额**：`acquire()` 的等待者只挂在条件变量上，被取消
  （如客户端断开）时从队列移除即可，不会泄漏连接。
- **脏连接不出借**：归还时若连接可能仍在执行查询（超时/断开）或
  刚发生 SQL 错误，则转入后台清理：先发服务端取消并等待查询任务
  收尾，再回滚遗留事务，最后 `SELECT 1` 验证会话空闲；任何一步失败
  即关闭，由后台按目标补开（不是无条件立即新建）。
- **借出前把关**：借出时先做本地状态检查；空闲超过
  `checkout_verify_after` 秒（默认 10）的连接额外做一次 `SELECT 1`
  活性验证，防止数据库重启后把已死的空闲连接借给请求。

## 已知限制

- 连接关闭失败（如对端无响应且超时）时，客户端按已关闭记账，服务端
  可能短暂残留连接，依赖 PostgreSQL 的 TCP keepalive 最终回收；
  `/pool/stats` 的 `opened_connections` 是客户端视角。
- 排队唤醒不保证严格 FIFO（`asyncio.Condition` 语义），极端情况下
  可能有请求等待偏久，但都会在自己的 `acquire_timeout` 内得到确定结果。
- 单进程单事件循环；多副本部署时每个进程各有自己的连接池，
  总连接数 = 副本数 × 各进程的 `target`，需要值班员按此规划。
  多副本时应给每个副本分别调用 `/pool/target`。
- 缩容只保证“不再多开、尽快收缩”，不会强行中断已借出的查询：
  若某个慢查询迟迟不结束，`opened_connections` 会在其归还前暂时
  高于 `target`（此时 `target_reached=false`，新借出暂停）。
- 后台建连是串行的（一次最多一条在途），大规模扩容（如从 1 调到
  100）需要逐个建立，达成目标的时间与建连耗时成正比；期间
  `/pool/stats` 的 `connecting` 可见进度。
- 活性验证只在“借出时”进行；验证通过与实际执行之间的极短窗口内
  连接仍可能失效（此时该请求报错、连接被更换，属正常兜底路径）。
