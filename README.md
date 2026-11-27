# 本地共享渲染队列（Python + SQLite）

多条流水线对同一素材版本和参数的重复请求会加入同一个渲染轮次：只计算一次、
SQLite 中只保存一份成果正文，各流水线分别领取并确认投递。

仅使用 Python 标准库；不需要安装第三方依赖。

## 一条验证命令

```bash
./verify.sh
```

也可以直接运行：

```bash
python3 verify.py
```

验证程序会用 `fixtures/` 中自带的输入夹具，在临时目录创建 SQLite 数据库、启动
真实的队列服务/渲染工作进程/流水线接收进程，并在通常约 12 秒、最多 80 秒内输出逐项
检查结果。覆盖：

- 同素材、同版本、同参数的跨流水线共享计算；
- 已保存成果的迟到订阅直接复用；
- 接收方积压与排空；
- 接收方停机时工作进程仍继续处理其他计算；
- 计算租约到期、其他工作进程接手、旧凭据不能续期/提交；
- 投递租约续期、到期、同流水线重领且成果 SHA-256 不变；
- 服务在成果提交事务完成后、响应发出前硬退出，重启后继续投递；
- `acked` / `revoked` 状态及原请求号幂等重试在重启后不变；
- 按流水线 + 原请求号排查卡单：轮次历次计算领取/换手/成果保存，与该订阅历次
  投递领取/换手/服务端确认/接收端落盘全链路可查，重启后历史仍在；
- 接收端离线落盘后重启按原领取补报，同一领取只显示一条落盘记录，补报不触碰
  租约、归属、确认与撤销；
- 确认响应丢失时按服务端已提交状态显示，并区分「服务端已确认」与「接收端已落盘」；
- 最后一个订阅终结后，同条件新请求另起一轮；
- 每条流水线可配置的未结订阅上限，满额只对该流水线返回 `429`；
- 应用层增删改记录数和写入字节数计量；
- `N=1/6/20` 时，成果完成请求均只写 `rounds` 的 1 条记录、字节数相同。

## 手动启动

以下命令都在项目根目录执行。队列服务、工作进程和接收进程可以分别在不同终端
启动；任意进程都可以独立停止和重启。

### 1. 启动 SQLite 队列服务

```bash
python3 -m render_queue.server \
  --host 127.0.0.1 \
  --port 8765 \
  --db data/queue.db \
  --fixtures-dir fixtures \
  --default-open-quota 8 \
  --pipeline-quota demo=2 \
  --compute-lease 30 \
  --delivery-lease 60
```

`--pipeline-quota 流水线名=上限` 可重复指定；未单独配置的流水线使用
`--default-open-quota`。

### 2. 启动渲染工作进程

```bash
python3 -m render_queue.worker \
  --server http://127.0.0.1:8765 \
  --worker-id renderer-1 \
  --fixtures fixtures
```

可启动多个工作进程。工作进程限时领取计算，并在长渲染期间自动续期；租约到期后，
其他工作进程可以接手。

### 3. 启动流水线接收进程

每条流水线启动自己的接收进程（同一流水线也可启动多个实例做重启/切换）：
重复启动示例命令前，若想清空手动演示数据，可先停止服务并删除 `data/`；请求号在
同一流水线内是幂等键，保留数据库时重复请求会返回原订阅。

```bash
python3 -m render_queue.receiver \
  --server http://127.0.0.1:8765 \
  --pipeline-id demo \
  --receiver-id receiver-demo-1 \
  --output-dir received
```

接收进程领取本流水线的投递、校验 SHA-256、保存到
`received/<pipeline>/<request-id>.rnd`，然后用租约凭据确认。处理耗时长于续期间隔
时会自动续期。

每次落盘都会写入持久 journal 并向服务端补报「接收端已落盘」；服务不可达时由后台
重放循环和下次启动自动补报。`--offline-persist` 用于演练离线场景：接收端在成果与
journal 落盘后、确认前直接退出，重启后自动按原领取补报并重新领取到期租约。

### 4. 提交和重试请求

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  submit \
  --pipeline demo \
  --request-id req-0001 \
  --asset harbor-title \
  --version 1.0.0 \
  --param scene=harbor \
  --param width=640
```

用完全相同的 `--pipeline` 和 `--request-id` 再执行一次，会返回原订阅和当前状态，
不会创建第二条订阅。

撤销订阅：

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  revoke --pipeline demo --request-id req-0001
```

查询状态：

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  get --pipeline demo --request-id req-0001
```

排查卡单（按流水线 + 原请求号返回完整事件链路）：

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  trace --pipeline demo --request-id req-0001
```

按轮次查看该轮所有订阅的事件链路：

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  round-trace --round-id 12
```

接收端离线落盘、重启联网后由接收进程自动补报；也可用原领取凭据手动补报：

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  report-persisted --subscription-id 7 --fence 1 --token <原领取 token>
```

查看累计写入计量：

```bash
python3 -m render_queue.client \
  --server http://127.0.0.1:8765 \
  write-metrics
```

## 关键语义

### 轮次与订阅

轮次键为：

```text
(asset_id, asset_version, canonical(params), fixture_manifest_hash)
```

只要该键对应的轮次仍有 `open` 订阅，新请求就加入该轮；若轮次已经 `saved`，新订阅
立即变成可投递状态并复用同一份 `result_body`。

最后一个订阅 `ack` 或 `revoke` 后，轮次进入 `closed`。SQLite 部分唯一索引保证同一
条件最多只有一个 `queued/saved` 活动轮次；之后的同条件请求会创建新轮次。

### 租约与凭据

- 计算租约字段在 `rounds`：`compute_token`、`compute_fence`、
  `compute_lease_expires_at`、`compute_attempts`。
- 投递租约字段在 `subscriptions`：`delivery_token`、`delivery_fence`、
  `delivery_lease_expires_at`、`delivery_attempts`。
- 续期、提交成果和确认都必须携带当前 token，且租约不能到期。
- 到期后的条件更新允许新的工作进程或同流水线接收进程领取；旧 token 会得到
  `401` / `409` / `410`，不能覆盖新成果或改变终态。

### 重启与响应丢失

成果提交在一个 SQLite 事务中更新 `rounds` 行。验证程序通过
`--enable-faults` 打开测试注入：服务可在事务提交后、发送 HTTP 响应前 `_exit`。
重启同一 DB 文件后，已提交的 `saved` 轮次和 `acked/revoked` 订阅仍保持原状，接收
进程仍可领取并确认该唯一成果。

确认（ack）同样支持响应丢失注入：服务在 ack 事务提交后、响应发出前 `_exit`。
重启后排查接口与状态查询都按服务端已提交的 `acked` 状态显示，接收端用同一凭据
重试得到幂等应答，不会产生重复确认事件。

### 卡单排查与落盘补报

排查数据落在两张只增不改的持久表，服务重启后仍可查询，且不暴露任何租约 token：

- `round_events`（轮次侧）：`compute_claim`（首次领取）、`compute_handoff`
  （换手，即租约到期后被其他工作进程领取）、`result_saved`（成果保存）。
- `delivery_events`（订阅侧）：`delivery_claim`（首次投递领取）、
  `delivery_handoff`（投递换手）、`receiver_persisted`（接收端已落盘）、
  `delivery_acked`（服务端已确认）。

排查接口：

- `GET /v1/pipelines/{pipeline}/requests/{request}/trace`：按流水线 + 原请求号
  返回所属轮次的轮次侧事件，以及该订阅的全部投递侧事件，并给出 `summary`
  （领取/换手次数、落盘对应的领取 fence、确认数、`stuck_status` 等）。
- `GET /v1/rounds/{id}/trace`：按轮次返回轮次侧事件及该轮每个订阅的投递侧事件。

`summary.stuck_status` 明确区分两类事实：

- `server_confirmed` 来自订阅行的持久状态（服务端是否已确认）；
- `receiver_persisted` 来自接收端显式上报的落盘事件（接收端是否已落盘）。
  例如 `persisted_pending_ack` 表示成果已在接收端磁盘、但服务端尚未确认；
  换手前的落盘记录按当时的领取 fence 单独保留。

接收端在每次落盘时先原子写成果、收据和 `.journal/` 持久日志（含原领取凭据），
再向服务端上报 `receiver_persisted`。离线（或使用 `--offline-persist` 模拟落盘后
断网退出）时上报留在 journal 中，重启联网后由
`POST /v1/deliveries/{id}/persisted` 按原领取自动补报：

- 部分唯一索引保证「同一订阅 + 同一领取 fence」最多一条落盘记录，重复补报返回
  `replayed: true`，同一次领取的落盘记录始终只显示一条；
- 补报是纯追加，不更新 `subscriptions` / `rounds`，因此不影响现有租约、投递归属，
  也不影响订阅的确认与撤销（撤销后补报仍幂等，订阅保持 `revoked`）；
- 补报凭据按原领取事件中的 token 校验，凭据不符返回 `401`，未知 fence 返回 `404`。

## 写入计量

`render_queue/meter.py` 对所有经过 `Database` 写入助手的 INSERT/UPDATE/DELETE：

- 累计 CUD 语句数；
- 累计受影响记录数；
- 累计实际应用值字节数：
  - TEXT 按 UTF-8 字节；
  - BLOB 按原始字节；
  - 数字/布尔按固定 8/1 字节应用标量计量；
  - NULL 为 0；
- 每次改写都重新累计；
- 更新变大的记录时，新写入值字节全额计入；
- 删除影响的记录数计入，并额外提供被删行镜像字节数供审计。

SQLite 页、WAL、checkpoint 的物理写入不在该指标内。

每个 HTTP 响应还带有本次请求范围的 `request_write_metrics`，以及
`X-Write-Records`、`X-Write-App-Bytes` 响应头。`./verify.sh` 据此断言：无论同轮
未结订阅是 1、6 还是 20，工作进程提交成果的请求都只更新 1 条 `rounds` 记录，且
应用值字节数完全相同。

## 目录

```text
render_queue/
  server.py      # HTTP/JSON API、SQLite 事务、租约、配额、故障注入、卡单排查
  db.py          # schema（含 round_events/delivery_events）与带计量的 CUD 助手
  meter.py       # 应用层写入计量
  worker.py      # 渲染工作进程
  receiver.py    # 接收/确认进程、落盘 journal 与离线补报
  client.py      # 提交、重试、撤销、查询、排查 trace、手动补报 CLI
  renderer.py    # 基于 fixtures/ 的确定性渲染
fixtures/        # 项目自带输入夹具
verify.py        # 端到端验证
verify.sh        # 一条验证命令
```

接收端的持久日志位于输出目录下的 `<output-dir>/<pipeline>/.journal/`，每个文件
对应一次原始领取（`sub-<订阅id>-fence-<fence>.json`），记录原领取凭据与补报状态。
