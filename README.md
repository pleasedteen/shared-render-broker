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
  server.py      # HTTP/JSON API、SQLite 事务、租约、配额、故障注入
  db.py          # schema 与带计量的 CUD 助手
  meter.py       # 应用层写入计量
  worker.py      # 渲染工作进程
  receiver.py    # 流水线接收/确认进程
  client.py      # 提交、重试、撤销、查询 CLI
  renderer.py    # 基于 fixtures/ 的确定性渲染
fixtures/        # 项目自带输入夹具
verify.py        # 端到端验证
verify.sh        # 一条验证命令
```
