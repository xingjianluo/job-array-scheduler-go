# job-array-scheduler-go

单机可运行的作业数组（批量作业）调度与执行模拟。用**本地文件持久化 + 进程内工作者 +
可模拟崩溃的任务**复刻真实集群的工作负载管理：批量屏障准入、并发领取与 fencing、
失败重试/退避、超时、取消、租约过期恢复与"副作用恰好一次"。无任何外部依赖，
`go test ./...` 与 `go run ./cmd/demo` 即可运行。

## 1. 快速开始

```bash
go test ./...                 # 全部场景测试（含 -race）
go test -race ./...           # 并发安全
go run ./cmd/demo             # 确定性端到端演示，打印判定依据与分类汇总
```

演示结束时会对总分类做强断言（例如
`total=9 succeeded=4 failed=1 timed_out=1 canceled=3`），
多次运行结论保持一致。

## 2. 包结构

| 包 | 职责 |
|---|---|
| `internal/domain` | 状态机类型：作业/组/尝试/事件、策略、配置 |
| `internal/store` | 并发安全的持久化协调层：快照 + 追加事件日志、领取/fencing、恢复 |
| `internal/task` | 本地模拟任务（ok/fail/flaky/sleep/panic/crash）与**副作用台账** |
| `internal/policy` | 纯策略函数：屏障准入、quorum 算术、组终态推导 |
| `internal/executor` | 工作者循环：领取→心跳→超时/取消上下文执行→fencing 回报 |
| `internal/scheduler` | 引擎：批量提交、准入循环、租约恢复、工作者编排 |
| `internal/stats` | 稳定、可复现的终态分类统计报告 |
| `cmd/demo` | 端到端演示入口 |

## 3. 作业状态机

```
                         ┌──────────── 组屏障准入 ────────────┐
PENDING ──(组 admitted)──▶ READY ──(被某 worker 领取, 发 token)─▶ RUNNING
  │                          ▲  ▲                                   │
  │                          │  └──────── 失败但仍有重试额度(退避等待)┤
  │                          │                                      ├─▶ SUCCEEDED（终态）
  │                          │                                      ├─▶ FAILED     （终态：重试耗尽 / panic / 租约过期耗尽）
  │                          │                                      ├─▶ TIMED_OUT  （终态：超时，不重试）
  │                          └────────── 租约过期恢复(requeue) ─────┤
  │                                                                 └─▶ CANCELED   （终态：用户取消 / 组 fail-fast / 屏障前拒绝）
  └──────────────────────────────────────▶ CANCELED（屏障未开即取消/拒绝）
```

要点：

- **终态四分类** `SUCCEEDED / FAILED / TIMED_OUT / CANCELED`，互不混淆；
  非成功终态另有可区分的 **Reason**：`task_failed`、`task_panic`、`timeout`、
  `user_canceled`、`group_failfast`、`group_rejected`、`lease_expired`。
- **失败可重试**：普通任务错误（含 panic）在额度内回到 `READY`，由 `NextAttemptAt`
  实现退避等待；**超时终态不重试**；**取消终态不重试**。
- 每次领取生成一条 append-only 的 `Attempt` 记录（尝试号 + 全局单调 fencing token +
  worker + 起止时间 + 结果），使重试、重复领取、崩溃恢复全程可审计。

## 4. 组（批次）调度规则

### 4.1 准入策略 `AdmissionPolicy`（整组决定何时开始，杜绝"半个组"）

| 策略 | 何时开闸 |
|---|---|
| `immediate` | 有成员即可运行，成员随到随跑 |
| `all` | 到齐 `ExpectedMembers` 个成员**才**整体开闸；之前所有成员保持 PENDING |
| `quorum` | 达到多数 `n/2+1` 即开闸；**迟到成员**绕过已开闸的屏障直接 READY |

开闸在 store 的单一互斥锁内**原子完成**（组 → RUNNING，全体 PENDING 成员 → READY），
任何观察者都不会看到"部分成员已跑、部分还被屏障挡住"的中间态。

- `all` 组通过一次性 `SubmitGroup` 提交时必须带齐完整成员（否则当场报错并把组头
  置为 `CANCELED/group_rejected`，避免永远等不到的屏障）；也可以先建组再用
  `AddJobs` 分批凑齐。
- 屏障关闭时取消整组：组立即 `CANCELED`，所有成员 `CANCELED/user_canceled`，
  没有任何作业被启动过。

### 4.2 失败策略 `FailurePolicy`（成员失败时整组如何反应）

| 策略 | 行为 | 组终态 |
|---|---|---|
| `continue` | 其余成员继续运行，互不影响 | 至少一个成功 → `SUCCEEDED` |
| `failfast` | 任一成员 **FAILED 或 TIMED_OUT** 即触发：未开始/等待中的同伴立即
`CANCELED/group_failfast`，运行中的同伴被协作式取消 | 有失败→`FAILED` |

触发者保留自己的失败原因（如 `timeout`/`task_failed`），同伴统一为
`group_failfast`，因此统计里两类数量始终可区分，不会把"被连带取消"误记为失败。

### 4.3 组终态推导（`policy.GroupTerminal`，与完成顺序无关）

- 至少一个 `SUCCEEDED` → 组 `SUCCEEDED`；
- 无成功且存在 `FAILED`/`TIMED_OUT` → 组 `FAILED`；
- 全体 `CANCELED` → 组 `CANCELED`。

## 5. 并发领取、去重与崩溃恢复（不重复、不漏执行）

1. **原子领取 + 全局并行度上限**：`ClaimNext` 在互斥锁内做
   `READY+退避已过 → RUNNING` 的比较并设置，同时发放全局单调递增的 **fencing
   token**。多个 worker 并发领取不可能拿到同一作业；超过 `MaxParallel` 不发新租约。
2. **fencing 校验**：心跳与完成都必须携带当前租约 token。
   - 对同一 token 的重复 `FinishAttempt` → `ErrConflict`，结果不二次生效；
   - 租约过期后旧 worker（僵尸）的迟到回报 → token 不匹配 → `ErrConflict`，
     结果被丢弃，作业只属于新租约。
3. **租约过期恢复**：worker 每 `HeartbeatInterval` 续约；恢复循环发现
   `now - LastHeartbeat > LeaseTimeout` 的 RUNNING 尝试，将其关闭为
   `lease_expired` 并把作业重新置为 READY（受同一重试额度约束）。
4. **副作用恰好一次（任务台账）**：任务每次完成都以 `job#attempt` 为键写入
   `task-ledger.json`（原子 rename 落盘）。崩溃后重放同一尝试时直接返回台账结果，
   **不再执行任务体**；`crash` 类任务还会把"先提交副作用、后死亡"的场景跨尝试号
   重放（新尝试从同一作业最近的已提交结果恢复）。注册表暴露 `BodyRuns(job,n)`
   用于断言某尝试的真实执行次数为 0/1。
5. **持久化模型**：
   - `state.json`：每次结构性状态变更后**原子重写**（tmp + fsync + rename）的规范状态；
   - `events.log`：每次状态变更先 fsync 追加的 JSON-lines 审计轨迹；
   - 重开时以快照为准，再重放快照序号之后的事件（心跳/取消请求/租约过期），
     因此任意时刻崩溃都不会破坏一致性，且所有判定都有事件可追溯。

## 6. 重试与超时策略

`RetryPolicy`（组级配置，对全体成员生效）：

| 字段 | 含义 |
|---|---|
| `MaxAttempts` | 总尝试次数；`1` = 不重试；`<=1` 视为 1 |
| `Backoff` | `fixed`（恒为 BaseDelay）/ `linear`（BaseDelay×失败次数）/
`exponential`（BaseDelay×2^(n-1)，受 MaxDelay 封顶） |
| `BaseDelay` / `MaxDelay` | 退避基准与单次上限 |

- 第 n 次失败后等待 `Delay(n)` 才可被再次领取（事件 `job_retry_wait` 记录）。
- 每个作业有 `Timeout`（缺省取 `DefaultJobTimeout`）。执行者用
  `context.WithTimeout` 执行；超时 → `TIMED_OUT/timeout`（终态、不重试），
  心跳/取消轮询 goroutine 随之退出，worker 立即被回收去领取后续作业。
- 取消为**协作式**：运行中的作业被打上持久化的 `CancelRequested + CancelReason`，
  worker 轮询到后取消上下文并回报 `CANCELED`（原因 `user_canceled` 或
  `group_failfast`，即使与超时同时发生也以取消标记为准）。终态后重复取消返回
  `ErrConflict`（可判定的幂等冲突）。

## 7. 模拟任务（`domain.TaskSpec`）

| kind | payload | 语义 |
|---|---|---|
| `ok` | `["10ms"][":输出文本"]` | 延时后成功，返回输出 |
| `fail` | `["10ms|"]错误信息` | 延时后以该信息失败 |
| `sleep` | `100ms` | 睡眠后成功（用于超时/取消） |
| `flaky` | `N` | 前 N-1 次真实执行失败，第 N 次成功（计数来自台账，崩溃重放不消耗次数） |
| `panic` | 信息 | 任务体 panic；执行者 recover 为 `FAILED/task_panic`，worker 存活 |
| `crash` | 同 ok | 副作用先持久化，然后返回 `ErrSimulatedCrash` 模拟 worker 死亡、不回报 |

## 8. 配置项（`domain.Config`，`DefaultConfig()` 给出本地默认）

| 字段 | 默认 | 说明 |
|---|---|---|
| `DataDir` | `.scheduler-data` | 快照、事件日志、任务台账目录 |
| `NumWorkers` | 4 | 并发执行者 goroutine 数 |
| `MaxParallel` | 4 | 全局运行中作业上限（0 取 NumWorkers） |
| `HeartbeatInterval` | 20ms | worker 租约续约周期 |
| `LeaseTimeout` | 200ms | 心跳超过该时长视为 worker 死亡（应远大于心跳周期） |
| `DefaultJobTimeout` | 5s | 作业未显式指定超时时的缺省值 |
| `DefaultRetry` | MaxAttempts=1, fixed 20ms | 缺省重试策略 |
| `PollInterval` | 5ms | 调度/执行轮询周期 |

## 9. 可观测性与统计

- 每个决策都有事件：`group_submitted/admitted/rejected`、`job_submitted/ready/
  claimed/heartbeat/lease_expired/succeeded/failed/retry_wait/timed_out/
  cancel_requested/canceled`、`group_succeeded/failed/canceled`，事件序号严格递增。
- `stats.Build(store)` 按确定性 ID 顺序生成报告，含每组
  total/succeeded/failed/timed_out/canceled、按 reason 的细分和每作业终态、尝试次数、
  输出；同一输入重复构建/重复运行结果字节级一致。

## 10. 测试覆盖

- `TestConcurrentClaimDistinct`：8 worker 并发抢 40 作业，无重复领取、每作业恰一次成功尝试；
- `TestDuplicateClaimAndStaleFencing` / `TestStaleWorkerFencedAfterRecovery`：
  运行中重复领取为空、重复回报被拒、租约过期后新 token 领取、僵尸回报被 fencing；
- `TestRetryBackoffLinear` / `TestRetryExhausted`：线性退避实测、重试耗尽不再执行；
- `TestGroupContinuePartialFailure` / `TestGroupFailFastPartialFailure` /
  `TestFailFastTriggeredByTimeout`：组内部分失败两种策略 + 超时触发 fail-fast；
- `TestJobTimeout` / `TestCancelRunningJob` / `TestCancelWholeGroupRunning`：
  超时终态不重试、单作业与整组取消、终态重复取消冲突；
- `TestAdmissionImmediate/AllBarrier/Quorum/BarrierCancel`：三种屏障与屏障前取消；
- `TestRestartExactlyOnce`：全引擎关闭重开后恢复，重放尝试真实执行体次数为 0；
- `TestSnapshotDurabilityAcrossReopen` / `TestRecoverAfterReopen`：快照/日志持久化与
  崩溃时租约的重开恢复；任务台账幂等/跨重开/崩溃重放与退避算术单测。

所有关键用例均在日志中打印 `INPUT`（策略、数量、超时）与 `JUDGMENT`（终态、原因、
token/尝试依据）。
