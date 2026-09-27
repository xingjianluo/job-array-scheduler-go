# job-array-scheduler-go

单机可运行的作业数组（job array）调度与执行系统。它用**本地进程 + 文件持久化 +
固定容量槽**模拟真实集群里的工作负载管理，提供：

- 作业提交与**成组（group/array）批量提交**，组屏障（barrier）决定整组何时开跑；
- 多个执行者（worker）**并发领取**，基于租约（lease）保证重复领取 / 重试 /
  进程重启下作业**不会被重复执行，也不会丢失**；
- **失败重试**（次数 + 退避可控）、**超时**、**取消**，超时/取消后进程与容量都被回收；
- 终态统计严格区分 **成功 / 失败 / 超时 / 取消**，相同输入重复运行结论一致；
- 所有状态变更写入只追加的、带校验和的审计日志，**可恢复、可追溯**。

## 快速开始

```bash
# 运行内置演示：CONTINUE 组 + FAIL_FAST 组 + 屏障关闭后取消，打印终态报告与审计轨迹
go run ./cmd/jas --state-dir ./.jas-state

# 用同一目录重启（模拟故障恢复）：只打印报告，不会重复执行任何已终态作业
go run ./cmd/jas --state-dir ./.jas-state --report-only
```

演示输出包含每组的策略、终态、每个作业的 `状态 / 失败原因 / 尝试次数 / 退出码`，
以及每个作业的关键事件链，例如：

```
b-slow1  CANCELED  reason=GROUP_ABORTED  attempts=1 exit=-1
b-slow2  CANCELED  reason=GROUP_ABORTED  attempts=0 exit=-1
...
b-slow1  SUBMITTED CLAIMED GROUP_ABORTED
```

## 运行测试

```bash
go test ./...                 # 全部单元 + 集成测试
go test ./... -race --count=3 # 竞态检测 + 重复运行验证稳定性
```

测试覆盖：并发领取不重复、重复领取 / 重启不重跑、租约过期恢复、组内部分失败
（FAIL_FAST 原子中止其余成员）、重试成功 / 重试耗尽、超时回收容量、取消运行中与
就绪作业、屏障保持与打开等。每个关键用例都在日志中打印**输入**与**判定依据**。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/job` | 领域模型：作业 / 组 / 状态 / 失败原因 / 事件 |
| `internal/store` | 持久化：跨进程文件锁 + 校验和 WAL（崩溃重放修复）+ 快照压缩 |
| `internal/executor` | 本地进程执行器：进程组隔离，超时/取消整组 SIGKILL 并回收 |
| `internal/scheduler` | 提交/屏障、领取租约、worker 池、容量槽、重试、恢复、取消 |
| `internal/stats` | 终态分桶统计（四桶互斥且穷尽） |
| `cmd/jas` | 自包含演示程序 |

详细设计见 [docs/design.md](docs/design.md)。

## 关键配置项

`scheduler.Config`：

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `Slots` | 4 | 并发执行容量（模拟集群槽位），worker 领取前先占槽 |
| `LeaseTTL` | 10s | 领取后租约有效期，超过未续约即判定执行者死亡 |
| `LeaseRenew` | TTL/3 | 心跳续约间隔，覆盖进程运行与重试退避全程 |
| `PollInterval` | 50ms | 无可领取作业时的轮询间隔 / Wait 轮询粒度 |
| `RecoverInterval` | 1s | 扫描并回收过期租约的周期 |
| `WorkDir` | 继承 | 子进程工作目录 |
| `OwnerPrefix` | `proc` | 租约 owner 标识前缀（进程内再拼唯一序号） |

每个作业的 `job.Spec`：

| 字段 | 含义 |
| --- | --- |
| `Command` / `Args` / `Env` | 本地可执行程序、参数、追加环境变量 |
| `Timeout` | 单次尝试的硬超时（>0 生效），到期整组进程被 kill |
| `MaxRetries` | 首次失败后允许的重试次数（0 = 只跑一次） |
| `RetryBackoff` | 每次重试前的固定退避等待 |

`store.Config`：`Dir`（LOCK + journal.log 所在目录）、`LeaseTTL`（透传统一配置）、
`Now`（注入时钟，便于测试）。

## 状态机（作业）

```
PENDING ──组屏障打开──▶ READY ──领取(占槽+租约)──▶ RUNNING
   │                     ▲  ▲                         │
   │                     │  └────租约过期(REQUEUED)────┘ (执行者崩溃/重启)
   │                     │
   │                     └──────────重试退避后再次尝试（作业保持 RUNNING+心跳）
   │
   └──取消(CANCEL/屏障关闭)──▶ CANCELED (USER_CANCELED)

RUNNING ──进程退出0────────────▶ SUCCEEDED
RUNNING ──重试预算耗尽─────────▶ FAILED (RETRIES_EXHAUSTED)
RUNNING ──Timeout 到期─────────▶ TIMED_OUT (TIMEOUT)
RUNNING ──用户取消─────────────▶ CANCELED (USER_CANCELED)
READY/PENDING/RUNNING ─FAIL_FAST 组成员失败─▶ CANCELED (GROUP_ABORTED)
```

四个终态 `SUCCEEDED / FAILED / TIMED_OUT / CANCELED` 互斥；失败原因
（`RETRIES_EXHAUSTED / TIMEOUT / USER_CANCELED / GROUP_ABORTED / EXIT_NON_ZERO /
SPAWN_FAILURE`）让"为什么结束"可区分、可解释。
