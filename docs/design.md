# 设计说明：单机作业数组调度与执行

本文件说明调度规则、状态机、领取 / 租约 / 恢复协议、重试与超时策略、持久化与
崩溃恢复，以及"一致性结论为何可重复"。

## 1. 目标与范围

在单机上用本地 OS 进程模拟集群工作负载：

- **容量**用固定数量的容量槽（`slots`）表示，类似集群的可调度 CPU/节点配额。
- **执行者**是同进程内的多个 worker goroutine；也支持用同一存储目录启动多个
  OS 进程（靠文件锁串行化写入、靠租约互斥领取）。
- **任务**是任意本地可执行命令，成功 / 失败看退出码，超时 / 取消靠 context +
  进程组 kill。

系统保证：

1. 同一作业任意时刻**至多一个执行者在跑**（互斥领取 + 租约 owner 守卫）。
2. 作业**至少被执行到终态一次**（领取后执行者崩溃 → 租约过期 → 重新就绪）。
3. 所有状态变更**原子提交且可审计**（WAL + 事件日志），重启后从最后一条完好
   记录恢复，撕裂尾部自动截断。
4. 终态统计四桶互斥且穷尽，纯函数计算，重复运行结论稳定。

> 关于"恰好一次"：状态机层面的终态转换是恰好一次的（不会出现两个终态、不会重跑
> 已终态作业）。但如果任务在外部世界有副作用，且执行者在"副作用已产生、终态尚未
> 提交"的窗口崩溃，任务会被重新执行一次。因此任务应尽量**幂等**。这是
> at-least-once 执行 + 去重状态机的标准语义。

## 2. 核心抽象

- `job.Group`：一批作业的集合，带策略 `FAIL_FAST` / `CONTINUE`、屏障开关 `Open`
  与聚合状态。
- `job.Job`：一个数组元素，带 `Spec`、状态、尝试次数 `Attempts`、租约字段
  （`LeaseOwner` / `LeaseUntil`）、终态失败原因与退出码。
- `job.Event`：不可变状态迁移记录（SUBMITTED / READY / CLAIMED /
  LEASE_RENEWED / ATTEMPT_START / RETRY_READY / REQUEUED / SUCCEEDED /
  FAILED / TIMED_OUT / CANCELED / GROUP_ABORTED / GROUP_FINALIZED /
  BARRIER_OPENED / GROUP_CANCELED）。
- `store.Store`：持久化边界。`Read` 取锁内快照；`Mutate` 是"克隆 → 修改 →
  fsync WAL → 内存换入"的原子事务，函数返回错误即整体回滚。
- `executor.Runner`：跑一次尝试，返回互斥的结构化结果（成功 / 非零退出 /
  启动失败 / 超时 / 取消）。
- `scheduler.Scheduler`：协调者 + worker 池 + 恢复器。

## 3. 组（数组）调度规则

### 3.1 批量提交是一个事务

`SubmitGroup` 在**单个 store 事务**内插入组记录和全部成员，并为每个成员发
SUBMITTED 事件。要么整组都在，要么都不在，不存在"提交了半个组"。

### 3.2 组屏障（何时整组开始）

- `startNow=true`：提交事务同时把组置为 RUNNING、成员置 READY，并记
  BARRIER_OPENED。
- `startNow=false`：组成员停在 PENDING，worker 只领取 `group.Open==true` 的
  READY 作业，因此屏障关闭时作业绝不会被执行；`OpenGroup` 在单事务内翻转开关
  并把所有 PENDING 成员原子置 READY。

这保证"同一组按整体策略决定何时开始"。

### 3.3 FAIL_FAST vs CONTINUE

- **CONTINUE**：某成员失败 / 超时不影响其它成员，各自跑到终态。
- **FAIL_FAST**：某成员**首次提交 FAILED 或 TIMED_OUT 的同一事务内**，把组内所有
  非终态成员置 `CANCELED / GROUP_ABORTED`，并对其中原本 RUNNING 的成员触发进程
  打断。失败决定与兄弟中止**同生共死**，因此绝不会出现"半个组卡在不确定状态"。

注意一个可解释的边界：如果一个"成功"成员在失败成员的中止事务提交**之前**已经
提交 SUCCEEDED，它保持 SUCCEEDED（已完成的工作不回滚）；只有当时仍未终态的成员
被 GROUP_ABORTED。演示中 b-ok 即成功、b-slow1/2 被中止。

### 3.4 组终态聚合（确定性）

当所有成员终态后，在同一事务内聚合组状态（`aggregateGroupStatus`）：

1. 组已被显式 `GROUP_CANCELED` → 保持 CANCELED；
2. 任一成员 FAILED / TIMED_OUT，或存在被 GROUP_ABORTED 的兄弟 → **FAILED**；
3. 仅有"成功 + 用户取消"混合 → CANCELED；
4. 全部成功 → SUCCEEDED。

## 4. 作业状态机与失败原因

### 4.1 状态

`PENDING → READY → RUNNING → {SUCCEEDED, FAILED, TIMED_OUT, CANCELED}`

- PENDING：已持久化但组屏障未开，不可领取。
- READY：屏障已开，可被领取。
- RUNNING：某 worker 持有有效租约。
- 四个终态不可逆。

### 4.2 失败原因（互斥、可区分）

| Reason | 含义 | 对应终态 |
| --- | --- | --- |
| `EXIT_NON_ZERO` | 进程启动成功但退出码非 0（单次尝试） | 通常进入重试，最终 FAILED |
| `SPAWN_FAILURE` | 进程根本没启动（找不到二进制等） | FAILED（计入重试预算） |
| `TIMEOUT` | 单尝试 `Spec.Timeout` 到期 | TIMED_OUT |
| `USER_CANCELED` | 用户 / CancelGroup 取消 | CANCELED |
| `GROUP_ABORTED` | FAIL_FAST 下因别的成员失败被中止 | CANCELED |
| `RETRIES_EXHAUSTED` | 重试预算用完仍失败（终态原因，附带最后退出码） | FAILED |

终态 FAILED 的作业同时保留 `ExitCode`（最后一次退出码）与 `Attempts`，事件
详情里还有 `last_error` 与每次尝试的退出信息，便于解释。

## 5. 领取、租约与"不重不丢"

### 5.1 领取是原子条件翻转

`claimNext` 在一个 `Mutate` 里：挑选最早提交（时间戳前缀 ID 字典序）的、属于
已打开且非终态组的 READY 作业，把它置 RUNNING、`Attempts=1`、写新 owner 与
`LeaseUntil=now+TTL`，发 CLAIMED 事件。挑选与翻转同事务，因此：

- N 个并发 worker 同时领取，**只有一个**能把某个 READY 翻成 RUNNING；其余看到的
  已是 RUNNING，去挑下一个。重复领取天然被条件翻转挡住。

### 5.2 心跳续约

作业在**整个生命周期**（含多次尝试与退避等待）都保持 RUNNING；一个心跳 goroutine
每 `LeaseRenew` 续约一次，续约也是带 owner 守卫的事务（只有
`RUNNING && LeaseOwner==我` 才能延长 `LeaseUntil`）。

### 5.3 终态写入带 owner + 状态守卫

尝试结束后，终态提交只在"作业仍 RUNNING 且租约 owner 仍是我"时生效：

- 若期间发生了用户取消（作业已被置 CANCELED）或租约被恢复者回收并被他人领取，
  迟到的终态写入被**拒绝**，旧执行者无法覆盖新结果（防止僵尸执行者双写）。

### 5.4 崩溃 / 重启恢复（at-least-once）

恢复器每 `RecoverInterval`（也可手动 `RecoverStaleLeases`）扫描：
`status==RUNNING && now>=LeaseUntil` 的作业，在单事务内置回 READY、清租约、
保留 `Attempts`（**重试预算不因崩溃重置**），发 REQUEUED 事件。随后任意 worker
可重新领取。进程重开同一 `Dir` 时：

- `flock(LOCK_EX)` 保证同一时刻只有一个进程能写（旧进程死掉后 OS 自动释放锁，
  新进程拿到锁并接管）；
- WAL 重放到最后一条完好记录，作业不会因重启而停在 RUNNING 黑洞里。

### 5.5 关停语义

取消 Run 的 context 时：worker 停止领取新作业；**正在跑的作业不被强制写终态**，
其租约随后过期并由下一个存活进程重新就绪。因此"杀进程再启动"等同于一次故障恢复，
不丢作业、也不把作业误记为失败 / 取消。

## 6. 重试与超时策略

### 6.1 重试

- `MaxRetries` 是首次之后允许的重试次数；总尝试上限 = `MaxRetries+1`。
- `Attempts` 在**每次进程启动之前**就持久化递增，崩溃恢复后从存储读到的计数继续
  累加，预算判断 `attemptNo <= MaxRetries`。
- 退避 `RetryBackoff` 期间作业保持 RUNNING 并继续心跳；退避可被用户取消打断
  （立即转 CANCELED）。
- 首次尝试成功 → SUCCEEDED；非 0 退出 / 启动失败且仍有预算 → 退避后再试；预算
  用尽 → FAILED / RETRIES_EXHAUSTED（FAIL_FAST 下同时原子中止兄弟）。

> 实现注意：执行者每轮会从存储刷新作业状态以感知取消，但尝试计数使用 worker
> 本地单调递增的 `attemptNo`（初值取自持久化的 Attempts），避免"刷新状态把本地
> 计数重置为 1"导致无限 / 错误重试。

### 6.2 超时与资源回收

- 每次尝试用 `context.WithTimeout(parent, Spec.Timeout)` 包裹；超时后
  `exec.CommandContext` 的 `Cancel` 对**整个进程组**发 SIGKILL（子进程
  `Setpgid`），并设置 `WaitDelay` 等待回收，孙进程也不会泄漏。
- 取消同理（SIGKILL 进程组）。执行器测试用唯一名标记进程验证 kill 后无残留。
- worker 用 `defer slot.Release()` 保证无论成功 / 失败 / 超时 / 取消 / 丢租约，
  占用的容量槽都归还。单槽 + 超时作业后跟一个正常作业的测试，验证槽被正确释放，
  否则后一个作业永远跑不上来。

## 7. 持久化与可审计性

`FileStore` 目录内容：

- `LOCK`：`flock` 跨进程互斥。
- `journal.log`：只追加 WAL，每行一条
  `<8位十六进制 CRC32><JSON 全量状态快照 + 本次事件>`。

每次 `Mutate` 成功：

1. 在内存**克隆**上跑事务闭包（出错直接丢弃克隆 = 回滚）；
2. 全量状态序列号递增，事件追加并编号；
3. 先 `write` 再 `fsync` 落盘，**然后**才换入内存（崩溃时最多丢最后一条未刷盘
   的，且该条对外尚未生效）；
4. 记录数超过阈值时用"临时文件 + fsync + rename + 目录 fsync"做单快照压缩，
   压缩失败不影响正确性。

打开时从头扫描，遇到第一条 CRC / 结构损坏的行即停止并把文件截断到最后一条完好
记录的边界（修复崩溃中途的撕裂写）。审计事件与状态快照同在 WAL 中，因此
`Store.Events(jobID)` 可随时给出某作业完整、有序、可解释的状态迁移历史。

选择"每事务全量快照"而非经典增量回放，是为了让恢复极简且确定：最后一条完好记录
即完整状态；规模增大后由压缩控制文件体积。

## 8. 统计为何稳定

`stats.Build(jobs, groups)` 是纯函数：每个**终态**作业恰好落入一个桶
（Succeeded/Failed/TimedOut/Canceled），非终态计入 InFlight 而不计桶。桶的归属
只取决于终态 `(status, reason)`，报告内作业按 `(CreatedAt, ID)` 稳定排序，组按
ID 排序。因此相同最终状态在任意次运行下产生逐字节一致的计数。组报告还逐作业给出
原因、尝试次数与退出码，使总数可下钻解释。

## 9. 关键配置一览

| 配置 | 位置 | 默认 | 说明 |
| --- | --- | --- | --- |
| Slots | scheduler | 4 | 并发容量；领取前先占槽，结束必释放 |
| LeaseTTL | scheduler | 10s | 租约有效期 |
| LeaseRenew | scheduler | TTL/3 | 续约心跳间隔（须 < TTL） |
| PollInterval | scheduler | 50ms | 空闲轮询 / Wait 粒度 |
| RecoverInterval | scheduler | 1s | 过期租约回收周期 |
| WorkDir | scheduler | "" | 子进程 cwd |
| OwnerPrefix | scheduler | proc | owner token 前缀 |
| Timeout | job.Spec | 0(不限) | 单尝试硬超时 |
| MaxRetries | job.Spec | 0 | 首次后重试次数 |
| RetryBackoff | job.Spec | 0 | 重试前固定退避 |
| Dir | store | 必填 | LOCK + journal.log 目录 |
| Now | store | time.Now | 注入时钟（测试） |

## 10. 测试场景对照

| 场景 | 测试 | 断言要点 |
| --- | --- | --- |
| 批量成功 | TestSuccessfulBatch | 3/3 SUCCEEDED，组 SUCCEEDED，无 in-flight |
| 并发领取 | TestConcurrentClaimsNoDuplicateExecution | 40 作业每作业恰好 1 次 CLAIMED |
| 重复领取/重启 | TestDuplicateClaimCannotDoubleRun | 小 TTL 竞争恢复后仍 30/30；重开存储不重跑 |
| 租约过期恢复 | TestStaleLeaseRecovered | 崩溃后出现 REQUEUED，Attempts 跨重启保留 |
| 重试成功 | TestRetryThenSuccess | 第 3 次成功，Attempts==3 |
| 重试耗尽 | TestRetriesExhaustedFails | FAILED/RETRIES_EXHAUSTED，尝试 2 次，保留退出码 3 |
| 组内部分失败 | TestFailFastPartialFailureAbortsSiblings | FAIL_FAST 下慢成员 GROUP_ABORTED，组 FAILED |
| 超时+容量回收 | TestTimeoutReclaimsResourcesAndDistinguished | TIMED_OUT 与后续 SUCCEEDED 分桶，槽被释放 |
| 取消 | TestCancelRunningAndReadyJobs | 运行中 + 就绪作业全部 USER_CANCELED，无半状态 |
| 屏障 | TestBarrierHoldsThenOpens | 关闭时保持 PENDING，打开后才执行 |
| 分桶稳定性 | stats.TestBuildBucketsAreExclusiveAndStable | 四桶互斥穷尽，重复 Build 一致 |
