# 注:该文档仅作为说明,不应该并入主分支


# Hook 执行计数器说明

## 概述

测试程序通过 pool 的 **Hook 系统** 注册了 4 个任务生命周期回调，用于追踪任务在每个阶段的调用次数。这些计数以 `hook_submitted`、`hook_enqueued`、`hook_started`、`hook_completed` 四列写入 CSV 输出。

默认开启，可通过 `--nohook` 参数关闭。

## 四个计数器与任务生命周期的对应关系

```
  Submit(task)           enqueue             worker picks up        Process() returns
      │                    │                      │                      │
      ▼                    ▼                      ▼                      ▼
  ┌───────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │submitted│ → │    enqueued      │ → │     started       │ → │    completed      │
  └───────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
```

| 计数器 | Hook 回调 | 触发时机 | 含义 |
|--------|----------|---------|------|
| `hook_submitted` | `OnTaskSubmitted` | 任务被 pool 接受（Submit 返回前） | 提交总数 |
| `hook_enqueued` | `OnTaskEnqueued` | 任务进入 handoff channel 队列 | 入队任务数（受队列容量限制） |
| `hook_started` | `OnTaskStarted` | worker 开始执行 `task.Process()` | 已开始执行的任务数 |
| `hook_completed` | `OnTaskCompleted` | 任务执行完毕（含 panic 恢复） | 已完成的任务数 |

### 计数器之间的关系

```
submitted ≥ enqueued ≥ started ≥ completed
```

- **submitted > enqueued**: NONBLOCK 模式下，队列满时任务被丢弃，enqueued 为队列中实际接收的任务数
- **enqueued > started**: 采样时部分任务还在队列中排队
- **started > completed**: 采样时部分任务正在执行中

当测试结束后（`-e` 等待期结束时）采样，通常满足：

- **BLOCK 模式**: `submitted = enqueued = started = completed`（所有任务完成）
- **NONBLOCK 模式**: `submitted > enqueued = started = completed`（超出队列容量的任务被丢弃）

## 实现细节

每个回调内部仅做一次 **`atomic.AddInt64`** 操作。计数器之间没有互锁，因此单次采样读到的 4 个值并非严格同一时刻的快照——应将其理解为 "至少 N 次" 的近似值。

```
hook 回调执行路径:
  pool.Submit(task)
    → p.hooks.mu.RLock()         // 池级读写锁（读锁）
    → 遍历 hooks.taskSubmitted
    → invokeTaskHookSafely(fn, task, "OnTaskSubmitted")
        → atomic.AddInt64(&submittedCount, 1)  // 唯一的业务逻辑
    → p.hooks.mu.RUnlock()
```

## 性能考量

### 默认模式（Hook 开启）

- **内存开销**: `hookEnabled` (1 bool) + 4 × `int64` = **33 bytes**，可忽略
- **Hook 结构**: pool 内部 `hooks` 持有 5 个 slice（各 1 个元素）+ 1 个 `sync.RWMutex`
- **每次 Submit/Enqueue/Start/Complete 路径**额外开销：
  - `RLock` + `RUnlock`（读锁，多个 goroutine 可并发持有）
  - 1 次 `atomic.AddInt64`（无锁，但可能触发 cache-line 写争用）

### 潜在竞争

当 worker 数量极大（如 20,000+）且任务粒度极轻（< 1ms）时：

1. **RWMutex 读锁**: 所有进入 `dispatchTaskSubmitted` 等的 goroutine 并发持有读锁，读锁之间不互斥，**无竞争**。
2. **Cache-line bouncing**: 4 个 `int64` 计数器可能位于同一 cache line（64 bytes），大量 goroutine 对同一 cache line 执行 `atomic.AddInt64` ≤4 个计数器 > 会导致该 cache line 在不同 CPU 核心间反复无效化（false sharing），**轻微增加内存总线压力**。
3. **回调闭包调用开销**: 每次 dispatch 遍历 hook slice（长度 1），函数调用 + defer/recover 开销约 ~20ns。

### 实测影响

在 12 个对比测试场景（含 10000 workers / 100000 tasks 的极限场景）中，Hook 开启与关闭的性能差异 **< 3.3%**，处于运行间噪声范围内。

**对于任务粒度 ≥ 10ms 的场景，Hook 计数的性能影响完全可以忽略。**

## `--nohook` 参数

### 用法

```bash
# 默认：Hook 开启，CSV 包含 4 列 hook 计数
.\agilepool_test.exe -T fixed --task-base 100 -t 50000 -w 20000 -i 1 -f csv

# 关闭 Hook：CSV 不含 4 列 hook 计数（header 和数据均省略）
.\agilepool_test.exe -T fixed --task-base 100 -t 50000 -w 20000 -i 1 -f csv --nohook
```

### 何时使用 `--nohook`

| 场景 | 建议 |
|------|------|
| 日常测试 / 观察任务执行情况 | 不开启（默认），利用计数器验证任务完整性 |
| 纯性能基准测试（benchmark） | 开启 `--nohook`，排除一切非必要开销 |
| Worker 数 > 50000 且任务 < 1ms | 建议开启 `--nohook`，避免 cache-line 写争用 |
| 调试 NONBLOCK 丢弃率 | 不开启，通过 `submitted ≠ completed` 观察丢弃情况 |

### 关闭后的行为

- `setupHookTracking` 是空操作，pool 的 `hooks` 不会被修改
- pool 内部 dispatch 函数（`dispatchTaskSubmitted` 等）仍会执行，但因 hook slice 为空（`len=0`），遍历立即结束
- `readHookCounters` 返回 `(0, 0, 0, 0)`
- CSV 输出**不包含** 4 列 hook 计数（header 和数据行均省略），JSON 同理

## 未来规划

后续将考虑为 Task 引入 **`context.Context` 支持**，使任务可携带标准 Go context（含 traceID、spanID、租户标识等元数据），Hook 回调能够提取这些上下文信息进行分析。具体方向包括：

- **Task 内嵌 Context**: 允许提交任务时附带 `context.Context`，Hook 回调可通过 Task 接口获取
- **跨阶段追踪**: 同一任务的 submitted → enqueued → started → completed 全链路共享同一个 context，实现端到端可观测
- **上下文透传分析**: Hook 回调可提取 context 中的 traceID、deadline、自定义 key-value 等，写入 CSV 或对接外部 tracing 系统
- **低侵入设计**: 可能通过可选接口（如 `TaskWithContext`）扩展，不强制所有 Task 实现，保持现有 Task 接口的简洁性
- **内置分析上下文**: 准备一些预置基于 context 的 latency 分段统计、错误分类、重试计数等分析能力，开箱即用

这些能力将使 Hook 从纯计数观察升级为**可编程的任务级可观测性管道**，适用于分布式追踪、性能诊断、业务埋点、自适应调参等进阶场景。
