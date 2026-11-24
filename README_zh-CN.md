# agilePool

<p align="center">
  <img src="assets/logo.jpg" alt="agilePool logo" width="260">
</p>

<p align="center">
  <a href="https://github.com/Yiming1997/agilePool/actions/workflows/ci.yml"><img src="https://github.com/Yiming1997/agilePool/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/Yiming1997/agilePool"><img src="https://codecov.io/gh/Yiming1997/agilePool/branch/main/graph/badge.svg" alt="Codecov"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.23.4-00ADD8" alt="Go Version"></a>
  <a href="https://github.com/Yiming1997/agilePool/tags"><img src="https://img.shields.io/github/v/tag/Yiming1997/agilePool?label=tag" alt="Tag"></a>
  <a href="https://pkg.go.dev/github.com/Yiming1997/agilePool/v2"><img src="https://pkg.go.dev/badge/github.com/Yiming1997/agilePool/v2.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/Yiming1997/agilePool/v2"><img src="https://goreportcard.com/badge/github.com/Yiming1997/agilePool/v2" alt="Go Report Card"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/Yiming1997/agilePool" alt="License"></a>
  <a href="https://agilepool.stardreamer.cloud"><img src="https://img.shields.io/badge/%E5%AE%98%E7%BD%91-agilepool.stardreamer.cloud-2F6FED?logo=googlechrome&logoColor=white" alt="官网"></a>
  <a href="https://hub.xerj.org/contributors/Yiming1997"><img src="https://hub.xerj.org/contributors/Yiming1997/badge.svg" alt="XERJ Corpus Hub contributor"></a>
  <a href="https://awesome-go.com/#goroutines"><img src="https://awesome.re/mentioned-badge.svg" alt="收录于 Awesome Go"></a>
</p>

[English](README.md)

`agilePool` 是一个高性能 Go goroutine 池。核心亮点是**基于滑动窗口速率统计的自适应扩缩容**（adaptive scaler），配合有界并发、空闲 worker 复用、无界任务缓冲（带背压）、可重试任务和优雅关闭——适合需要提交海量小型异步任务、同时又不希望无限制创建 goroutine 的应用。

## 特性

- **自适应扩缩容** — 基于精确中位数的速率追踪 + 感知任务耗时的积压目标，自动调节 worker 数量匹配提交负载，支持可配置的积压衰减因子应对突发流量。
- **有界 worker** — 通过 `WithWorkerNumCapacity` 限制最大并发数。
- **无界任务缓冲** — 小容量内部交接 channel + 分块链表缓冲区，带背压上限（`maxChunkLen`）。
- **阻塞 / 非阻塞提交模式** — `BLOCK` 模式下任务入队等待；`NONBLOCK` 模式下队列满时丢弃。
- **空闲 worker 自动清理** — 后台清理器定期移除超时空闲 worker。
- **可插拔空闲容器** — LinkedList（FIFO 链表）、MinHeap（LRU 最小堆）、Treap（LRU 树堆）、Slice（FIFO 动态数组）、RingQueue（环形缓冲区，O(1) Pop）。
- **可重试任务** — 默认指数退避，支持自定义退避策略。
- **Context 感知提交** — `SubmitCtx` 支持入队前和入队后的协作取消。
- **带超时的提交** — `SubmitBefore` 设定执行截止时间。
- **优雅关闭** — `Wait` 等待所有运行中任务完成；`Close` 停止接收新任务。
- **自定义 Logger** — 可接入任何实现了 `Printf`/`Println` 的日志库（如 `zap.SugaredLogger`）。
- **生命周期钩子** — `hook.NewHooks()` 分发 Submitted / Enqueued / Started / Completed / PoolClosed 回调，并支持可选的 Rejected 扩展；回调 panic 会被隔离并记录日志。

## 安装

```bash
go get github.com/Yiming1997/agilePool/v2
```

## 快速开始

```go
package main

import (
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

func main() {
	pool := agilepool.NewPool(agilepool.NewConfig())
	defer pool.Close()

	for i := 0; i < 1000; i++ {
		pool.Submit(agilepool.TaskFunc(func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}))
	}

	pool.Wait()
}
```

## 配置

通过 `NewConfig` 创建配置，使用函数式 Option 调整行为：

```go
pool := agilepool.NewPool(agilepool.NewConfig(
	agilepool.WithWorkerNumCapacity(20000),
	agilepool.WithCleanPeriod(500*time.Millisecond),
	agilepool.WithBlockMode(agilepool.BLOCK),
	agilepool.WithIdleContainerType(agilepool.LinkedListType),
	agilepool.WithScalerPeriod(10*time.Millisecond),
	agilepool.WithBacklogDecayFactor(0.3),
))
defer pool.Close()
```

### 可用配置项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `WithCleanPeriod` | `500ms` | 后台清理器检查过期空闲 worker 的频率。 |
| `WithWorkerNumCapacity` | `math.MaxInt64` | 最大运行 worker 数量。 |
| `WithBlockMode` | `BLOCK` | `BLOCK` 队列满时阻塞等待；`NONBLOCK` 队列满时丢弃。 |
| `WithIdleContainerType` | `LinkedListType` | 空闲 worker 存储结构（详见[空闲 Worker 容器](#空闲-worker-容器)）。 |
| `WithStatsSamplePeriod` | `100ms` | 速率统计采样间隔。 |
| `WithStatsWindowSize` | `10` | 用于中位数计算的滑动窗口数量。 |
| `WithScalerPeriod` | `10ms` | 扩缩容器的检查周期。 |
| `WithBacklogDecayFactor` | `0.3` | 积压在扩缩公式中的权重（0–1）。值越大，扩容越激进。 |
| `WithZeroConsumeScaleInterval` | `100ms` | 零消费兜底扩容的最小间隔：池子有排队但实测消费速率为 0 时（如空闲后突发长任务），按排队量扩容的频率上限。 |
| `WithMaxZeroConsumeScaleStep` | `64` | 单次零消费扩容最多新增的 worker 数。 |
| `WithDrainHorizon` | `15ms` | 清空当前积压的目标时间。积压驱动的爬坡目标为 `ceil(queuedTasks × serviceTime / drainHorizon)`，即能在该时间内干完排队任务所需的 worker 数。值越小爬坡越猛、峰值越高。 |

> **注意**：`WithTaskQueueSize` 控制内部交接 channel 的容量。超过该容量的任务会进入动态增长的分块缓冲区。

## 提交任务

### 基础提交

```go
pool.Submit(agilepool.NewTask(func() error {
	// 在这里执行任务逻辑。
	return nil
}))
```

`NewTask` 可将普通的 `func() error` 便捷地包装为任务。`Task` 的执行方法为包内私有方法，业务代码不需要实现任务接口；需要使用重试时，可继续使用 `TaskWithRetry`。

`TaskFunc` 返回 `error` 是为了兼容可重试任务模式；普通 `Submit` 不会检查返回值。

### 带 Context 提交

`SubmitCtx` 支持协作取消：

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

pool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// 执行任务逻辑。
		return nil
	}
}))
```

取消语义：
- 提交前 `ctx` 已取消 → 任务**直接丢弃**。
- `BLOCK` 模式下等待队列空间 → `ctx` 取消时**中断等待**并丢弃。
- 已入队但尚未开始 → worker 取到任务后检查 `ctx`，已取消则**跳过执行**。
- 已开始执行 → 池**不会**强制停止 goroutine；任务需主动检查 `ctx.Done()` 或将 `ctx` 传递给下游调用。

### 带超时提交

`SubmitBefore` 给任务设置执行截止时间。若 worker 取到任务时已超时，则跳过。`BLOCK` 模式下若已达到背压上限，等待提交的调用也会在截止时间到达时返回，不再入队该任务。

```go
pool.SubmitBefore(
	agilepool.TaskFunc(func() error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}),
	10*time.Second,
)
```

## 可重试任务

`TaskWithRetry` 在任务返回错误时自动重试。默认使用 `MinBackOff` 到 `MaxBackOff` 之间的指数退避；可通过 `BackOffStrategy` 自定义策略。

```go
pool.Submit(&agilepool.TaskWithRetry{
	MinBackOff: 1 * time.Second,
	MaxBackOff: 30 * time.Second,
	RetryNum:   3,
	Task: func() error {
		return errors.New("临时故障")
	},
})
```

## 空闲 Worker 容器

池会复用空闲 worker。根据场景选择合适的存储结构：

| 容器 | 排序方式 | Pop | 过期清理 | 适用场景 |
| --- | --- | --- | --- | --- |
| `LinkedListType` | 插入顺序（FIFO） | O(1) | O(n) 全量扫描 | 通用场景，简单 FIFO 复用。 |
| `MinHeapType` | `lastActiveAt`（LRU） | O(log n) | O(k log n) 提前停止 | 大量 worker，高效过期扫描。 |
| `TreapType` | `lastActiveAt`（LRU） | 期望 O(log n) | 期望 O(log n + k) | 适合批量清理大量过期 worker。 |
| `SliceType` | 插入顺序（FIFO） | O(1) | O(log n + k) 二分查找 | 中等空闲数，缓存友好。 |
| `RingQueueType` | 插入顺序（FIFO） | O(1) | O(n) 环形扫描 | 固定空闲容量，O(1) Pop。 |

```go
pool := agilepool.NewPool(agilepool.NewConfig(
	agilepool.WithIdleContainerType(agilepool.RingQueueType),
	agilepool.WithWorkerNumCapacity(20000),
))
```

## 自适应扩缩容

Scaler 是整个池的核心调度机制，每 **`scalerPeriod`**（默认 10ms）运行一次，通过三步流水线决策是否需要扩容：

**1. 速率采样** — `statsSampler` 每隔 `statsSamplePeriod`（100ms）swap 并清零 `submitCount`、`exitCount` 两个计数器，把采样值连同 `consumeCount` 的增量和当前 running worker 数一起写入滑动窗口。

**2. 中位数提取** — 每个窗口**精确保留**最近 `statsWindowSize`（10）个采样。`getMedianRates()` 返回 submit / consume / exit 速率的**中位数**——相比平均值，中位数对短暂毛刺的抵抗力强得多；`getRunningAverage()` 返回同一窗口内 running worker 数的均值。

> 速率刻意用精确值统计。scaler 的目标值是两个速率的**比值**，而分桶直方图会把这个比值毁掉：submit 速率 10000 和 consume 速率 3571 落进同一个桶，都被报成 6000，scaler 于是认为自己在跟上，压根不扩容。精确采样保住了这个比值。

**3. 扩缩决策** — `scaleIfNeeded()` 用两个互相独立的信号算目标 worker 数，取较大的那个：

```
rateTarget  = submitMed × runningAvg / consumeMed          (Little's Law 反推，稳态)
drainTarget = ceil(queuedTasks × serviceTime / drainHorizon)  (突发爬坡)
target      = max(rateTarget, drainTarget)
```

这里用的是**窗口均值** `runningAvg`，不是实时的 running 值。这个区别很关键：反推出的服务时间（`runningAvg / consumeMed`）是观察窗口的属性，所以扩容不会抬高池子自己的目标值。用实时值时，target 与当前 worker 数成正比，一次突发会在每个 scaler tick 上把池子乘一遍，直到被积压刹车拦住。

| | 实时 `running` | 窗口 `runningAvg` |
| --- | --- | --- |
| 扩容后的 target | 下一跳又升高 | 窗口内不变 |
| 稳态峰值 goroutine（2 万任务/秒，任务 1ms） | 439 | 47 |

`drainTarget` 补的是窗口留下的缺口。速率目标每个统计窗口才能动一次，冷池接到突发后先用一个 worker 扛满整个窗口，之后每个窗口再涨一步有上限的一点点——IO 场景里突发往往在目标反应过来之前就结束了，吞吐就丢了。

`drainTarget` 是「能在 `drainHorizon`（默认 15ms）内干完排队任务所需的 worker 数」。其中的服务时间用上一个 scaler tick 的完成数实测，一个 tick 内就有了，不用等整窗口；它描述的是任务本身而不是队列深度——同样深的队列，慢任务让池子开大，快任务让池子保持精简。这个目标里没有 worker 数，所以不会反过来影响池子的扩缩决策；它又随队列一起回落，所以不会失控。

当存在任务积压（channel + 分块缓冲区中的任务），scaler 通过**动态衰减因子**变得更激进：

```
bufPressure  = min(1.0, bufDepth / consumeMed × 0.15)
dynamicDecay = backlogDecayFactor + (1 - backlogDecayFactor) × bufPressure
effectiveSubmit = submitMed + totalBacklog × dynamicDecay
```

这意味着：
- **积压较浅** → decay 接近 `backlogDecayFactor`（0.3）→ 保守扩容，避免过度反应。
- **积压较深** → `bufPressure` 将 decay 推向 1.0 → 激进扩容，快速消化积压。

此外，scaler 还会补偿预期退出的 worker 数（`exitMed`），并在持锁状态下二次确认防止超容。

### 调优建议

| 场景 | 建议调整 |
| --- | --- |
| 突发短流量 | 增大 `backlogDecayFactor`（如 0.6），加快扩容响应。 |
| 突发 IO 流量 | 调小 `WithDrainHorizon`（如 8ms），让池子更快爬坡到能消化积压的规模。 |
| 平稳可预测负载 | 减小 `scalerPeriod` 至 5ms，更紧密跟踪。 |
| 内存受限 | 降低 `WithWorkerNumCapacity` 限制峰值 worker 数。 |

## 生命周期

**Wait** — 阻塞等待所有已提交任务执行完成：

```go
pool.Wait()
```

**Close** — 停止接收新任务，已提交的任务继续执行完毕。幂等操作，可从任意 goroutine 安全调用。

```go
pool.Close()
```

典型关闭模式：

```go
pool.Close()
pool.Wait()
```

## 自定义 Logger

替换默认的 `log.Default()` 为任何实现了 `Printf`/`Println` 的日志实例：

```go
pool.SetLogger(log.Default())       // 标准库
pool.SetLogger(zapLogger.Sugar())   // zap
```

## 生命周期钩子

`hook` 包提供五类基础生命周期事件及可选拒绝事件的默认分发器。请在提交任务前注册回调，然后通过 `SetHook` 安装：

```go
import (
	"context"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

h := hook.NewHooks()
h.AddTaskSubmitted(func(ctx context.Context) { /* 已提交 */ })
h.AddTaskEnqueued(func(ctx context.Context)  { /* 已入队 */ })
h.AddTaskRejected(func(ctx context.Context)  { /* 接受前被拒绝，可选事件 */ })
h.AddTaskStarted(func(ctx context.Context)   { /* 已开始 */ })
h.AddTaskCompleted(func(ctx context.Context, recovered any) {
	// 正常结束为 nil，否则为 panic 传入的值。
})
h.AddPoolClosed(func(p *agilepool.Pool) { /* 池已关闭 */ })
pool.SetHook(h)
```

- 回调在触发事件的 goroutine 中执行：`Submitted`/`Enqueued`/`Rejected` 在提交方，`Started`/`Completed` 在 worker，`PoolClosed` 在 `Close` 调用方。
- 回调 panic 会被 recover 并记录日志，同一事件的其余回调不受影响。
- `Enqueued` 对每个被接受的任务恰好触发一次。溢出缓冲路径下它在缓冲锁释放后派发，因此可能晚于 `Started`（甚至 `Completed`）被观察到；生命周期事件之间的顺序不做保证。
- 回调不再接收 Task 本身，按任务携带数据请用 `SubmitCtx` 写入 ctx。
- 必须在池开始处理任务前注册：每次分发都会无同步地读取钩子集合。自定义实现只需满足 `agilepool.Hooks` 的五个方法。

## Prometheus 指标

可选的 `metrics` 包使用 Prometheus 文本格式 0.0.4 暴露十个指标，
不新增第三方依赖。任务计数保存在按需启用的 `Collector` 中，
请在首次提交任务前，将其回调与用户回调注册到同一个 dispatcher：

```go
import (
    "github.com/Yiming1997/agilePool/v2/hook"
    "github.com/Yiming1997/agilePool/v2/metrics"
)

collector := metrics.NewCollector(pool)
hooks := hook.NewHooks() // 也支持 hook/dynamic.NewHooks()。
collector.Register(hooks)
// 将用户自己的回调也注册到 hooks。
pool.SetHook(hooks) // 在首次提交任务前安装，运行中不要替换。

mux := http.NewServeMux()
mux.Handle("/metrics", metrics.Handler(collector))
// 也可以写入任意 io.Writer：
// err := metrics.WriteText(w, collector)
```

运行 `go run ./example/prometheus_example`，请求
`http://127.0.0.1:8080/submit` 后访问 `http://127.0.0.1:8080/metrics`。
示例使用 NONBLOCK：接受任务返回 202，拒绝任务返回 503。

| 指标名（默认） | 类型 | 含义 |
| --- | --- | --- |
| `agilepool_workers_running` | gauge | 处于运行循环中的 worker，包含取任务阶段，不含空闲停放的 worker。 |
| `agilepool_workers_idle` | gauge | 等待复用的空闲 worker 对象数。 |
| `agilepool_workers_created_total` | counter | 累计分配的 worker 对象数，不是 goroutine 启动次数。 |
| `agilepool_tasks_submitted_total` | counter | 从注册回调开始观察到的接受任务数；任务内部重试不重复计数。 |
| `agilepool_tasks_completed_total` | counter | 观察到的已结束 worker 处理的任务数，包含恢复的 panic 和取消后跳过的任务，不代表业务成功。 |
| `agilepool_tasks_rejected_total` | counter | 观察到的在接受前因池关闭、NONBLOCK 队列满或取消而拒绝的非 nil 任务，包含提交前已取消的 `SubmitCtx`。 |
| `agilepool_tasks_queued` | gauge | channel 分片与溢出缓冲区的任务数，不含阻塞提交者和 worker 已批量取出的任务。 |
| `agilepool_task_queue_capacity` | gauge | 配置的 channel 总容量，不含溢出缓冲区和分片向上取整增加的槽位。 |
| `agilepool_worker_capacity` | gauge | 配置的运行 worker 数量上限。 |
| `agilepool_tasks_pending` | gauge | 尚未结束的任务数，包含排队、执行、预取和等待入队的任务。 |

nil 任务不计数。接受后再取消的任务计入 completed，不计入 rejected。
计数由 Collector 持有，与 scaler 周期性清零的速率统计独立，新建 Collector 时从零开始。
请在首次提交任务前注册一次，以覆盖池的完整生命周期；未注册的 Collector 不会观察到任务事件。
不要将同一个 Collector 用于多个池。`Register` 支持静态和 dynamic dispatcher；
自定义 dispatcher 可调用 `OnTaskEnqueued`、`OnTaskCompleted` 和 `OnTaskRejected`，
并实现可选的 `agilepool.RejectedHooks` 接口以提供准确拒绝计数。
原有五方法 `Hooks` 接口保持兼容。

completed 回调会在 `Wait` 返回前更新；拒绝回调在 pending 记账平衡之后、提交方法返回之前执行，
核对最终拒绝数前应等待提交者返回。各指标分别采样，不构成一致性快照：
极快的 worker 可能先完成，提交者才记录接受数，因此只应在提交者全部返回、
任务全部排空后核对计数之间的等式。

采集缓冲区长度会使用现有互斥锁，采集本身不安装或替换 hook，也不改变调度逻辑。
queued 是并发环境下的队列占用快照，不能当作全部“尚未开始执行”的任务数。

`metrics.WithNamespace("myapp")` 添加前缀，生成
`myapp_agilepool_workers_running`；`metrics.WithSubsystem("pool")` 生成
`agilepool_pool_workers_running`，两者可以组合，空字符串使用默认命名。
拼接后的指标名必须由 ASCII 字母、下划线和数字组成，且不能以数字开头。
名称不合法、Collector 或池为 nil 时，`WriteText` 返回错误，`Handler` 返回 HTTP 500；
底层 writer 写入错误由 `WriteText` 返回。Handler 支持 GET 和 HEAD，
其他方法返回 405，并携带 `Allow: GET, HEAD`。

这些是供 Prometheus 抓取的应用指标。接入 Kubernetes HPA 还需要配置抓取和
自定义指标适配器，单独暴露 `/metrics` 不会自动启用扩缩容。

运行 `go test ./metrics` 验证文本与 HTTP 行为。将官方 `promtool` 加入 PATH 后，
`go test ./metrics -run TestPromtool -v` 会额外解析并检查输出；工具缺失时仅跳过
这项可选检查。运行 `go test ./metrics -run '^$' -bench BenchmarkWriteText -benchmem`
可测量空闲状态和并发提交负载下的采集开销。

未注册计数回调时，接受和执行任务的路径不增加指标递增操作；拒绝路径会检查可选的拒绝 hook。
启用 Collector 后，即使没有抓取指标，也会产生回调分发和原子计数开销。
上游原版、关闭采集和开启采集的对比见[三组吞吐量报告](docs/benchmarks/prometheus-metrics-optin/README.md)。

## 基准测试

完整的基准测试套件对比了 agilePool 与其他主流 Go goroutine 池的性能表现，详见 [agilePool-benchmark](https://github.com/Yiming1997/agilePool-benchmark)。

## 致谢

自适应扩缩容算法的设计灵感来源于 [Little's Law](https://en.wikipedia.org/wiki/Little%27s_law) 排队论——特别感谢 [@knowledge404](https://github.com/a3141294854) 提出的将排队论应用于 worker pool 自动扩缩容的宝贵建议。

## 许可证

[MIT](LICENSE)
