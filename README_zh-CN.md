# agilePool

<p align="center">
  <img src="assets/logo.jpg" alt="agilePool logo" width="260">
</p>

<p align="center">
  <a href="https://github.com/Yiming1997/agilePool/actions/workflows/ci.yml"><img src="https://github.com/Yiming1997/agilePool/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.23.4-00ADD8" alt="Go Version"></a>
  <a href="https://github.com/Yiming1997/agilePool/tags"><img src="https://img.shields.io/github/v/tag/Yiming1997/agilePool?label=tag" alt="Tag"></a>
  <a href="https://pkg.go.dev/github.com/Yiming1997/agilePool"><img src="https://pkg.go.dev/badge/github.com/Yiming1997/agilePool.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/Yiming1997/agilePool"><img src="https://goreportcard.com/badge/github.com/Yiming1997/agilePool" alt="Go Report Card"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/Yiming1997/agilePool" alt="License"></a>
</p>

[English](README.md)

`agilePool` 是一个高性能 Go goroutine 池。核心亮点是**基于滑动窗口速率统计的自适应扩缩容**（adaptive scaler），配合有界并发、空闲 worker 复用、无界任务缓冲（带背压）、可重试任务和优雅关闭——适合需要提交海量小型异步任务、同时又不希望无限制创建 goroutine 的应用。

## 特性

- **自适应扩缩容** — 基于直方图中位数的速率追踪，自动调节 worker 数量匹配提交负载，支持可配置的积压衰减因子应对突发流量。
- **有界 worker** — 通过 `WithWorkerNumCapacity` 限制最大并发数。
- **无界任务缓冲** — 小容量内部交接 channel + 分块链表缓冲区，带背压上限（`maxChunkLen`）。
- **阻塞 / 非阻塞提交模式** — `BLOCK` 模式下任务入队等待；`NONBLOCK` 模式下队列满时丢弃。
- **空闲 worker 自动清理** — 后台清理器定期移除超时空闲 worker。
- **可插拔空闲容器** — LinkedList（FIFO 链表）、MinHeap（LRU 最小堆）、Slice（FIFO 动态数组）、RingQueue（环形缓冲区，O(1) Pop）。
- **可重试任务** — 默认指数退避，支持自定义退避策略。
- **Context 感知提交** — `SubmitCtx` 支持入队前和入队后的协作取消。
- **带超时的提交** — `SubmitBefore` 设定执行截止时间。
- **优雅关闭** — `Wait` 等待所有运行中任务完成；`Close` 停止接收新任务。
- **自定义 Logger** — 可接入任何实现了 `Printf`/`Println` 的日志库（如 `zap.SugaredLogger`）。

## 安装

```bash
go get github.com/Yiming1997/agilePool
```

## 快速开始

```go
package main

import (
	"time"

	agilepool "github.com/Yiming1997/agilePool"
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

> **注意**：`WithTaskQueueSize` 保留以兼容旧 API，但已无实际效果——内部交接 channel 容量固定为 10,000，主缓冲区动态增长。

## 提交任务

### 基础提交

```go
pool.Submit(agilepool.TaskFunc(func() error {
	// 在这里执行任务逻辑。
	return nil
}))
```

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

`SubmitBefore` 给任务设置执行截止时间。若 worker 取到任务时已超时，则跳过。

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

**1. 速率采样** — `statsSampler` 每隔 `statsSamplePeriod`（100ms）原子性地 swap 并清零 `submitCount`、`consumeCount`、`exitCount` 三个计数器，将采样值写入滑动窗口**直方图**。

**2. 中位数提取** — 每个直方图用固定桶 + 环形缓冲区保留最近 `statsWindowSize`（10）个采样。`getMedianRates()` 返回 submit / consume / exit 速率的**近似中位数**——相比平均值，中位数对短暂毛刺的抵抗力强得多。

**3. 扩缩决策** — `scaleIfNeeded()` 计算目标 worker 数：

```
target = submitMed × running / consumeMed          (Little's Law 反推)
```

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

## 基准测试

完整的基准测试套件对比了 agilePool 与其他主流 Go goroutine 池的性能表现，详见 [agilePool-benchmark](https://github.com/Yiming1997/agilePool-benchmark)。

## 致谢

自适应扩缩容算法的设计灵感来源于 [Little's Law](https://en.wikipedia.org/wiki/Little%27s_law) 排队论——特别感谢 [@knowledge404](https://github.com/a3141294854) 提出的将排队论应用于 worker pool 自动扩缩容的宝贵建议。

## 许可证

[MIT](LICENSE)
