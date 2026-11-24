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
  
  <a href="LICENSE"><img src="https://img.shields.io/github/license/Yiming1997/agilePool" alt="License"></a>
  <a href="https://agilepool.stardreamer.cloud"><img src="https://img.shields.io/badge/Website-agilepool.stardreamer.cloud-2F6FED?logo=googlechrome&logoColor=white" alt="Website"></a>
  <a href="https://hub.xerj.org/contributors/Yiming1997"><img src="https://hub.xerj.org/contributors/Yiming1997/badge.svg" alt="XERJ Corpus Hub contributor"></a>
  <a href="https://awesome-go.com/#goroutines"><img src="https://awesome.re/mentioned-badge.svg" alt="Mentioned in Awesome Go"></a>
</p>

[简体中文](README_zh-CN.md)

`agilePool` is a high-performance goroutine pool for Go. It features **adaptive worker scaling** driven by sliding-window rate statistics, bounded concurrency, idle worker reuse, unbounded task buffering with backpressure, retryable tasks, and graceful shutdown — ideal for applications submitting millions of small asynchronous jobs without unbounded goroutine growth.

## Features

- **Adaptive scaler** — exact median rate tracking plus a service-time-aware backlog target automatically adjust the worker count to match submission load, with configurable backlog decay for burst handling.
- **Bounded workers** with configurable capacity (`WithWorkerNumCapacity`).
- **Unbounded task buffering** — small internal handoff channel + chunked linked-list buffer with backpressure (`maxChunkLen`).
- **Blocking & non-blocking** submit modes.
- **Automatic idle worker cleanup** — background cleaner purges idle workers that exceed the idle timeout.
- **Pluggable idle containers** — LinkedList (FIFO), MinHeap (LRU), Treap (LRU), Slice (FIFO), RingQueue (circular buffer, O(1) Pop).
- **Retryable tasks** with exponential backoff (default) or custom backoff strategy.
- **Context-aware submission** — `SubmitCtx` supports cancellation before and after queueing.
- **Time-bounded submission** — `SubmitBefore` with a deadline window.
- **Graceful shutdown** — `Wait` blocks until all in-flight tasks complete; `Close` stops new submissions.
- **Custom logger** — plug any `Printf`/`Println` implementation (e.g. `zap.SugaredLogger`).
- **Lifecycle hooks** — `hook.NewHooks()` dispatches Submitted / Enqueued / Started / Completed / PoolClosed callbacks, with an optional Rejected extension; panicking callbacks are isolated and logged.

## Installation

```bash
go get github.com/Yiming1997/agilePool/v2
```

## Quick Start

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
		pool.Submit(agilepool.NewTask(func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}))
	}

	pool.Wait()
}
```

## Configuration

Create a pool with `NewConfig` and functional options:

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

### Available options

| Option | Default | Description |
| --- | --- | --- |
| `WithCleanPeriod` | `500ms` | How often the background cleaner purges expired idle workers. |
| `WithWorkerNumCapacity` | `math.MaxInt64` | Maximum number of running workers. |
| `WithBlockMode` | `BLOCK` | `BLOCK` queues tasks; `NONBLOCK` drops submissions when full. |
| `WithIdleContainerType` | `LinkedListType` | Data structure for idle workers (see [Idle Worker Containers](#idle-worker-containers)). |
| `WithStatsSamplePeriod` | `100ms` | Rate-statistics sampling interval. |
| `WithStatsWindowSize` | `10` | Number of sliding windows for median calculation. |
| `WithScalerPeriod` | `10ms` | How often the scaler evaluates whether to spawn workers. |
| `WithBacklogDecayFactor` | `0.3` | Weight (0–1) for backlog in the scaler target formula. Higher values make the scaler more aggressive at draining queues. |
| `WithZeroConsumeScaleInterval` | `100ms` | Minimum interval between growth steps of the zero-consume fallback: when the pool has queued work but the measured consumption is zero (e.g. a burst of long tasks after an idle period), the scaler grows on queue depth at most once per interval. |
| `WithMaxZeroConsumeScaleStep` | `64` | Maximum number of workers a single zero-consume growth step may add. |
| `WithDrainHorizon` | `30ms` | Time the scaler aims to spend clearing the current backlog. The backlog-driven ramp-up target is `ceil(queuedTasks × serviceTime / drainHorizon)`, the number of workers that can finish the queued work within that time. Shorter horizons ramp up harder and cost more peak workers. |
| `WithPanicHandler` | `nil` | Custom `func(task Task, recovered any, stack []byte)` invoked when a task panics, replacing the default log line. Defaults to logging with `SetLogger`. |

> **Note**: `WithTaskQueueSize` controls the capacity of the internal handoff channel. Tasks beyond that capacity are stored in the dynamically growing chunked buffer.

## Submitting Tasks

### Basic submission

```go
pool.Submit(agilepool.NewTask(func() error {
	// Do work here.
	return nil
}))
```

`NewTask` wraps a plain `func() error` as a task. The `Task` execution method is private, so application code does not need to implement the task interface; use `TaskWithRetry` when retries are needed.

`TaskFunc` returns `error` for compatibility with retry patterns; plain `Submit` does not inspect the returned error.

### Context-aware submission

`SubmitCtx` supports cooperative cancellation:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

pool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Do work here.
		return nil
	}
}))
```

Cancellation semantics:
- If `ctx` is already canceled before submission → task is **dropped**.
- In `BLOCK` mode, waiting for queue space is **interruptible** by `ctx` cancellation.
- If the task is queued but not yet started → worker skips execution when `ctx` is canceled.
- If the task has already started → the pool does **not** forcibly stop the goroutine; the task must check `ctx.Done()` or pass `ctx` to downstream calls.

### Deadline-bound submission

`SubmitBefore` schedules a task with a timeout window. If the deadline expires before the worker picks up the task, it is skipped. In `BLOCK` mode, a call waiting at the backpressure limit also returns when the deadline expires, without enqueueing the task.

```go
pool.SubmitBefore(
	agilepool.TaskFunc(func() error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}),
	10*time.Second,
)
```

## Retryable Tasks

`TaskWithRetry` retries a task when it returns an error. Uses exponential backoff by default; provide `BackOffStrategy` for custom behaviour.

```go
pool.Submit(&agilepool.TaskWithRetry{
	MinBackOff: 1 * time.Second,
	MaxBackOff: 30 * time.Second,
	RetryNum:   3,
	Task: func() error {
		return errors.New("temporary failure")
	},
})
```

## Idle Worker Containers

The pool reuses idle workers. Choose the container that fits your workload:

| Container | Ordering | Pop | Cleanup | Best for |
| --- | --- | --- | --- | --- |
| `LinkedListType` | Insertion order (FIFO) | O(1) | O(n) full scan | General-purpose, simple FIFO reuse. |
| `MinHeapType` | `lastActiveAt` (LRU) | O(log n) | O(k log n) early-stop | Many workers, efficient expiry scan. |
| `TreapType` | `lastActiveAt` (LRU) | Expected O(log n) | Expected O(log n + k) | Large batches of expired idle workers. |
| `SliceType` | Insertion order (FIFO) | O(1) | O(log n + k) binary-search | Moderate idle counts, cache-friendly. |
| `RingQueueType` | Insertion order (FIFO) | O(1) | O(n) scan with wrap | Fixed-capacity idle pool, O(1) Pop. |

```go
pool := agilepool.NewPool(agilepool.NewConfig(
	agilepool.WithIdleContainerType(agilepool.RingQueueType),
	agilepool.WithWorkerNumCapacity(20000),
))
```

## Adaptive Scaler

The scaler is the core mechanism that keeps the worker count aligned with the submission rate. It runs **every `scalerPeriod`** (default 10 ms) and uses a three-step pipeline:

**1. Rate sampling** — Every `statsSamplePeriod` (100 ms), the `statsSampler` swaps and resets the `submitCount` and `exitCount` counters and records them, together with the `consumeCount` delta and the running worker count, into sliding windows.

**2. Median extraction** — Each window retains the last `statsWindowSize` (10) samples **exactly**. `getMedianRates()` returns the **median** submit / consume / exit rate, which is far more resilient to short spikes than an average, and `getRunningAverage()` returns the mean running worker count over the same window.

> The rates are measured exactly on purpose. The scaler derives its target from the *ratio* of two rates, and a bucketed histogram destroyed that ratio: a submit rate of 10000 and a consume rate of 3571 landed in the same bucket and both came back as 6000, so the scaler concluded that it was keeping up and never grew the pool. Exact samples keep the ratio intact.

**3. Scaling decision** — `scaleIfNeeded()` computes the target worker count from two independent signals and takes the larger one:

```
rateTarget  = submitMed × runningAvg / consumeMed          (Little's Law, steady state)
drainTarget = ceil(queuedTasks × serviceTime / drainHorizon)  (burst ramp-up)
target      = max(rateTarget, drainTarget)
```

`runningAvg` is the **windowed average** concurrency, not the live worker count. That distinction matters: the implied service time (`runningAvg / consumeMed`) is a property of the observed window, so a scale-up cannot raise the pool's own target. With the live count the target was proportional to the current worker size, which let one burst multiply the pool on every scaler tick until the backlog brake stopped it.

| | live `running` | windowed `runningAvg` |
| --- | --- | --- |
| Target after a scale-up | rises again next tick | unchanged within the window |
| Burst peak goroutines (20k tasks/s, 1 ms) | ~440 | ~39 |

`drainTarget` closes the gap the window leaves behind. The rate target only moves once per statistics window, so a cold pool that receives a burst stays at one worker for a whole window and then grows one bounded step per window — under IO workloads the burst is over before the target reacts and the throughput is lost.

`drainTarget` is the number of workers that can finish the queued work within `drainHorizon` (default 30 ms). Its service time is measured from the completions of the previous scaler tick, so it is available within a tick rather than a whole window, and it describes the workload rather than the queue depth: a pool serving slow tasks opens a large one for the same queue, a pool serving fast tasks keeps a small one. The target contains no worker count, so it cannot feed back on the pool's own scaling decision, and it shrinks with the queue, so it cannot run away.

When a backlog exists (tasks in the channel + chunked buffer), the scaler becomes more aggressive using a **dynamic decay factor**:

```
bufPressure  = min(1.0, bufDepth / consumeMed × 0.15)
dynamicDecay = backlogDecayFactor + (1 - backlogDecayFactor) × bufPressure
effectiveSubmit = submitMed + totalBacklog × dynamicDecay
```

This means:
- **Shallow backlog** → decay stays near `backlogDecayFactor` (0.3) → conservative scaling.
- **Deep backlog** → `bufPressure` pushes decay toward 1.0 → aggressive scaling to drain quickly.

The scaler also compensates for expected worker exits (`exitMed`) and re-checks under lock for thread safety before spawning.

### Tuning the scaler

| Scenario | Recommended tweak |
| --- | --- |
| Bursty, short-lived traffic | Increase `backlogDecayFactor` (e.g. 0.6) for faster ramp-up. |
| Bursty, IO-bound traffic | Lower `WithDrainHorizon` (e.g. 10ms) so the pool opens up fast enough to drain the backlog. |
| Steady, predictable load | Decrease `scalerPeriod` to 5 ms for tighter tracking. |
| Memory-constrained | Lower `WithWorkerNumCapacity` to cap peak workers. |

## Lifecycle

**Wait** — blocks until all submitted tasks complete:

```go
pool.Wait()
```

**Close** — stops accepting new tasks. Already submitted tasks continue to run. Idempotent and safe from any goroutine.

```go
pool.Close()
```

Typical shutdown pattern:

```go
pool.Close()
pool.Wait()
```

## Custom Logger

Replace the default `log.Default()` logger with any implementation of `Printf`/`Println`:

```go
pool.SetLogger(log.Default())       // stdlib
pool.SetLogger(zapLogger.Sugar())   // zap
```

## Lifecycle Hooks

The `hook` package ships the default dispatcher for the five lifecycle events. Register callbacks before submitting tasks, then install it with `SetHook`:

```go
import (
	"context"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

h := hook.NewHooks()
h.AddTaskSubmitted(func(ctx context.Context) { /* submitted */ })
h.AddTaskEnqueued(func(ctx context.Context)  { /* enqueued */ })
h.AddTaskRejected(func(ctx context.Context)  { /* refused before acceptance; optional event */ })
h.AddTaskStarted(func(ctx context.Context)   { /* started */ })
h.AddTaskCompleted(func(ctx context.Context, recovered any) {
	// recovered is nil on normal exit, otherwise the value passed to panic.
})
h.AddPoolClosed(func(p *agilepool.Pool) { /* closed */ })
pool.SetHook(h)
```

- Callbacks run in the goroutine that triggers the event: the submitting goroutine for `Submitted`/`Enqueued`/`Rejected`, a worker for `Started`/`Completed`, and the `Close` caller for `PoolClosed`.
- A panicking callback is recovered and logged; the remaining callbacks of that event still run.
- `Enqueued` fires exactly once per accepted task. On the overflow-buffer path it is dispatched after the buffer lock is released, so it may be observed after `Started` (or even `Completed`); ordering between lifecycle events is not guaranteed.
- The task itself is not passed to callbacks; carry per-task data through `ctx` via `SubmitCtx`.
- Register before the pool starts processing tasks: the hook set is read without synchronization on every dispatch. A custom implementation only needs the five methods of `agilepool.Hooks`.

## Prometheus metrics

The optional `metrics` package exposes ten pool statistics using Prometheus text
format 0.0.4. Task counters live in an opt-in `Collector` with no additional dependencies.
Register its callbacks alongside your own hooks before the first submission:

```go
import (
    "github.com/Yiming1997/agilePool/v2/hook"
    "github.com/Yiming1997/agilePool/v2/metrics"
)

collector := metrics.NewCollector(pool)
hooks := hook.NewHooks() // hook/dynamic.NewHooks() also works.
collector.Register(hooks)
// Add your own callbacks to the same dispatcher.
pool.SetHook(hooks) // Install once before submitting tasks.

mux := http.NewServeMux()
mux.Handle("/metrics", metrics.Handler(collector))
// Or write to any io.Writer:
// err := metrics.WriteText(w, collector)
```

Run `go run ./example/prometheus_example`, request
`http://127.0.0.1:8080/submit`, then inspect `http://127.0.0.1:8080/metrics`.
The example uses NONBLOCK and returns 503 for rejected tasks, 202 for accepted tasks.

| Metric (default name) | Type | Meaning |
| --- | --- | --- |
| `agilepool_workers_running` | gauge | Workers in the run loop, including fetching tasks; excludes parked workers. |
| `agilepool_workers_idle` | gauge | Worker structs parked for reuse. |
| `agilepool_workers_created_total` | counter | Lifetime worker struct allocations, not goroutine starts. |
| `agilepool_tasks_submitted_total` | counter | Accepted tasks observed since registration; each retrying task counts once. |
| `agilepool_tasks_completed_total` | counter | Observed tasks whose worker handling finished, including recovered panics and cancellation skips; not a success count. |
| `agilepool_tasks_rejected_total` | counter | Observed non-nil tasks refused before acceptance: closed pool, NONBLOCK saturation, or cancellation (including an already-cancelled `SubmitCtx`). |
| `agilepool_tasks_queued` | gauge | Handoff shards plus overflow buffer; excludes blocked submitters and batches already fetched by workers. |
| `agilepool_task_queue_capacity` | gauge | Configured handoff capacity; excludes overflow capacity and extra slots from shard rounding. |
| `agilepool_worker_capacity` | gauge | Configured maximum running workers. |
| `agilepool_tasks_pending` | gauge | Unfinished tasks, including queued, executing, prefetched, and blocked submissions. |

Nil tasks are ignored. Cancellation after acceptance counts as completion, not
rejection. Counters belong to the collector, are independent of the scaler's
resettable statistics, and start at zero when a collector is created. Register
once before the first submission for full pool-lifetime totals; an unregistered
collector observes no task events. Do not share one collector across pools.
`Register` supports both bundled dispatchers; custom dispatchers can call the
collector's `OnTaskEnqueued`, `OnTaskCompleted`, and `OnTaskRejected` callbacks.
They must implement the optional `agilepool.RejectedHooks` for exact rejection totals.
The existing five-method `Hooks` interface remains compatible.

Completion callbacks run before `Wait` returns. Rejection callbacks run after
pending bookkeeping is balanced and before the submission method returns; join
submitters before checking final rejection totals. Values are independent snapshots:
a very fast worker can complete before
its submitter records acceptance, so cross-metric equalities are only meaningful
after submitters have returned and the pool has drained.

Collection reads the overflow-buffer length under its existing mutex; it does not
install or replace hooks, or alter scheduling. Queue occupancy is an approximate concurrent
snapshot, not the exact number of all tasks that have yet to start.

`metrics.WithNamespace("myapp")` adds a prefix, producing
`myapp_agilepool_workers_running`; `metrics.WithSubsystem("pool")` produces
`agilepool_pool_workers_running`. Both options can be combined. Empty values use
the default names. Prefixes must form ASCII metric identifiers (letters,
underscores, digits, with no leading digit). Invalid names or a nil collector/pool return an
error from `WriteText`, or HTTP 500 from `Handler`. Writer errors are returned.
The handler supports GET and HEAD; other methods receive 405 with `Allow: GET, HEAD`.

These are scrapeable application metrics. Kubernetes HPA integration additionally
requires Prometheus scraping and a custom metrics adapter; exposing `/metrics`
alone does not configure autoscaling.

Validation: `go test ./metrics` covers the text and HTTP contracts. With the
official `promtool` on PATH, `go test ./metrics -run TestPromtool -v` also parses
and lints the output (otherwise that optional check is skipped).
Use `go test ./metrics -run '^$' -bench BenchmarkWriteText -benchmem` to measure
collection while idle and under concurrent submissions.

Without collector callbacks, accepted-task and worker execution paths perform
no new metric increments. Rejection paths check for the optional rejection hook.
Enabling a collector adds callback dispatch and atomic counting even when metrics
are not scraped. See the [three-way throughput report](docs/benchmarks/prometheus-metrics-optin/README.md)
for upstream, collector-disabled, and collector-enabled measurements.

## Benchmark

A comprehensive benchmark suite comparing agilePool against other popular Go goroutine pools is available at [agilePool-benchmark](https://github.com/Yiming1997/agilePool-benchmark).

## Acknowledgments

The adaptive scaler design draws inspiration from [Little's Law](https://en.wikipedia.org/wiki/Little%27s_law) — special thanks to [@knowledge404](https://github.com/a3141294854) for the insightful suggestion of applying queuing theory to worker pool auto-scaling.

## License

[MIT](LICENSE)
