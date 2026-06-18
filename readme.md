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

[简体中文](README.zh-CN.md)

`agilePool` is a high-performance goroutine pool for Go. It features **adaptive worker scaling** driven by sliding-window rate statistics, bounded concurrency, idle worker reuse, unbounded task buffering with backpressure, retryable tasks, and graceful shutdown — ideal for applications submitting millions of small asynchronous jobs without unbounded goroutine growth.

## Features

- **Adaptive scaler** — histogram-based median rate tracking automatically adjusts the worker count to match submission load, with configurable backlog decay for burst handling.
- **Bounded workers** with configurable capacity (`WithWorkerNumCapacity`).
- **Unbounded task buffering** — small internal handoff channel + chunked linked-list buffer with backpressure (`maxChunkLen`).
- **Blocking & non-blocking** submit modes.
- **Automatic idle worker cleanup** — background cleaner purges idle workers that exceed the idle timeout.
- **Pluggable idle containers** — LinkedList (FIFO), MinHeap (LRU), Slice (FIFO), RingQueue (circular buffer, O(1) Pop).
- **Retryable tasks** with exponential backoff (default) or custom backoff strategy.
- **Context-aware submission** — `SubmitCtx` supports cancellation before and after queueing.
- **Time-bounded submission** — `SubmitBefore` with a deadline window.
- **Graceful shutdown** — `Wait` blocks until all in-flight tasks complete; `Close` stops new submissions.
- **Custom logger** — plug any `Printf`/`Println` implementation (e.g. `zap.SugaredLogger`).

## Installation

```bash
go get github.com/Yiming1997/agilePool
```

## Quick Start

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

> **Note**: `WithTaskQueueSize` is retained for API compatibility but has no effect — the internal handoff channel capacity is fixed at 10,000 slots and the primary buffer grows dynamically.

## Submitting Tasks

### Basic submission

```go
pool.Submit(agilepool.TaskFunc(func() error {
	// Do work here.
	return nil
}))
```

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

`SubmitBefore` schedules a task with a timeout window. If the deadline expires before the worker picks up the task, it is skipped.

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

**1. Rate sampling** — Every `statsSamplePeriod` (100 ms), the `statsSampler` atomically swaps and resets three counters — `submitCount`, `consumeCount`, and `exitCount` — and records them into sliding-window **histograms**.

**2. Median extraction** — Each histogram retains the last `statsWindowSize` (10) samples in a fixed-bucket ring buffer. `getMedianRates()` returns the **approximate median** submit / consume / exit rate, which is far more resilient to short spikes than an average.

**3. Scaling decision** — `scaleIfNeeded()` computes the target worker count:

```
target = submitMed × running / consumeMed          (Little's Law)
```

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

## License

[MIT](LICENSE)
