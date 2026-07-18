---
name: agilepool
description: High-performance adaptive goroutine pool for Go with auto-scaling via Little's Law, sliding-window rate statistics, chunked overflow buffer, multiple idle container backends (LinkedList/MinHeap/Slice/RingQueue), customizable spinlock, retry with exponential backoff, and context-aware submission. Use when working with the github.com/Yiming1997/agilePool/v2 codebase — adding features, fixing bugs, writing tests, or answering questions about pool internals and API usage.
---

# agilePool — Adaptive Goroutine Pool

## Overview

Package `agilepool` (`github.com/Yiming1997/agilePool/v2`, Go 1.23.4) provides an adaptive goroutine pool that dynamically scales worker count using Little's Law. It features:

- **Adaptive scaling**: Median rate sampling over a sliding window → `target = submitMedian × runningWorkers / consumeMedian`, with backlog-weighted boosting and exit-rate compensation.
- **Chunked overflow buffer**: When the handoff channel is full, tasks spool into a linked-list of fixed-size chunks (4096 slots each). Soft cap at 100k total tasks; exceeding it blocks on the channel instead of growing unboundedly.
- **Multiple idle containers**: `LinkedListType` (default FIFO), `MinHeapType`, `SliceType`, `RingQueueType` — pluggable via config.
- **Configurable locking**: `muIdle` lock can be `sync.Mutex` (default) or a custom `spinLock` (three-phase: CAS → exponential backoff → Gosched).
- **Task variants**: `Task` interface, `TaskFunc` (bare func), `TaskWithRetry` (exponential backoff), context-aware via `SubmitCtx`, timeout via `SubmitBefore`.
- **Panic-safe**: Workers recover from panics and continue processing.
- **Graceful shutdown**: `Close()` + `Wait()` pattern.

## Quick Start

```go
import agilepool "github.com/Yiming1997/agilePool/v2"

pool := agilepool.NewPool(agilepool.NewConfig(
    agilepool.WithWorkerNumCapacity(1000),
    agilepool.WithTaskQueueSize(10000),
))
defer pool.Close()

pool.Submit(agilepool.TaskFunc(func() error {
    // your work here
    return nil
}))
pool.Wait()
```

## API Reference

### Pool Creation

```go
pool := agilepool.NewPool(config *Config)
```

`NewPool(nil)` uses defaults (unlimited workers, LinkedListType, MutexLock).

### Task Interface and Types

```go
// Core interface
type Task interface {
    Process()
}

// Simple function adapter
type TaskFunc func() error   // func() error → TaskFunc → Task
pool.Submit(agilepool.TaskFunc(func() error { return nil }))

// Retry with backoff
pool.Submit(&agilepool.TaskWithRetry{
    MinBackOff: 1 * time.Second,
    MaxBackOff: 200 * time.Second,
    RetryNum:   3,                    // retries on error, so total attempts = RetryNum + 1
    BackOffStrategy: nil,             // nil → default exponential: min * 2^retryNum, capped at max
    Task: func() error { return errors.New("fail") },
})
```

### Submission Methods

| Method | Behavior |
|--------|----------|
| `pool.Submit(task)` | Enqueues task; no-op on nil. Returns void. |
| `pool.TrySubmit(task) bool` | Returns `false` if rejected (pool closed, nil task, or NONBLOCK mode queue full). |
| `pool.SubmitCtx(ctx, task)` | Wraps task with context; skips execution if `ctx.Err() != nil` at dequeue time. |
| `pool.SubmitBefore(task, timeout)` | Skips task if not started within `timeout`. |

### Shutdown

```go
pool.Close()   // Idempotent. Stops cleaner/scaler. New submits become no-ops.
pool.Wait()    // Blocks until all in-flight tasks complete.
// Always: Close() first, then Wait().
```

**Critical**: Call `Close()` before `Wait()`. Calling `Wait()` alone blocks forever because the background goroutines never exit.

### Query Methods

```go
pool.GetRunningWorkersNum() int64   // currently active goroutines
pool.GetWorkerCreateCount() int64   // lifetime sync.Pool allocations
pool.GetIdleWorkerCount()   int64   // parked in idle container
pool.GetTaskQueueLen()      int     // len(taskQueue) — does NOT include chunked buffer
pool.GetCapacity()          int64   // max worker limit
agilepool.GetDefaultPool() *Pool    // most recently created pool (nil after Close)
```

### Logger

```go
pool.SetLogger(l Logger)  // interface { Printf(string, ...interface{}); Println(...interface{}) }
// Pass *log.Logger, zap.SugaredLogger, etc.
```

## Configuration Options

All via functional options pattern:

```go
agilepool.NewConfig(
    agilepool.WithCleanPeriod(500 * time.Millisecond),       // expired worker cleanup interval
    agilepool.WithTaskQueueSize(10000),                      // handoff channel capacity
    agilepool.WithWorkerNumCapacity(20000),                  // max concurrent workers
    agilepool.WithBlockMode(agilepool.BLOCK),                // BLOCK (default) or NONBLOCK
    agilepool.WithIdleContainerType(agilepool.LinkedListType), // LinkedList/MinHeap/Slice/RingQueue
    agilepool.WithLockType(agilepool.SpinLock),              // MutexLock (default) or SpinLock
    agilepool.WithStatsSamplePeriod(100 * time.Millisecond), // rate sampling interval
    agilepool.WithStatsWindowSize(10),                       // sliding window count for median
    agilepool.WithScalerPeriod(10 * time.Millisecond),       // scaler tick interval
    agilepool.WithBacklogDecayFactor(0.3),                   // queue backlog weight in scaler (0-1)
)
```

### WorkMode

- **BLOCK** (default): `Submit` blocks until the task is accepted (channel or chunked buffer).
- **NONBLOCK**: `Submit` drops the task when the handoff channel is full. Use `TrySubmit` to detect rejection.

### IdleContainerType

| Type | Add | Pop | Best for |
|------|-----|-----|----------|
| `LinkedListType` | O(1) | O(1) | General purpose (default) |
| `MinHeapType` | O(log n) | O(log n) | Ordered by lastActiveAt |
| `SliceType` | O(1) | O(n) | Simple, small pools |
| `RingQueueType` | O(1) | O(1) | High-frequency Pop; works well with SpinLock |

## Common Patterns

### Graceful Shutdown

```go
pool := agilepool.NewPool(config)
defer pool.Close()        // 1. Stop accepting new work

// ... submit tasks ...

pool.Wait()               // 2. Wait for completion
```

### Producer-Consumer

```go
pool := agilepool.NewPool(agilepool.NewConfig(
    agilepool.WithWorkerNumCapacity(runtime.NumCPU() * 2),
))
defer pool.Close()

var wg sync.WaitGroup
for _, item := range items {
    wg.Add(1)
    it := item
    pool.Submit(agilepool.TaskFunc(func() error {
        defer wg.Done()
        process(it)
        return nil
    }))
}
wg.Wait()
pool.Wait()
```

### Context-Aware with Cancellation

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

pool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        return doWork()
    }
}))
```

## Anti-Patterns & Pitfalls

1. **Don't call `Wait()` before `Close()`** — the pool has background goroutines (cleaner, scaler, sampler). `Wait()` without `Close()` blocks forever.

2. **Don't `Close()` from within a task without defer** — `Close()` stops the cleaner goroutine, causing idle workers to accumulate. Closing inside a task is safe but use `defer pool.Close()` only at the outer scope.

3. **Nil `*TaskWithRetry` panics** — `isNilTask` checks `*TaskWithRetry == nil` but not its `Task` field. Ensure `Task` is non-nil.

4. **`SubmitCtx` with `nil` context** — automatically uses `context.Background()`. An already-canceled context causes the task to be silently dropped.

5. **`GetTaskQueueLen()` is incomplete** — it only reports `len(taskQueue)`, not the chunked overflow buffer. For true backlog, check your own metrics.

6. **Task closure capture** — Go loop variable capture applies. Always copy:
   ```go
   for _, v := range items {
       v := v
       pool.Submit(agilepool.TaskFunc(func() error { use(v); return nil }))
   }
   ```

## Testing

Tests use package `agilepool_test` (external test package) — import `github.com/Yiming1997/agilePool/v2` and use the public API. Use `testing.Short()` guards for heavy benchmarks:

```go
if testing.Short() {
    taskCount = 20000  // reduced
}
```

## Internal Architecture (when modifying code)

- **`pool.go`**: Core Pool struct, NewPool, Submit path (channel → chunked buffer → backpressure), scaler with `scaleIfNeeded()`, Close/Wait.
- **`config.go`**: `Config` struct and `With*` functional options.
- **`worker.go`**: `worker.run()` — three-phase task acquisition loop: (1) channel receive, (2) chunked buffer batch (up to 8 per lock), (3) second channel check before parking.
- **`task.go`**: `Task` interface, `TaskFunc`, `TaskWithRetry` (exponential backoff default: `min × 2^retryNum`, capped at `max`).
- **`idle_container.go`**: `IdleWorkerContainer` interface + `IdleContainerType` enum.
- **`idle_linkedlist.go` / `idle_heap.go` / `idle_slice.go` / `idle_ring_queue.go`**: Concrete implementations.
- **`histogram.go`**: Fixed-bucket sliding-window histogram for rate sampling. Used by scaler to compute median submit/consume/exit rates.
- **`spinlock.go`**: Three-phase spin lock implementing `sync.Locker`. Used for `muIdle` when `WithLockType(SpinLock)`.
- **`stack_trace.go`**: `Stack(skip)` for panic recovery — reads source files, so not for production hot paths.

### Code Modification Notes

- `pendingTasks` is atomic — decremented in `pool.done()`, incremented on submit. It covers submitters blocked on the channel, giving the scaler full visibility.
- Workers are pooled via `sync.Pool`. **Do NOT** `workerPool.Put(w)` when the worker is also in `idleWorks` — that causes dual ownership and data races.
- The scaler runs every `scalerPeriod` (default 10ms). It reads median rates from histograms, computes target via Little's Law with backlog-weighted boosting.
- `taskMu` protects the chunked buffer. Workers batch-pop up to 8 tasks per lock acquisition to amortize contention.
- The spinlock does NOT call into the kernel — optimal when hold times are <1µs (e.g., RingQueue index swaps). Use `MutexLock` for longer or unpredictable hold times.
