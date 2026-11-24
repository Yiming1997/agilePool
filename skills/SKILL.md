---
name: agilepool
description: High-performance adaptive goroutine pool for Go (github.com/Yiming1997/agilePool/v2, Go 1.23.4) — Little's Law auto-scaling from sliding-window medians, sharded handoff queues plus a chunked overflow buffer, five idle-container backends (LinkedList/MinHeap/Slice/RingQueue/Treap), optional spinlock, retry with exponential backoff, context- and deadline-aware submission, panic handler and lifecycle hooks. Use when working in this codebase — adding features, fixing bugs, writing tests, reviewing changes, or answering questions about pool internals, configuration or API usage.
---

# agilePool — Adaptive Goroutine Pool

## Overview

Package `agilepool` (`github.com/Yiming1997/agilePool/v2`, Go 1.23.4) is a goroutine pool that
grows and shrinks itself from measured rates. Key traits, as implemented today:

- **Adaptive scaling** — every `scalerPeriod` (10ms) the scaler computes
  `target = submitMedian × runningWorkers / consumeMedian`, boosts it with backlog weight,
  compensates for worker exits, and clamps it to `pendingTasks` and `capacity`.
- **Sharded handoff queues** — the submit channel is split into a power-of-two number of shards
  (default: `min(GOMAXPROCS, 32)` once the handoff capacity is ≥ 1024) so producers and workers do not
  serialise on one channel lock.
- **Chunked overflow buffer** — when every shard is full, tasks spool into a linked list of
  fixed 4096-slot chunks, hard-capped at 100 000 tasks. Reaching the cap blocks the submitter
  instead of growing memory further.
- **Five idle containers** — `LinkedListType` (default), `MinHeapType`, `SliceType`,
  `RingQueueType`, `TreapType`; `muIdle` can be a `sync.Mutex` or a three-phase spin lock.
- **Task variants** — `TaskFunc`/`NewTask`, `TaskWithRetry` (exponential backoff),
  `SubmitCtx` (cooperative cancellation), `SubmitBefore` (start deadline),
  `UpdateTask` (attach a context for hook dispatch via `Submit`/`TrySubmit`).
- **Panic-safe workers** — a panicking task is recovered; the worker keeps serving, and the
  `Completed` hook observes the recovered value. An optional `PanicHandler` replaces logging.
- **Lifecycle hooks** — Submitted / Enqueued / Started / Completed / PoolClosed, with
  per-callback panic isolation; a dynamic dispatcher allows registration while the pool runs.
- **Graceful shutdown** — `Close()` stops admission and the idle cleaner; `Wait()` blocks until
  every accepted task has completed.

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

`NewConfig()` is mandatory: `Config` has no usable zero value (its fields are unexported, and
`NewPool` passes `cleanPeriod`/`scalerPeriod`/`statsSamplePeriod` straight to `time.NewTicker`,
which panics on a non-positive duration). Always pair a pool with `Close()`; the pool starts three
background goroutines (`expiredWorkerCleaner`, `statsSampler`, `scaler`) in `NewPool`.

## API Reference

### Pool Creation

```go
pool := agilepool.NewPool(config *Config)   // nil → NewConfig()
```

`NewPool(nil)` uses defaults: unbounded workers (`math.MaxInt64`), 10 000 handoff slots,
`BLOCK`, `LinkedListType`, `MutexLock`.

### Task Interface and Types

```go
// Core interface. process() is unexported, so packages outside agilepool cannot
// implement Task; use the adapters below.
type Task interface {
    process()
}

// func() error → TaskFunc → Task
pool.Submit(agilepool.TaskFunc(func() error { return nil }))
pool.Submit(agilepool.NewTask(func() error { return nil }))   // same thing, sugar

// Retry with backoff
pool.Submit(&agilepool.TaskWithRetry{
    MinBackOff: 1 * time.Second,
    MaxBackOff: 200 * time.Second,
    RetryNum:   3,          // EXTRA attempts: total executions = RetryNum + 1
    BackOffStrategy: nil,   // nil → default: min × 2^retryNum, capped at max
    Task: func() error { return errors.New("fail") },  // must be non-nil
})

// Attach a context so hooks receive it through Submit/TrySubmit (SubmitBefore
// wraps the task in a plain TaskFunc, so the context is lost there)
task := agilepool.UpdateTask(ctx, agilepool.TaskFunc(fn))
pool.Submit(task)
```

Default backoff details: the loop runs `i = 1..RetryNum`, sleeping **before** each retry, so the
first sleep is `2 × MinBackOff`. No backoff is taken before the first attempt, and a retry loop
sleeps inside the worker goroutine (the worker slot is held for the whole retry sequence).

`TaskFunc` returns `error` only so it can also be used with `TaskWithRetry`; plain `Submit`
ignores the returned error.

`UpdateTask` returns an unexported `*contextTask`; treat it as an opaque `Task`.

### Submission Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `Submit` | `(task Task)` | Fire-and-forget. Nil/typed-nil task is a no-op. Rejection is invisible. |
| `TrySubmit` | `(task Task) bool` | Reports acceptance. Meaningful with `NONBLOCK`; in `BLOCK` it waits and returns `false` only when the pool is closed (or the task is nil). |
| `SubmitCtx` | `(ctx context.Context, task Task)` | Wraps the task in a `contextTask`; `nil` ctx → `context.Background()`. Already-cancelled ctx → returns before submission (no hooks fire). |
| `SubmitBefore` | `(task Task, timeout time.Duration)` | Start deadline, not an execution timeout: the task is skipped if a worker picks it up after the deadline. In `BLOCK` mode a submitter waiting on backpressure also gives up at the deadline. |

`SubmitCtx` cancellation semantics, stage by stage:

1. `ctx` already done at call time → task dropped, no hook is dispatched.
2. `BLOCK` mode waiting for handoff space → `ctx.Done()` aborts the wait, task dropped.
3. Queued but not started → the worker's `contextTask.process()` sees `ctx.Err() != nil` and skips
   execution (the `Started`/`Completed` hooks still fire, because `runTask` has already begun).
4. Already running → the pool never kills the goroutine; the task must observe `ctx.Done()`.

`SubmitBefore` implementation notes: it wraps the task in a plain `TaskFunc` with a
`context.WithTimeout`, so business code does **not** receive the context, and hooks see
`context.Background()`. `timeout <= 0` effectively guarantees the skip. On acceptance the
`cancel` is deferred until the task finishes; on rejection it is called immediately.

#### Submission pipeline and backpressure

```
submit() → pendingTasks++ → closed? reject → submitCount++ → cold-start worker if running==0
  → Submitted hook
  → NONBLOCK: one non-blocking send to a shard; full → done(), false (NO overflow buffer)
  → BLOCK:    non-blocking shard send; else PushAndForward into the chunked buffer
              (which also tries to forward the buffer head to a shard, preserving FIFO)
              buffer full (100 000) → block on shard send / ctx.Done / closePoolCn
```

Consequences worth remembering:

- `NONBLOCK` never touches the overflow buffer, so "queue full" means the handoff shards only.
- `BLOCK` backpressure only becomes a real block at ~`taskQueueSize + 100 000` outstanding tasks.
- The buffer cap (`maxChunkLen`) and chunk size (`taskChunkSize`) are compile-time constants.
- A cold pool spawns one worker immediately on the first submit (the worker goroutine itself is
  async); further growth is scaler-driven (10ms ticks), so bursts ramp up over a few ticks.
- `Submit`/`SubmitCtx` increment `submitCount` and dispatch `Submitted` before the enqueue
  attempt, so a submission later rejected in `NONBLOCK` mode still produces a `Submitted` event.

### Shutdown

```go
pool.Close()   // idempotent (CAS); stops admission, closes the overflow buffer, wakes waiters
pool.Wait()    // blocks until pendingTasks == 0
```

What `Close` actually does: CAS `closed` 0→1, clears the package-level default pool, marks the
overflow buffer closed (later pushes are rejected; already-buffered tasks still drain), closes
`closePoolCn` (wakes blocked submitters and terminates the idle cleaner), dispatches
`PoolClosed`. It does **not** wait, and it does not stop the sampler or scaler immediately.

What those two do after `Close`: they keep ticking until `closed == 1 && pendingTasks == 0`, so
accepted tasks still get workers while draining. The cleaner exits as soon as `closePoolCn`
closes.

`Wait` returns immediately when `pendingTasks == 0`; it is reusable and safe to call concurrently
with `Submit` (a submission rejected by a closed pool still decrements the counter, so it cannot
panic or strand the waiter). It does not release the background goroutines — `Close` does that.

Recommended pattern (`Close`-then-`Wait`, or `Wait`-then-`Close`, both are valid):

```go
defer pool.Close()   // releases cleaner/sampler/scaler once drained
// ... submit ...
pool.Wait()          // waits for accepted work
```

`Close` may be called from inside a task. `Wait` must not be: the caller's own task is part of
`pendingTasks`, so the count can never reach zero and the call deadlocks.

### Query Methods

```go
pool.GetRunningWorkersNum() int64   // workers currently in the run loop (excludes parked/idle)
pool.GetWorkerCreateCount() int64   // lifetime allocations from sync.Pool.New (cumulative)
pool.GetIdleWorkerCount()   int64   // workers parked in the idle container
pool.GetTaskQueueLen()      int     // tasks in handoff shards only — excludes the chunked buffer
pool.GetTaskQueueCapacity() int     // configured total handoff capacity (not per-shard)
pool.GetCapacity()          int64   // configured worker ceiling
agilepool.GetDefaultPool()  *Pool   // most recently created pool; nil once it is closed
```

There is no exported getter for the overflow-buffer depth; if you need the true backlog, combine
`GetTaskQueueLen()` with your own submit/complete accounting.

### Logger and PanicHandler

```go
pool.SetLogger(l Logger)  // interface { Printf(string, ...interface{}); Println(...interface{}) }
```

`SetLogger` stores the logger atomically, so it may be called while the pool runs; `nil`
restores `log.Default()`.

```go
agilepool.WithPanicHandler(func(task agilepool.Task, recovered any, stack []byte) {
    // route task panics to your error reporting
})
```

The default handler logs `recovered` and the formatted stack via the pool logger. A panicking
handler is itself recovered and logged, so it cannot kill a worker or skip completion bookkeeping.

### Lifecycle Hooks

```go
import "github.com/Yiming1997/agilePool/v2/hook"

h := hook.NewHooks()
h.AddTaskSubmitted(func(ctx context.Context) {})
h.AddTaskEnqueued(func(ctx context.Context) {})
h.AddTaskStarted(func(ctx context.Context) {})
h.AddTaskCompleted(func(ctx context.Context, recovered any) {})
h.AddPoolClosed(func(p *agilepool.Pool) {})

pool.SetHook(h)   // call BEFORE tasks start flowing
```

`Pool.SetHook` stores the dispatcher in a plain field that every dispatch path reads without
synchronisation; replacing it while tasks are in flight is a data race. `SetHook` returns an
`error` for API compatibility and always returns `nil`; passing `nil` disables hooks.

Where callbacks run (all synchronously, in the triggering goroutine):

| Event | Goroutine | Notes |
|-------|-----------|-------|
| `DispatchTaskSubmitted` | submitter | fires per submit attempt that passed the closed check |
| `DispatchTaskEnqueued` | submitter | exactly once per accepted task |
| `DispatchTaskStarted` | worker | immediately before `task.process()` |
| `DispatchTaskCompleted` | worker | after `process`, with the recovered panic value or `nil` |
| `DispatchPoolClosed` | `Close` caller | once, guarded by the CAS in `Close` |

Ordering caveat: buffer-accepted tasks dispatch `Enqueued` after the buffer lock is released, so
`Enqueued` can be observed after `Started` (rarely after `Completed`). `Submitted → Started →
Completed` does hold for every task, because `Submitted` is dispatched before the enqueue attempt
and `Started` only fires after a worker received the task; `Enqueued` is the one event that can
appear out of order.

Other contract points:

- Callbacks do not receive the `Task`; per-task data must travel through `ctx`
  (`SubmitCtx` or `UpdateTask`).
- A panicking callback is recovered and logged; the remaining callbacks of the same event still
  run. `dispatchHook` additionally absorbs panics from a custom `Hooks` implementation.
- Slow callbacks are on the hot path: submitter for Submitted/Enqueued, the worker for
  Started/Completed.
- `hook.Hooks` is the static dispatcher (register before publishing). For registration while the
  pool is processing, use the `dynamic` dispatcher (`hook/dynamic`, `dynamic.NewHooks()`), which
  publishes an immutable callback snapshot per dispatch.

A custom implementation only needs the five methods of `agilepool.Hooks`.

## Configuration Options

All options are `ConfigOption func(*Config)` applied in order by `NewConfig`.
**Invalid values are silently ignored** and the default is kept — there is no error path, so a
setting that appears not to work should be checked against the accepted range.

| Option | Default | Meaning and accepted values |
|--------|---------|-----------------------------|
| `WithCleanPeriod(d time.Duration)` | `500ms` | How often the idle cleaner scans for expired workers. `<= 0` ignored. Does **not** control the expiry age — that is hard-coded to 1s. |
| `WithTaskQueueSize(n int64)` | `10000` | Total capacity of the sharded handoff channel. Split as `ceil(n/shards)` per shard, so the effective total is ≥ n. `<= 0` ignored. |
| `WithTaskQueueShards(n int)` | `0` = auto | Shard count, rounded up to a power of two and never above `taskQueueSize`. `<= 0` → auto: 1 shard if `taskQueueSize < 1024`, else `min(GOMAXPROCS, 32)`. |
| `WithWorkerNumCapacity(n int64)` | `math.MaxInt64` | Hard ceiling on concurrently running workers. `<= 0` ignored. Set it in production. |
| `WithBlockMode(m WorkMode)` | `BLOCK` | `BLOCK` or `NONBLOCK`; any other value behaves like `BLOCK`. |
| `WithIdleContainerType(t IdleContainerType)` | `LinkedListType` | Unknown values fall back to `LinkedListType`. |
| `WithLockType(t LockType)` | `MutexLock` | `MutexLock` or `SpinLock` for `muIdle`. |
| `WithStatsSamplePeriod(d time.Duration)` | `100ms` | Interval at which submit/consume/exit counters are swapped into the histograms. `<= 0` ignored. |
| `WithStatsWindowSize(n int)` | `10` | Number of samples retained per histogram (median horizon). `<= 0` ignored. |
| `WithScalerPeriod(d time.Duration)` | `10ms` | Scaler decision tick. `<= 0` ignored. |
| `WithBacklogDecayFactor(f float64)` | `0.3` | Backlog weight in the scaling target, `[0, 1]`; outside that range ignored. Higher = more aggressive. |
| `WithZeroConsumeScaleInterval(d time.Duration)` | `100ms` | Minimum gap between two growth steps of the zero-consume fallback. `<= 0` ignored. |
| `WithMaxZeroConsumeScaleStep(n int64)` | `64` | Workers a single zero-consume growth step may add. `<= 0` ignored. |
| `WithPanicHandler(h PanicHandler)` | `nil` | `func(task Task, recovered any, stack []byte)`; `nil` keeps logging. |

Hard-coded constants (not configurable): overflow buffer cap `maxChunkLen = 100 000`,
chunk size `taskChunkSize = 4096`, idle expiry `1s`, ring-queue initial capacity `64`,
`minShardableQueueSize = 1024`, `maxAutoShards = 32`.

### WorkMode

- **BLOCK** (default): `Submit` waits for handoff space; if the shards are full the task goes to
  the chunked buffer; only when the buffer is also full does it block. Submitters blocked here
  are woken by `Close` and their task is dropped.
- **NONBLOCK**: one non-blocking shard send; on failure the task is rejected immediately and the
  overflow buffer is not used. `Submit` drops silently — use `TrySubmit` to observe the rejection.

### IdleContainerType

| Type | Add | Pop | Expiry cleanup | Best for |
|------|-----|-----|----------------|----------|
| `LinkedListType` (default) | O(1) | O(1), FIFO | O(n) full scan | general purpose |
| `MinHeapType` | O(log n) | O(log n), LRU by `lastActiveAt` | O(k log n), early stop | many idle workers |
| `SliceType` | O(1) amortized | O(1), FIFO | O(n) scan | moderate idle counts, cache-friendly |
| `RingQueueType` | O(1) amortized (doubles) | O(1), FIFO | O(n) scan | high-frequency Pop, pairs with `SpinLock` |
| `TreapType` | expected O(log n) | expected O(log n), LRU | expected O(log n + k) split subtree | bulk expiry of many workers |

All containers are single-threaded; the pool serialises access through `muIdle`.

### LockType

- `MutexLock` (default) — `sync.Mutex`; prefer when the critical section can become long or
  unpredictable (e.g. expiring a huge idle container).
- `SpinLock` — pure user-space CAS; phase 1 is a 100-iteration CAS loop yielding every 16,
  phase 2 adds exponential `Gosched` backoff for up to 1024 rounds, phase 3 degrades to pure
  `Gosched` so a preempted holder cannot pin the CPU. Never enters the kernel, so it wins when
  hold times are well under 1µs (RingQueue index swaps). `agilepool.NewSpinLock()` exposes one
  as a `sync.Locker` for external use.

## Common Patterns

### Graceful shutdown

```go
pool := agilepool.NewPool(config)
defer pool.Close()   // 1. stop admission; also releases background goroutines once drained

// ... submit tasks ...

pool.Wait()          // 2. wait for accepted work
```

### Producer-consumer with a submit-side WaitGroup

```go
pool := agilepool.NewPool(agilepool.NewConfig(
    agilepool.WithWorkerNumCapacity(runtime.NumCPU() * 2),
))
defer pool.Close()

var wg sync.WaitGroup
for _, item := range items {
    wg.Add(1)
    pool.Submit(agilepool.TaskFunc(func() error {
        defer wg.Done()
        process(item)   // Go 1.22+ per-iteration loop variables; no copy needed
        return nil
    }))
}
wg.Wait()
pool.Wait()
```

### Context-aware submission

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

pool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
    select {
    case <-ctx.Done():
        return ctx.Err()   // the pool will not stop a running task for you
    default:
        return doWork(ctx)
    }
}))
```

### Bounded admission for request handling (NONBLOCK + TrySubmit)

```go
pool := agilepool.NewPool(agilepool.NewConfig(
    agilepool.WithBlockMode(agilepool.NONBLOCK),
    agilepool.WithTaskQueueSize(32),
    agilepool.WithWorkerNumCapacity(8),
))
if !pool.TrySubmit(task) {
    http.Error(w, "system busy", http.StatusTooManyRequests)
}
```

### Retry with backoff

```go
pool.Submit(&agilepool.TaskWithRetry{
    MinBackOff: 100 * time.Millisecond,
    MaxBackOff: 5 * time.Second,
    RetryNum:   3,
    Task:       func() error { return callFlakyAPI() },
})
```

## Anti-Patterns & Pitfalls

1. **`Wait()` from inside a task** — deadlocks: `pendingTasks` includes the calling task, which
   cannot finish while it waits. (`Close()` from inside a task is safe.)
2. **Expecting `Wait()` to stop background goroutines** — it only waits for tasks; call `Close()`
   to release the cleaner/sampler/scaler. Conversely, `Wait()` does not hang when the pool is idle
   (it returns immediately at `pendingTasks == 0`).
3. **`SetHook` after tasks started** — unsynchronised read of the hook field, a data race. Register
   before submitting, or use `hook/dynamic`.
4. **Nil retry task** — `isNilTask` checks `*TaskWithRetry == nil` but not its `Task` field; a nil
   `Task` panics when `process` calls it (recovered, but the task silently does nothing).
5. **Assuming `NONBLOCK` has a deep buffer** — it rejects as soon as the handoff shards fill;
   the 100 000-slot chunked buffer is BLOCK-only.
6. **Relying on `Submit` to report failure** — it returns nothing. Use `TrySubmit`, or inspect
   `GetTaskQueueLen`/your own metrics. After `Close`, every submission is silently dropped.
7. **`GetTaskQueueLen()` as total backlog** — it excludes the chunked buffer; it is only the
   handoff shards.
8. **Forgetting `WithWorkerNumCapacity`** — the default is `math.MaxInt64`; peak worker count is
   then bounded only by `pendingTasks`.
9. **`TaskWithRetry` for latency-sensitive work** — retries sleep inside the worker, holding the
   concurrency slot, and the retry loop has no context awareness.
10. **Copying a `Pool` value** — it embeds mutexes; always pass and store `*Pool`.
11. **Old loop-variable workaround** — `for _, v := range x { v := v }` is unnecessary: the module
    targets Go 1.23.4, which has per-iteration loop variables.

## Testing

The repository has three test surfaces:

- **Root module tests** — `go test ./...`. The package mixes white-box (`package agilepool`,
  e.g. `wait_internal_test.go`, `hook_test.go`) and black-box (`package agilepool_test`)
  tests. `shard_stress_test.go` covers unique execution, NONBLOCK accounting, reentrant submits,
  graceful shutdown and cancellation under stress; `scaler_cold_start_test.go` covers the
  zero-consume fallback. Tests can reach internals such as `pendingTasks`, shard counts and the
  chunked buffer, so keep those invariants in mind when refactoring.
- **Benchmarks** — `benchmark/` is `package benchmark_test` inside the root module:
  `go test ./benchmark -run '^$' -bench .`.
- **Performance harness** — `test/` is a **separate module** (`test/go.mod`, with
  `replace ... => ../`) exposing a plugin-based subcommand CLI: `cd test && go run . --list`.
  It is intentionally not part of `go test ./...`; see `test/README.md` and
  `docs/test-harness.md`.

When adding a test, prefer the external package (`agilepool_test`) if the public API suffices;
drop into `package agilepool` only when the assertion genuinely needs internal state.

## Internal Architecture

File map (all paths relative to the repository root):

- **`pool.go`** — `Pool` struct, `NewPool`, the full submit pipeline, `Close`/`Wait` draining,
  `scaleIfNeeded`, rate sampling, hook dispatch, getters.
- **`config.go`** — `Config` and every `With*` option; re-exports the idle-container constants.
- **`task.go`** — `Task` interface, `TaskFunc`, `TaskWithRetry`, default backoff strategy.
- **`worker.go`** — `worker` state (`lastActiveAt`, per-worker shard `cursor`) and the run loop:
  scan shards from the cursor → batch-pop up to 8 tasks from the overflow buffer → park
  (`runningWorkersNum--`, `exitCount++`, push self into the idle container, return).
- **`task_buffer.go`** — `chunkedTaskBuffer`: 4096-slot chunks, `PushAndForward` (push tail, then
  try to forward the head to a shard while holding `taskMu`), `PopBatch`, chunk recycling.
- **`histogram.go`** — fixed-bucket histogram with FIFO ring eviction; `median()` returns the
  midpoint of the bucket containing the middle sample, i.e. an approximation.
  Submit/consume buckets: 0, 1-5, 6-20, 21-100, 101-500, 501-2000, 2001-10000, 10001+;
  exit buckets: 0, 1-2, 3-5, 6-10, 11-20, 21-50, 51-100, 101+.
- **`spinlock.go`** — three-phase spin lock implementing `sync.Locker`; `NewSpinLock` is exported.
- **`hook.go`** — `Hooks` interface and the `TaskHook`/`TaskCompleteHook`/`PoolHook` types.
- **`hook/hook.go`** — static dispatcher (`hook.NewHooks`), mutex-guarded registration.
- **`hook/dynamic/dynamic.go`** — snapshot dispatcher safe for concurrent registration.
- **`stack_trace.go`** — `Stack(skip)`; opens and scans source files, so never call it on a hot path.
- **`internal/idle/`** — `idle.go` (interface + enum), `linkedlist.go`, `heap.go`, `slice.go`,
  `ring_queue.go`, `treap.go`.
- **`internal/context/`** — a context carrier with labels/trace/timing helpers. **Not wired into
  the pool yet**; the harness rejects `--hook mode=trace` until that integration lands.

### Code Modification Notes

- **`pendingTasks` accounting** — incremented in `addPending()` *before* the closed check, and
  decremented exactly once per submission: on every rejection path, or in `runTask`'s deferred
  `done()` once the task has run. `Wait` is built on the 1→0 transition, so every new rejection
  branch must call `done()` and every new execution path must not add an extra one.
- **Worker ownership** — a `*worker` may live in `idleWorks` or be running, never both. Note that
  `workerPool` is currently allocation-only (`Get` is called, `Put` is never called), so the idle
  container is the only reuse path and `GetWorkerCreateCount` effectively counts every worker ever
  created. Never add a `Put` for a worker that is also in `idleWorks`; that is exactly the
  dual-ownership bug the comment in `worker.go` warns about.
- **Shards** — count is always a power of two (`shardMask = len-1`); per-shard capacity is
  `ceil(taskQueueSize / shards)`; workers rotate a per-worker `cursor` so they do not all probe
  shard 0; `nextShard` is a lock-free round-robin cursor.
- **Buffer lock discipline** — `PushAndForward` calls `tryForward` (a non-blocking channel send)
  while holding `taskMu`. Keep that callback non-blocking; a blocking forward would stall every
  producer and consumer.
- **Hook set is read without synchronisation** — `SetHook` must stay a pre-startup operation.
  Any new dispatch site needs `dispatchHook` so panics cannot break bookkeeping.
- **Background goroutines outlive `Close`** — sampler and scaler stop only on
  `closed == 1 && pendingTasks == 0`; the cleaner stops on `closePoolCn`. Do not change the
  scaler to exit at `Close`, or accepted tasks can be stranded and `Wait` can hang.
- **Scaler invariants** — target is clamped by `pendingTasks` and `capacity`; spawning is
  re-checked under `p.lock`; spare idle workers are reused before allocating;
  `lastZeroConsumeScale` is touched only by the scaler goroutine, so it needs no lock.
- **Rate sampling** — `sampleRates` swaps counters to zero under `statMu`; medians are read under
  the same mutex. Adding a counter means updating sampling, histograms and `getMedianRates`.
- **Idle expiry** — the 1s age is hard-coded in `expiredWorkerCleaner`; `cleanPeriod` only sets
  the scan frequency.
- **`Stack(skip)`** — performs file I/O per frame; only use it on error/panic paths.
