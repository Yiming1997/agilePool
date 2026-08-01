package main

import (
	"sync/atomic"

	agilepool "github.com/Yiming1997/agilePool"
)

// hookEnabled is set to true after setupHookTracking successfully registers
// callbacks. When false, readHookCounters returns all zeros (fast path).
var hookEnabled bool

// Atomic counters tracking task lifecycle via pool hooks.
// These are written by the hook callbacks (from many worker goroutines
// concurrently) and read by Couter (single goroutine).  atomic operations
// keep the reads/writes consistent without a separate mutex.
var (
	submittedCount int64
	enqueuedCount  int64
	startedCount   int64
	completedCount int64
)

// setupHookTracking registers task-lifecycle hooks on the pool so that
// the harness can observe how many tasks passed through each stage.
//
// When noHook is true the function is a no-op — hooks are not registered,
// the pool's internal hooks mutex sees zero contention from the harness,
// and readHookCounters returns all zeros.
//
// Each hook callback does a single atomic.AddInt64.  The pool invokes
// hooks from the submitting / dispatching / worker goroutines, so under
// high concurrency those atomics may incur cache-line bouncing.
// See HOOK_USAGE.md for guidance on when to pass --nohook.
func setupHookTracking(pool *agilepool.Pool) {
	hookEnabled = true

	pool.OnTaskSubmitted(func(task agilepool.Task) {
		atomic.AddInt64(&submittedCount, 1)
	})
	pool.OnTaskEnqueued(func(task agilepool.Task) {
		atomic.AddInt64(&enqueuedCount, 1)
	})
	pool.OnTaskStarted(func(task agilepool.Task) {
		atomic.AddInt64(&startedCount, 1)
	})
	pool.OnTaskCompleted(func(task agilepool.Task, recovered any) {
		atomic.AddInt64(&completedCount, 1)
	})
}

// readHookCounters returns a point-in-time snapshot of the four hook
// counters.  Callers that need a consistent cross-counter view should
// treat the values as "at least N" rather than exact simultaneous
// readings, because the underlying counters are advanced independently.
func readHookCounters() (submitted, enqueued, started, completed int64) {
	if !hookEnabled {
		return 0, 0, 0, 0
	}
	return atomic.LoadInt64(&submittedCount),
		atomic.LoadInt64(&enqueuedCount),
		atomic.LoadInt64(&startedCount),
		atomic.LoadInt64(&completedCount)
}
