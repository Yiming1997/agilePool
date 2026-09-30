package agilepool

import (
	"context"
	"sync/atomic"
	"time"
)

type worker struct {
	pool         *Pool
	lastActiveAt time.Time
	// cursor is the shard index this worker starts scanning from. A per-worker
	// start point spreads channel-lock pressure across the handoff shards.
	cursor uint64
}

// DatedTime returns the last-active timestamp of the worker.
// It satisfies the Dated constraint required by the idle container
// implementations in internal/idle.
func (w *worker) DatedTime() time.Time { return w.lastActiveAt }

func newWorker(p *Pool) *worker {
	w := &worker{
		pool: p,
	}
	return w
}
func (w *worker) run(task Task) {
	w.lastActiveAt = time.Now()
	if task != nil {
		w.runTask(task)
	}

	// NOTE: workerPool.Put(w) is intentionally called only on the "terminal"
	// exit paths (queue closed / nil task) below, NOT in a defer. Putting w to
	// the sync.Pool when the worker has just been added to idleWorks would
	// place the same *worker pointer in two containers at once; subsequent
	// Submits could then concurrently spawn two goroutines on the same
	// *worker via Pop and workerPool.Get respectively, causing a data race
	// on w.lastActiveAt and phantom duplicates in idleWorks.

	for {
		// Drain the handoff-queue shards first. Scanning replaces the old
		// double non-blocking channel poll: a task is taken from the first
		// shard that has one, so a busy queue is served with a single channel
		// lock while lock pressure is spread across all shards.
		if t, ok := w.nextShardTask(); ok {
			w.lastActiveAt = time.Now()
			w.runTask(t)
			continue
		}

		// Fall back to the chunked overflow buffer, grabbing a batch per lock
		// acquisition to amortise the mutex overhead and reduce contention
		// with the submission path.
		const batchSize = 8
		var batch [batchSize]Task
		n := w.pool.taskBuf.PopBatch(batch[:])
		if n > 0 {
			for i := 0; i < n; i++ {
				w.lastActiveAt = time.Now()
				w.runTask(batch[i])
			}
			continue
		}

		// Park: both the shards and the overflow buffer are empty. Decrement
		// the running count so the scaler can spawn replacements when new work
		// arrives, then hand the worker back to the idle container.
		// Do NOT also put w in workerPool.sync.Pool — see the note above.
		w.pool.addRunningWorkersNum(-1)
		atomic.AddInt64(&w.pool.exitCount, 1)
		w.pool.addToIdle(w)
		return
	}
}

// nextShardTask scans the handoff-queue shards, starting from this worker's
// cursor, and returns the first available task. The cursor advances to the
// shard that yielded the task so subsequent polls stay on a warm shard and a
// worker keeps hitting the same shard while under load. The pool never closes
// these channels, so a closed channel is treated as empty to avoid a busy loop
// if that ever changes.
func (w *worker) nextShardTask() (Task, bool) {
	p := w.pool
	n := uint64(len(p.taskQueues))
	if n == 0 {
		return nil, false
	}
	start := w.cursor
	for i := uint64(0); i < n; i++ {
		idx := (start + i) & p.shardMask
		select {
		case t, ok := <-p.taskQueues[idx]:
			if !ok || t == nil {
				continue
			}
			w.cursor = idx
			return t, true
		default:
		}
	}
	return nil, false
}

func (w *worker) runTask(task Task) {
	atomic.AddInt64(&w.pool.consumeCount, 1)

	// Balance the submit-side wg.Add exactly once per task. Registered before
	// any hook dispatch and kept on its own defer, so a hook implementation
	// that panics cannot skip done() and deadlock Wait()/Close().
	defer w.pool.done()

	hookCtx := context.Background()
	if wrapped, ok := task.(*contextTask); ok {
		hookCtx = wrapped.ctx
	}

	// Capture task panics so the Completed hook can observe the recovered
	// value. The configured PanicHandler (or the default logger) is invoked
	// via w.pool.handlePanic; w.pool.done() above runs afterwards regardless
	// of what the hooks or handler do.
	var recovered any
	defer func() {
		if p := recover(); p != nil {
			recovered = p
			w.pool.handlePanic(task, p, Stack(1))
		}
		w.pool.dispatchHook(func(h Hooks) {
			h.DispatchTaskCompleted(hookCtx, recovered)
		})
	}()

	w.pool.dispatchHook(func(h Hooks) {
		h.DispatchTaskStarted(hookCtx)
	})
	task.process()
}
