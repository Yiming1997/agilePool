package agilepool

import (
	"context"
	"sync/atomic"
	"time"
)

type worker struct {
	pool         *Pool
	lastActiveAt time.Time
}

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

loop:
	for {
		select {
		case task, ok := <-w.pool.taskQueue:
			if !ok {
				w.pool.logger.Println("taskQueue closed,exiting")
				w.pool.addRunningWorkersNum(-1)
				atomic.AddInt64(&w.pool.exitCount, 1)
				w.pool.workerPool.Put(w)
				return
			}

			if task == nil {
				w.pool.logger.Println("nil task received, exiting")
				w.pool.addRunningWorkersNum(-1)
				atomic.AddInt64(&w.pool.exitCount, 1)
				w.pool.workerPool.Put(w)
				return
			}
			w.lastActiveAt = time.Now()
			w.runTask(task)

		default:
			// Try the chunked buffer before the second channel check.
			// Grab a batch of up to 8 tasks per lock acquisition to
			// amortise the mutex overhead across multiple tasks and
			// reduce contention with the submission path.
			const batchSize = 8
			var batch [batchSize]Task
			n := 0
			w.pool.taskMu.Lock()
			for n < batchSize {
				t, ok := w.pool.popHead()
				if !ok {
					break
				}
				batch[n] = t
				n++
			}
			w.pool.taskMu.Unlock()
			for i := 0; i < n; i++ {
				w.lastActiveAt = time.Now()
				w.runTask(batch[i])
			}
			if n > 0 {
				continue
			}

			// Lock-free second check: catch tasks that arrived in the
			// tiny window between the two select polls. Submit no longer
			// holds p.lock, so serialisation via lock is unnecessary.
			// If a task slips through both selects, the scaler will
			// spawn workers within scalerPeriod (10ms) to pick it up.
			select {
			case task, ok := <-w.pool.taskQueue:
				if !ok {
					w.pool.logger.Println("taskQueue closed,exiting")
					w.pool.addRunningWorkersNum(-1)
					atomic.AddInt64(&w.pool.exitCount, 1)
					w.pool.workerPool.Put(w)
					return
				}
				if task == nil {
					w.pool.logger.Println("nil task received, exiting")
					w.pool.addRunningWorkersNum(-1)
					atomic.AddInt64(&w.pool.exitCount, 1)
					w.pool.workerPool.Put(w)
					return
				}
				w.lastActiveAt = time.Now()
				w.runTask(task)
			default:
				// Parking: no task found in second check, worker goes idle.
				// Do NOT also put w in workerPool.sync.Pool — see the
				// note at the top of run().
				w.pool.addRunningWorkersNum(-1)
				atomic.AddInt64(&w.pool.exitCount, 1)
				w.pool.addToIdle(w)
				break loop
			}
		}
	}
}

func (w *worker) runTask(task Task) {
	atomic.AddInt64(&w.pool.consumeCount, 1)

	// Extract timing context to trigger queue wait duration recording
	if ct, ok := task.(*contextTask); ok {
		sendCtx := context.WithValue(ct.ctx, StartedAt, time.Now())
		// Create a new context to avoid data race with submit() - never modify ct.ctx directly
		defer func() { // Record completion time at the end
			sendCtx = context.WithValue(sendCtx, CompletedAt, time.Now())
			SendByContext(sendCtx, w.pool.sampleRate)
		}()
	}

	defer func() {
		if p := recover(); p != nil {
			w.pool.logger.Printf("worker exits from panic: %v\n%s\n", p, Stack(1))
		}
	}()
	defer w.pool.done()
	task.Process()
}
