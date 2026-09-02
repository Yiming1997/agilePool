package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

var (
	hookSubmitted atomic.Int64
	hookEnqueued  atomic.Int64
	hookStarted   atomic.Int64
	hookCompleted atomic.Int64
)

// setupHooks installs the selected instrumentation. none is the raw pool
// baseline; hook measures callback dispatch; trace also records timing and
// obtains a TraceID at every lifecycle point.
func setupHooks(pool *agilepool.Pool, mode string) {
	hookSubmitted.Store(0)
	hookEnqueued.Store(0)
	hookStarted.Store(0)
	hookCompleted.Store(0)
	if mode == "none" {
		return
	}
	pool.OnTaskSubmitted(func(context.Context, agilepool.Task) { hookSubmitted.Add(1) })
	pool.OnTaskEnqueued(func(context.Context, agilepool.Task) { hookEnqueued.Add(1) })
	pool.OnTaskStarted(func(context.Context, agilepool.Task) { hookStarted.Add(1) })
	pool.OnTaskCompleted(func(context.Context, agilepool.Task, any) { hookCompleted.Add(1) })
	if mode != "trace" {
		return
	}
	submitted, enqueued, started, completed := agilepool.TimingHook()
	pool.OnTaskSubmitted(withTraceID(submitted))
	pool.OnTaskEnqueued(withTraceID(enqueued))
	pool.OnTaskStarted(withTraceID(started))
	pool.OnTaskCompleted(func(ctx context.Context, task agilepool.Task, recovered any) {
		if pc := agilepool.PoolContextFrom(ctx); pc != nil {
			_, _ = pc.TraceID()
		}
		completed(ctx, task, recovered)
		if pc := agilepool.PoolContextFrom(ctx); pc != nil {
			pc.Timing()
			pc.Release()
		}
	})
}

func withTraceID(next agilepool.TaskHook) agilepool.TaskHook {
	return func(ctx context.Context, task agilepool.Task) {
		if pc := agilepool.PoolContextFrom(ctx); pc != nil {
			_, _ = pc.TraceID()
		}
		next(ctx, task)
	}
}

func submitTask(pool *agilepool.Pool, durFn func() time.Duration, mode string) {
	if mode == "trace" {
		ctx := agilepool.NewContext(context.Background())
		ctx.EnableTiming()
		pool.Submit(agilepool.UpdateTask(ctx, newTask(durFn)))
		return
	}
	pool.Submit(newTask(durFn))
}

func submitTaskWithWG(pool *agilepool.Pool, durFn func() time.Duration, wg *sync.WaitGroup, mode string) bool {
	if mode == "trace" {
		ctx := agilepool.NewContext(context.Background())
		ctx.EnableTiming()
		accepted := pool.TrySubmit(agilepool.UpdateTask(ctx, newTaskWithWG(durFn, wg)))
		if !accepted {
			ctx.Release()
		}
		return accepted
	}
	return pool.TrySubmit(newTaskWithWG(durFn, wg))
}
