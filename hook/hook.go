// Package hook provides the bundled lifecycle callback dispatcher for
// agilePool. Create one with NewHooks, register callbacks, and hand it to
// Pool.SetHook before submitting tasks:
//
//	h := hook.NewHooks()
//	h.AddTaskStarted(func(ctx context.Context) { ... })
//	pool.SetHook(h)
//
// Every callback runs in the goroutine that triggers the event: the
// submitting goroutine for Submitted/Enqueued, a worker for Started/
// Completed, and the Close caller for PoolClosed. A panicking callback is
// recovered and logged, and does not affect the remaining callbacks of the
// same event.
package hook

import (
	"context"
	"log"
	"sync"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// Hooks stores and dispatches lifecycle callbacks. It implements the
// agilepool.Hooks interface.
type Hooks struct {
	mu            sync.RWMutex
	taskSubmitted []agilepool.TaskHook
	taskEnqueued  []agilepool.TaskHook
	taskStarted   []agilepool.TaskHook
	taskCompleted []agilepool.TaskCompleteHook
	poolClosed    []agilepool.PoolHook
	logger        log.Logger
}

// NewHooks returns an empty dispatcher that logs recovered callback panics
// through the standard logger.
func NewHooks() *Hooks {
	return &Hooks{
		logger: *log.Default(),
	}
}

// AddTaskSubmitted registers a callback for task submission.
func (h *Hooks) AddTaskSubmitted(fn agilepool.TaskHook) {
	h.mu.Lock()
	h.taskSubmitted = append(h.taskSubmitted, fn)
	h.mu.Unlock()
}

// AddTaskEnqueued registers a callback for task enqueue.
func (h *Hooks) AddTaskEnqueued(fn agilepool.TaskHook) {
	h.mu.Lock()
	h.taskEnqueued = append(h.taskEnqueued, fn)
	h.mu.Unlock()
}

// AddTaskStarted registers a callback for task start.
func (h *Hooks) AddTaskStarted(fn agilepool.TaskHook) {
	h.mu.Lock()
	h.taskStarted = append(h.taskStarted, fn)
	h.mu.Unlock()
}

// AddTaskCompleted registers a callback for task completion. The callback
// receives the value a panicking task panicked with, or nil on normal exit.
func (h *Hooks) AddTaskCompleted(fn agilepool.TaskCompleteHook) {
	h.mu.Lock()
	h.taskCompleted = append(h.taskCompleted, fn)
	h.mu.Unlock()
}

// AddPoolClosed registers a callback for pool close.
func (h *Hooks) AddPoolClosed(fn agilepool.PoolHook) {
	h.mu.Lock()
	h.poolClosed = append(h.poolClosed, fn)
	h.mu.Unlock()
}

// DispatchTaskSubmitted dispatches submission callbacks. It must only be
// called by the pool submission path.
func (h *Hooks) DispatchTaskSubmitted(ctx context.Context) {
	for _, fn := range h.taskSubmitted {
		h.invoke(func() { fn(ctx) }, "OnTaskSubmitted")
	}
}

// DispatchTaskEnqueued dispatches enqueue callbacks. It must only be called
// by the pool enqueue path.
func (h *Hooks) DispatchTaskEnqueued(ctx context.Context) {
	for _, fn := range h.taskEnqueued {
		h.invoke(func() { fn(ctx) }, "OnTaskEnqueued")
	}
}

// DispatchTaskStarted dispatches start callbacks. It must only be called by
// the worker execution path.
func (h *Hooks) DispatchTaskStarted(ctx context.Context) {
	for _, fn := range h.taskStarted {
		h.invoke(func() { fn(ctx) }, "OnTaskStarted")
	}
}

// DispatchTaskCompleted dispatches completion callbacks. It must only be
// called by the worker completion path.
func (h *Hooks) DispatchTaskCompleted(ctx context.Context, recovered any) {
	for _, fn := range h.taskCompleted {
		h.invoke(func() { fn(ctx, recovered) }, "OnTaskCompleted")
	}
}

// DispatchPoolClosed dispatches pool-close callbacks. It must only be called
// by Pool.Close.
func (h *Hooks) DispatchPoolClosed(pool *agilepool.Pool) {
	if pool == nil {
		h.logger.Println("[ERROR] DispatchPoolClosed: received nil pointer")
		return
	}
	for _, fn := range h.poolClosed {
		h.invoke(func() { fn(pool) }, "OnPoolClosed")
	}
}

func (h *Hooks) invoke(fn func(), name string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Printf("hook %s panicked: %v\n%s \n", name, recovered, agilepool.Stack(2))
		}
	}()
	fn()
}
