// Package dynamic provides an opt-in lifecycle hook dispatcher whose
// callbacks may be registered while a pool is processing tasks.
//
// Each Dispatch method reads one immutable callback snapshot. A callback
// registered during a dispatch therefore applies to later dispatches, never
// to the one already in progress. Different lifecycle events for the same
// task can observe different snapshots.
package dynamic

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// Hooks stores and dispatches lifecycle callbacks. Unlike hook.Hooks, it is
// safe to add callbacks while events are being dispatched. It implements
// agilepool.Hooks.
type Hooks struct {
	mu        sync.Mutex
	snapshots atomic.Pointer[callbackSnapshot]
	logger    log.Logger
}

// callbackSnapshot is one immutable generation of lifecycle callbacks.
// Dispatch reads one snapshot for the entire event, while registrations
// publish a new snapshot for later events.
type callbackSnapshot struct {
	taskSubmitted []agilepool.TaskHook
	taskEnqueued  []agilepool.TaskHook
	taskStarted   []agilepool.TaskHook
	taskCompleted []agilepool.TaskCompleteHook
	poolClosed    []agilepool.PoolHook
}

var _ agilepool.Hooks = (*Hooks)(nil)

// NewHooks returns an empty dynamic dispatcher that logs recovered callback
// panics through the standard logger.
func NewHooks() *Hooks {
	h := &Hooks{logger: *log.Default()}
	h.snapshots.Store(&callbackSnapshot{})
	return h
}

// AddTaskSubmitted registers a callback for future task-submission events.
func (h *Hooks) AddTaskSubmitted(fn agilepool.TaskHook) {
	h.mu.Lock()
	current := h.snapshot()
	next := *current
	next.taskSubmitted = appendCallback(current.taskSubmitted, fn)
	h.snapshots.Store(&next)
	h.mu.Unlock()
}

// AddTaskEnqueued registers a callback for future task-enqueue events.
func (h *Hooks) AddTaskEnqueued(fn agilepool.TaskHook) {
	h.mu.Lock()
	current := h.snapshot()
	next := *current
	next.taskEnqueued = appendCallback(current.taskEnqueued, fn)
	h.snapshots.Store(&next)
	h.mu.Unlock()
}

// AddTaskStarted registers a callback for future task-start events.
func (h *Hooks) AddTaskStarted(fn agilepool.TaskHook) {
	h.mu.Lock()
	current := h.snapshot()
	next := *current
	next.taskStarted = appendCallback(current.taskStarted, fn)
	h.snapshots.Store(&next)
	h.mu.Unlock()
}

// AddTaskCompleted registers a callback for future task-completion events.
func (h *Hooks) AddTaskCompleted(fn agilepool.TaskCompleteHook) {
	h.mu.Lock()
	current := h.snapshot()
	next := *current
	next.taskCompleted = appendCallback(current.taskCompleted, fn)
	h.snapshots.Store(&next)
	h.mu.Unlock()
}

// AddPoolClosed registers a callback for future pool-close events.
func (h *Hooks) AddPoolClosed(fn agilepool.PoolHook) {
	h.mu.Lock()
	current := h.snapshot()
	next := *current
	next.poolClosed = appendCallback(current.poolClosed, fn)
	h.snapshots.Store(&next)
	h.mu.Unlock()
}

// DispatchTaskSubmitted dispatches one stable snapshot of submission callbacks.
func (h *Hooks) DispatchTaskSubmitted(ctx context.Context) {
	for _, fn := range h.snapshot().taskSubmitted {
		h.invoke(func() { fn(ctx) }, "OnTaskSubmitted")
	}
}

// DispatchTaskEnqueued dispatches one stable snapshot of enqueue callbacks.
func (h *Hooks) DispatchTaskEnqueued(ctx context.Context) {
	for _, fn := range h.snapshot().taskEnqueued {
		h.invoke(func() { fn(ctx) }, "OnTaskEnqueued")
	}
}

// DispatchTaskStarted dispatches one stable snapshot of start callbacks.
func (h *Hooks) DispatchTaskStarted(ctx context.Context) {
	for _, fn := range h.snapshot().taskStarted {
		h.invoke(func() { fn(ctx) }, "OnTaskStarted")
	}
}

// DispatchTaskCompleted dispatches one stable snapshot of completion callbacks.
func (h *Hooks) DispatchTaskCompleted(ctx context.Context, recovered any) {
	for _, fn := range h.snapshot().taskCompleted {
		h.invoke(func() { fn(ctx, recovered) }, "OnTaskCompleted")
	}
}

// DispatchPoolClosed dispatches one stable snapshot of pool-close callbacks.
func (h *Hooks) DispatchPoolClosed(pool *agilepool.Pool) {
	if pool == nil {
		h.logger.Println("[ERROR] DispatchPoolClosed: received nil pointer")
		return
	}
	for _, fn := range h.snapshot().poolClosed {
		h.invoke(func() { fn(pool) }, "OnPoolClosed")
	}
}

// snapshot returns the currently published callback generation. The nil
// fallback keeps the zero value of Hooks safe; NewHooks always publishes an
// initial empty snapshot.
func (h *Hooks) snapshot() *callbackSnapshot {
	if snapshot := h.snapshots.Load(); snapshot != nil {
		return snapshot
	}
	return &callbackSnapshot{}
}

// appendCallback returns a new callback slice. Plain append is deliberately
// avoided because it may reuse spare capacity and mutate the backing array
// of an already published snapshot, which lock-free dispatch depends on
// never happening.
func appendCallback[T any](callbacks []T, callback T) []T {
	next := make([]T, len(callbacks)+1)
	copy(next, callbacks)
	next[len(callbacks)] = callback
	return next
}

// invoke runs one callback with per-callback panic recovery. A panicking
// callback must not prevent the remaining callbacks of the same event from
// running.
func (h *Hooks) invoke(fn func(), name string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Printf("hook %s panicked: %v\n%s \n", name, recovered, agilepool.Stack(2))
		}
	}()
	fn()
}
