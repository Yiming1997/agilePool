package metrics

import (
	"context"
	"sync/atomic"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// Collector holds opt-in task counters for one pool. Register its callbacks
// before the first submission to observe the pool's full task lifetime.
// A Collector must not be copied after first use or shared between pools.
type Collector struct {
	pool      *agilepool.Pool
	submitted atomic.Int64
	completed atomic.Int64
	rejected  atomic.Int64
}

// NewCollector creates a collector for p. It does not install or replace hooks.
// A nil pool is rejected by WriteText and Handler when collecting metrics.
func NewCollector(p *agilepool.Pool) *Collector {
	return &Collector{pool: p}
}

// Registrar is implemented by hook.Hooks and hook/dynamic.Hooks.
type Registrar interface {
	AddTaskEnqueued(agilepool.TaskHook)
	AddTaskCompleted(agilepool.TaskCompleteHook)
	AddTaskRejected(agilepool.TaskHook)
}

// Register adds the collector's callbacks to h alongside user callbacks. Call
// it once, before submissions start, and install h with Pool.SetHook. No hook
// registration is performed by Handler or WriteText. Custom dispatchers can
// instead call the OnTask methods directly at the corresponding lifecycle events.
func (c *Collector) Register(h Registrar) {
	h.AddTaskEnqueued(c.OnTaskEnqueued)
	h.AddTaskCompleted(c.OnTaskCompleted)
	h.AddTaskRejected(c.OnTaskRejected)
}

// OnTaskEnqueued counts accepted tasks, not submission attempts. Workers may
// finish before this callback runs; concurrent totals are independent snapshots.
func (c *Collector) OnTaskEnqueued(context.Context) { c.submitted.Add(1) }

// OnTaskCompleted counts finished worker handling, including recovered panics
// and tasks skipped after acceptance due to cancellation. It runs before Wait
// wakes, and does not represent business success or individual retry attempts.
func (c *Collector) OnTaskCompleted(context.Context, any) { c.completed.Add(1) }

// OnTaskRejected counts non-nil tasks refused before acceptance. Cancellation
// after acceptance belongs to completion instead. Nil tasks are ignored by the pool.
func (c *Collector) OnTaskRejected(context.Context) { c.rejected.Add(1) }

// GetSubmittedTaskCount returns accepted tasks observed since registration.
func (c *Collector) GetSubmittedTaskCount() int64 { return c.submitted.Load() }

// GetCompletedTaskCount returns completed tasks observed since registration.
func (c *Collector) GetCompletedTaskCount() int64 { return c.completed.Load() }

// GetRejectedTaskCount returns rejected tasks observed since registration.
func (c *Collector) GetRejectedTaskCount() int64 { return c.rejected.Load() }
