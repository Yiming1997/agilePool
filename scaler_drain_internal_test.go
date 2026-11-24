package agilepool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// seedTick makes the next measureServiceTime call report the given number of
// completed tasks over a 10ms tick, which is what the scaler sees in
// production.
func seedTick(p *Pool, completed int64) {
	p.lastTickDone = atomic.LoadInt64(&p.doneCount) - completed
	p.lastTickAt = time.Now().Add(-10 * time.Millisecond)
}

// blockTasks submits n tasks that block until the returned release function is
// called, so the queue keeps its depth while the test inspects it.
func blockTasks(p *Pool, n int) (release func(), done func()) {
	var wg sync.WaitGroup
	wg.Add(n)
	ch := make(chan struct{})
	for i := 0; i < n; i++ {
		p.Submit(TaskFunc(func() error {
			defer wg.Done()
			<-ch
			return nil
		}))
	}
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) },
		func() { wg.Wait() }
}

// TestDrainTargetScalesWithTheMeasuredServiceTime is the core of the
// backlog-driven ramp-up target.
//
// The queue depth alone cannot say how many workers a backlog needs: 3000
// queued tasks are a lot for a pool that serves them in 100us each, and
// nothing at all for a pool that serves them in 10ms each. The target
// therefore multiplies the queue depth with the measured service time:
//
//	drainTarget = ceil(queuedTasks * serviceTime / drainHorizon)
func TestDrainTargetScalesWithTheMeasuredServiceTime(t *testing.T) {
	newPool := func() *Pool {
		return NewPool(NewConfig(
			WithScalerPeriod(time.Hour),
			WithStatsSamplePeriod(time.Hour),
			WithWorkerNumCapacity(100),
			WithDrainHorizon(30*time.Millisecond),
		))
	}

	// Fast tasks: 100 completions in a 10ms tick by one worker is 100us per
	// task. 3000 queued tasks are 0.3s of work, which 30ms of horizon turns
	// into 10 workers.
	fast := newPool()
	defer fast.Close()
	seedTick(fast, 100)
	releaseFast, doneFast := blockTasks(fast, 3000)
	defer func() { releaseFast(); doneFast() }()
	fast.scaleIfNeeded()
	if got := fast.GetRunningWorkersNum(); got < 6 || got > 20 {
		t.Fatalf("running workers = %d, want the drain target of about 10 for a 100us service time", got)
	}

	// Slow tasks: one completion in a 10ms tick is a 10ms service time. The
	// same queue now needs the capacity.
	slow := newPool()
	defer slow.Close()
	seedTick(slow, 1)
	releaseSlow, doneSlow := blockTasks(slow, 3000)
	defer func() { releaseSlow(); doneSlow() }()
	slow.scaleIfNeeded()
	if got := slow.GetRunningWorkersNum(); got != 100 {
		t.Fatalf("running workers = %d, want the drain target to reach the capacity for a 10ms service time", got)
	}
}

// TestDrainTargetNeedsAServiceTimeMeasurement checks that the target stays
// out of the way while the pool has not measured anything yet. A pool that
// just started, or one whose tasks have not finished a single tick, has no
// service time and must fall back to the rate-based target alone.
//
// The pool still grows by one worker: the zero-consume fallback adds a single
// bounded step when work is queued but nothing has completed yet. What must
// not happen is the drain target sizing the pool for a service time that was
// never measured - that would be the 100 workers a 10ms service time asks
// for.
func TestDrainTargetNeedsAServiceTimeMeasurement(t *testing.T) {
	p := NewPool(NewConfig(
		WithScalerPeriod(time.Hour),
		WithStatsSamplePeriod(time.Hour),
		WithWorkerNumCapacity(100),
		WithDrainHorizon(30*time.Millisecond),
	))
	defer p.Close()

	seedTick(p, 0)
	release, done := blockTasks(p, 3000)
	defer func() { release(); done() }()

	p.scaleIfNeeded()
	if got := p.GetRunningWorkersNum(); got > 2 {
		t.Fatalf("running workers = %d, want the pool to stay at its cold-start size without a service time measurement", got)
	}
}

// TestDrainTargetDoesNotFeedBackOnItself is the regression test for the
// self-referential target.
//
// The drain target is a function of the queue depth and the measured service
// time. Neither contains the worker count, so repeated scaling decisions on
// unchanged state must leave the pool at the same size.
func TestDrainTargetDoesNotFeedBackOnItself(t *testing.T) {
	p := NewPool(NewConfig(
		WithScalerPeriod(time.Hour),
		WithStatsSamplePeriod(time.Hour),
		WithWorkerNumCapacity(1000),
		WithDrainHorizon(30*time.Millisecond),
	))
	defer p.Close()

	seedTick(p, 100)
	release, done := blockTasks(p, 3000)
	defer func() { release(); done() }()

	p.scaleIfNeeded()
	afterFirst := p.GetRunningWorkersNum()
	createdFirst := p.GetWorkerCreateCount()
	if afterFirst < 6 {
		t.Fatalf("running workers after first scale = %d, want the drain target of about 10", afterFirst)
	}

	for i := 0; i < 5; i++ {
		p.scaleIfNeeded()
	}

	if got := p.GetRunningWorkersNum(); got > afterFirst {
		t.Fatalf("running workers grew from %d to %d across repeated scaling decisions on unchanged state",
			afterFirst, got)
	}
	if got := p.GetWorkerCreateCount(); got > createdFirst {
		t.Fatalf("workers created grew from %d to %d across repeated scaling decisions on unchanged state",
			createdFirst, got)
	}
}
