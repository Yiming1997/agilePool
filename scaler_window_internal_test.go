package agilepool

import (
	"sync"
	"testing"
	"time"
)

// TestInt64WindowMean checks the exact-value window used for the running
// worker count: it averages the samples it holds and evicts the oldest once
// it is full.
func TestInt64WindowMean(t *testing.T) {
	w := newInt64Window(3)
	if got := w.mean(); got != 0 {
		t.Fatalf("empty window mean = %v, want 0", got)
	}

	w.add(10)
	w.add(20)
	if got := w.mean(); got != 15 {
		t.Fatalf("mean = %v, want 15", got)
	}

	w.add(30)
	if got := w.mean(); got != 20 {
		t.Fatalf("mean = %v, want 20", got)
	}

	// The window is full: the next sample evicts the oldest one.
	w.add(60)
	if got := w.mean(); got != (20 + 30 + 60) / 3.0 {
		t.Fatalf("mean after eviction = %v, want %v", got, (20+30+60)/3.0)
	}
}

// TestInt64WindowZeroSize makes sure a non-positive size still yields a
// usable single-sample window instead of panicking.
func TestInt64WindowZeroSize(t *testing.T) {
	w := newInt64Window(0)
	w.add(7)
	if got := w.mean(); got != 7 {
		t.Fatalf("mean = %v, want 7", got)
	}
	w.add(9)
	if got := w.mean(); got != 9 {
		t.Fatalf("mean = %v, want 9", got)
	}
}

// TestScalerTargetStableWithinStaleWindow is the regression test for the
// self-referential target.
//
// The rate target used to be submitMed * running / consumeMed, with `running`
// read live. A scale-up therefore changed the pool's own input: the next
// scaler tick divided the same stale rates by the larger worker count and
// raised the target again, multiplying the pool size every tick until the
// backlog brake stopped it.
//
// With the running count taken from the statistics window instead, the
// target is a function of one consistent snapshot (arrivals, departures,
// concurrency) and cannot change while that snapshot is unchanged.
func TestScalerTargetStableWithinStaleWindow(t *testing.T) {
	// Freeze the background sampler and scaler so the seeded window is the
	// only state the scaler ever sees.
	p := NewPool(NewConfig(
		WithScalerPeriod(time.Hour),
		WithStatsSamplePeriod(time.Hour),
	))
	defer p.Close()

	// Seed one window: arrivals (2000/window -> bucket midpoint 1250.5) are
	// far above the observed completion rate (200/window -> 300.5), and the
	// windowed average concurrency is 20. The pool looks behind and wants to
	// scale up.
	for i := 0; i < 10; i++ {
		p.submitWindow.add(2000)
		p.consumeWin.add(200)
		p.runningWindow.add(20)
	}

	release := make(chan struct{})
	var wg sync.WaitGroup
	const n = 200
	wg.Add(n)
	for i := 0; i < n; i++ {
		p.Submit(TaskFunc(func() error {
			defer wg.Done()
			<-release
			return nil
		}))
	}

	p.scaleIfNeeded()
	afterFirst := p.GetRunningWorkersNum()
	createdFirst := p.GetWorkerCreateCount()
	if afterFirst <= 1 {
		t.Fatalf("running workers after first scale = %d, want the pool to have scaled up", afterFirst)
	}

	// Further scaling decisions inside the same (now stale) window must not
	// raise the target again.
	p.scaleIfNeeded()
	p.scaleIfNeeded()

	if got := p.GetRunningWorkersNum(); got > afterFirst {
		t.Fatalf("running workers grew from %d to %d inside one statistics window", afterFirst, got)
	}
	if got := p.GetWorkerCreateCount(); got > createdFirst {
		t.Fatalf("workers created grew from %d to %d inside one statistics window", createdFirst, got)
	}

	close(release)
	wg.Wait()
	p.Wait()
}

// TestScalerUsesWindowedRunningNotLiveCount pins the behaviour difference
// directly.
//
// The seeded window says the pool averaged 20 concurrent workers over the
// last window, so the rate target is submitMed(2000) * 20 / consumeMed(200)
// = 200. The live worker count is 1 at this moment, and using it instead
// would yield a target of only 10.
//
// The test asserts the pool follows the window snapshot, not the live count.
func TestScalerUsesWindowedRunningNotLiveCount(t *testing.T) {
	p := NewPool(NewConfig(
		WithScalerPeriod(time.Hour),
		WithStatsSamplePeriod(time.Hour),
	))
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.submitWindow.add(2000)
		p.consumeWin.add(200)
		p.runningWindow.add(20)
	}

	release := make(chan struct{})
	var wg sync.WaitGroup
	const n = 200
	wg.Add(n)
	for i := 0; i < n; i++ {
		p.Submit(TaskFunc(func() error {
			defer wg.Done()
			<-release
			return nil
		}))
	}

	p.scaleIfNeeded()
	got := p.GetRunningWorkersNum()

	// The window snapshot (average concurrency 20) puts the target at 200; the
	// live count (1 worker) would put it at 10.
	if got < 50 {
		t.Fatalf("running workers = %d, want the target derived from the windowed average (200), not from the live count", got)
	}
	if got > 260 {
		t.Fatalf("running workers = %d, want the target to stay near the windowed value of 200", got)
	}

	close(release)
	wg.Wait()
	p.Wait()
}
