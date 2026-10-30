package agilepool_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// idlePoolWithBurst returns a pool that has idled long enough for the scaling
// statistics window to be filled with zeros, then submits n tasks that block
// until the returned cleanup function is called.
func idlePoolWithBurst(t *testing.T, capacity int64, n int, opts ...agilepool.ConfigOption) (p *agilepool.Pool, started *int64, cleanup func()) {
	t.Helper()
	cfgOpts := append([]agilepool.ConfigOption{agilepool.WithWorkerNumCapacity(capacity)}, opts...)
	p = agilepool.NewPool(agilepool.NewConfig(cfgOpts...))

	// defaultStatsWindowSize (10) * defaultStatsSamplePeriod (100ms) = 1s.
	time.Sleep(1200 * time.Millisecond)

	release := make(chan struct{})
	started = new(int64)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		p.Submit(agilepool.TaskFunc(func() error {
			defer wg.Done()
			atomic.AddInt64(started, 1)
			<-release
			return nil
		}))
	}
	cleanup = func() {
		close(release)
		wg.Wait()
		p.Close()
	}
	return p, started, cleanup
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestColdPoolBurstScalesToCapacity covers the reported stall: a burst of
// blocking tasks submitted after an idle period used to run on a single
// cold-start worker because both scaling medians were zero.
func TestColdPoolBurstScalesToCapacity(t *testing.T) {
	const capacity = 8
	p, started, cleanup := idlePoolWithBurst(t, capacity, 16)
	defer cleanup()

	if !waitUntil(2*time.Second, func() bool { return atomic.LoadInt64(started) == capacity }) {
		t.Fatalf("workers started = %d, want %d (queued=%d, running=%d)",
			atomic.LoadInt64(started), capacity, p.GetTaskQueueLen(), p.GetRunningWorkersNum())
	}
	if got := p.GetRunningWorkersNum(); got > capacity {
		t.Fatalf("running workers = %d, want <= capacity (%d)", got, capacity)
	}
}

// TestColdPoolNoBacklogDoesNotScale ensures the fallback never spawns workers
// when nothing is waiting: one long task must keep the pool at one worker.
func TestColdPoolNoBacklogDoesNotScale(t *testing.T) {
	const capacity = 8
	p, started, cleanup := idlePoolWithBurst(t, capacity, 1)
	defer cleanup()

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt64(started); got != 1 {
		t.Fatalf("workers started = %d, want 1", got)
	}
	if got := p.GetRunningWorkersNum(); got != 1 {
		t.Fatalf("running workers = %d, want 1", got)
	}
}

// TestColdPoolRespectsCapacity checks the fallback stops at the configured
// concurrency limit.
func TestColdPoolRespectsCapacity(t *testing.T) {
	const capacity = 4
	_, started, cleanup := idlePoolWithBurst(t, capacity, 16)
	defer cleanup()

	if !waitUntil(2*time.Second, func() bool { return atomic.LoadInt64(started) == capacity }) {
		t.Fatalf("workers started = %d, want %d", atomic.LoadInt64(started), capacity)
	}
	// Give the scaler room to overshoot, then verify it did not.
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt64(started); got != capacity {
		t.Fatalf("workers started = %d, want exactly %d (capacity)", got, capacity)
	}
}

// TestColdPoolSingleQueuedTaskGrows checks a minimal backlog: one executing
// task plus one queued task should be enough to grow to two workers.
func TestColdPoolSingleQueuedTaskGrows(t *testing.T) {
	p, started, cleanup := idlePoolWithBurst(t, 8, 2)
	defer cleanup()

	if !waitUntil(2*time.Second, func() bool { return atomic.LoadInt64(started) == 2 }) {
		t.Fatalf("workers started = %d, want 2 (queued=%d)", atomic.LoadInt64(started), p.GetTaskQueueLen())
	}
}

// TestZeroConsumeScaleIntervalIsHonored verifies the fallback obeys the
// configured interval: with a very long interval only the first growth step may
// happen (1 -> 2 workers), even though plenty of work is queued.
func TestZeroConsumeScaleIntervalIsHonored(t *testing.T) {
	_, started, cleanup := idlePoolWithBurst(t, 8, 16, agilepool.WithZeroConsumeScaleInterval(time.Hour))
	defer cleanup()

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt64(started); got != 2 {
		t.Fatalf("workers started = %d, want 2 (one growth step under a 1h interval)", got)
	}
}

// TestDefaultCapacityBurstIsBounded guards the physical upper bound: with the
// default (unbounded) capacity, a burst of blocking tasks must never create
// more workers than there are pending tasks. Before the bound was added, the
// backlog target fed off the worker count and multiplied it every scaler tick,
// once the consume median became non-zero.
func TestDefaultCapacityBurstIsBounded(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig()) // capacity = math.MaxInt64
	defer p.Close()

	time.Sleep(1200 * time.Millisecond)

	const n = 1000
	release := make(chan struct{})
	defer func() {
		close(release)
		p.Wait()
	}()
	for i := 0; i < n; i++ {
		p.Submit(agilepool.TaskFunc(func() error {
			<-release
			return nil
		}))
	}
	time.Sleep(time.Second)

	running := p.GetRunningWorkersNum()
	created := p.GetWorkerCreateCount()
	queued := p.GetTaskQueueLen()
	if running > n {
		t.Fatalf("running workers = %d, want <= %d pending tasks", running, n)
	}
	// created counts sync.Pool.New allocations; a few extra spawns may come
	// from the exit compensation, but it must stay in the same order.
	if created > n+64 {
		t.Fatalf("workers created = %d, want <= %d for %d tasks (queued=%d)", created, n+64, n, queued)
	}
}
