package agilepool

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestScalerDemandPublished checks that the scaler publishes the demand on
// every tick, including ticks that decide that no new worker is needed.
//
// The demand is the decision from before the worker capacity clamps it. An
// external autoscaler consumes it instead of re-deriving the pool's scaling
// formula from raw queue depth.
func TestScalerDemandPublished(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(64),
		WithTaskQueueSize(1024),
	))
	defer p.Close()

	// An idle pool has no work, so the scaler has no reason to want workers.
	// The demand must still be published (as 0) rather than left unset.
	waitFor(t, time.Second, func() bool {
		return p.GetScalerDemand() == 0
	}, "idle pool demand should be 0")

	// Submit enough work for the scaler to want more than one worker.
	const tasks = 200
	for i := 0; i < tasks; i++ {
		p.Submit(TaskFunc(func() error {
			time.Sleep(20 * time.Millisecond)
			return nil
		}))
	}

	var peak int64
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if demand := p.GetScalerDemand(); demand > peak {
			peak = demand
		}
		if peak > 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if peak <= 1 {
		t.Fatalf("expected the scaler to demand more than one worker, peak demand was %d", peak)
	}
	t.Logf("peak scaler demand: %d, capacity: %d", peak, p.GetCapacity())

	p.Wait()
}

// TestScalerDemandNeverExceedsBacklog checks the clamp that still applies to
// the demand: a pool never wants more workers than it has outstanding tasks,
// because a worker runs at most one task at a time.
func TestScalerDemandNeverExceedsBacklog(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1024),
		WithTaskQueueSize(1024),
	))
	defer p.Close()

	// A single long task: the backlog never exceeds one, so neither may the
	// published demand.
	p.Submit(TaskFunc(func() error {
		time.Sleep(300 * time.Millisecond)
		return nil
	}))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		demand := p.GetScalerDemand()
		pending := p.GetPendingTaskCount()
		if pending > 0 && demand > pending {
			t.Fatalf("demand %d exceeds the pending backlog %d", demand, pending)
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.Wait()
}

// TestScalerDemandExceedsTargetWhenCapacityClamps pins the property that
// separates the demand from the target. The capacity clamp hides how far the
// pool wants to grow, so a saturated pool reports the same target whether it
// needs one more worker or fifty. The demand still reports the real need.
func TestScalerDemandExceedsTargetWhenCapacityClamps(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1024),
		WithScalerPeriod(time.Millisecond),
	))
	defer p.Close()

	blocking := make(chan struct{})
	defer close(blocking)
	for i := 0; i < 50; i++ {
		p.Submit(TaskFunc(func() error {
			<-blocking
			return nil
		}))
	}

	waitFor(t, 3*time.Second, func() bool {
		return p.GetScalerDemand() > p.GetScalerTarget()
	}, "expected the demand to exceed the clamped target")

	if got := p.GetScalerTarget(); got != p.GetCapacity() {
		t.Fatalf("expected the target to sit at the capacity ceiling, got %d, capacity %d", got, p.GetCapacity())
	}
	if demand, backlog := p.GetScalerDemand(), p.GetPendingTaskCount(); demand > backlog {
		t.Fatalf("demand %d exceeds the outstanding backlog %d", demand, backlog)
	}
}

// TestMeasureServiceTimeWithoutValidSamples checks that the service time
// measurement reports "not measured" instead of a stale or arbitrary value
// when the sample ring holds no valid entry.
func TestMeasureServiceTimeWithoutValidSamples(t *testing.T) {
	p := NewPool(nil)
	defer p.Close()

	// A pool that never finished a task has no sample at all.
	if got := p.measureServiceTime(); got != 0 {
		t.Fatalf("expected no measurement from a pool without samples, got %v", got)
	}

	// A pool that has seen work but whose ring holds no usable entry must
	// report zero rather than an arbitrary number. The pool has no running
	// task, so the scaler cannot write a sample concurrently.
	atomic.StoreInt64(&p.serviceSampleAt, time.Now().UnixNano())
	for i := range p.serviceDurations {
		atomic.StoreInt64(&p.serviceDurations[i], 0)
	}
	if got := p.measureServiceTime(); got != 0 {
		t.Fatalf("expected zero without a valid sample, got %v", got)
	}
}
