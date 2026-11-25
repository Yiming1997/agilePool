package agilepool

import (
	"testing"
	"time"
)

// TestServiceTimeIsTheTaskDuration checks that the service time is the time
// the task actually took.
//
// The estimate this replaces divided the tick duration by the completions per
// tick and scaled it by the running worker count:
//
//	perWorker = elapsed * running / done
//
// That only equals the task duration while every worker stays busy for the
// whole tick. A worker that finished early and waited for more work inflated
// the result to the tick period, and the backlog-driven target then sized
// the pool from that inflated number: the more spare workers the pool had,
// the longer the measured service time, and the more workers it asked for.
func TestServiceTimeIsTheTaskDuration(t *testing.T) {
	p := NewPool(NewConfig(
		// The scaler never runs during the test, so nothing but the measured
		// task can produce a service time.
		WithScalerPeriod(time.Hour),
		WithStatsSamplePeriod(time.Hour),
		WithWorkerNumCapacity(10),
	))
	defer p.Close()

	const want = 5 * time.Millisecond
	done := make(chan struct{})
	p.Submit(TaskFunc(func() error {
		time.Sleep(want)
		close(done)
		return nil
	}))
	<-done

	// The worker records the sample when the task returns, which is just
	// after the task itself signalled.
	got := waitForServiceTime(p, time.Second)
	if got < want/2 || got > want*2 {
		t.Fatalf("service time = %v, want about %v", got, want)
	}
}

// TestServiceTimeIsZeroWithoutSamples checks that a pool that has not
// finished a task reports no service time at all, so the backlog-driven
// target stays out of the way instead of sizing the pool from a guess.
func TestServiceTimeIsZeroWithoutSamples(t *testing.T) {
	p := NewPool(NewConfig(
		WithScalerPeriod(time.Hour),
		WithStatsSamplePeriod(time.Hour),
		WithWorkerNumCapacity(10),
	))
	defer p.Close()

	if got := p.measureServiceTime(); got != 0 {
		t.Fatalf("service time = %v, want 0 before any task has finished", got)
	}
}

// waitForServiceTime waits for the pool to record a service time sample.
func waitForServiceTime(p *Pool, timeout time.Duration) time.Duration {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := p.measureServiceTime(); got > 0 {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	return 0
}
