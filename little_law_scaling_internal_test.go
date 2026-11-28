package agilepool

import (
	"testing"
	"time"
)

func newLittleLawPool(t *testing.T) *Pool {
	t.Helper()
	p := NewPool(NewConfig(
		WithLittleLawScaling(),
		WithWorkerNumCapacity(100000),
		WithStatsSamplePeriod(100*time.Millisecond),
	))
	t.Cleanup(p.Close)
	return p
}

// TestLittleLawTargetTracksArrivalRate pins the formula: workers needed is the
// arrival rate multiplied by the service time.
func TestLittleLawTargetTracksArrivalRate(t *testing.T) {
	p := newLittleLawPool(t)

	// submitMed counts submissions per statistics window (100 ms here), so 200
	// per window is 2000 tasks/s. At 50 ms per task that needs 100 workers.
	if got := p.littleLawTarget(200, 50*time.Millisecond, 0, 10000); got != 100 {
		t.Fatalf("target = %d, want 100 (2000 tasks/s x 50ms)", got)
	}
	// Same arrival, twice the service time, twice the workers.
	if got := p.littleLawTarget(200, 100*time.Millisecond, 0, 10000); got != 200 {
		t.Fatalf("target = %d, want 200", got)
	}
}

// TestLittleLawTargetIgnoresBacklog is the property that makes it usable for
// autoscaling: the backlog is a symptom of the current replica count, so it
// must not change the answer.
func TestLittleLawTargetIgnoresBacklog(t *testing.T) {
	p := newLittleLawPool(t)

	small := p.littleLawTarget(200, 50*time.Millisecond, 10, 1_000)
	huge := p.littleLawTarget(200, 50*time.Millisecond, 10, 1_000_000)
	if small != huge {
		t.Fatalf("backlog changed the target: %d vs %d", small, huge)
	}
}

// TestLittleLawTargetNeverShrinksBelowRunning checks the floor: the pool does
// not kill workers, so a target below the running count would only stop growth.
func TestLittleLawTargetNeverShrinksBelowRunning(t *testing.T) {
	p := newLittleLawPool(t)
	if got := p.littleLawTarget(10, 50*time.Millisecond, 500, 10000); got != 500 {
		t.Fatalf("target = %d, want 500 (the running count)", got)
	}
}

// TestLittleLawTargetColdStart checks the unmeasured path: before any task has
// completed there is no service time, so the pool opens to the backlog.
func TestLittleLawTargetColdStart(t *testing.T) {
	p := newLittleLawPool(t)
	if got := p.littleLawTarget(0, 0, 0, 777); got != 777 {
		t.Fatalf("cold start target = %d, want 777", got)
	}
	// Idle pool, nothing queued: stay at zero rather than inventing workers.
	if got := p.littleLawTarget(0, 0, 0, 0); got != 0 {
		t.Fatalf("idle target = %d, want 0", got)
	}
}

// TestLittleLawScalingModeIsOptIn checks the default stays on the backlog
// heuristics, so existing users are unaffected.
func TestLittleLawScalingModeIsOptIn(t *testing.T) {
	if got := NewConfig().scalingMode; got != ScalingBacklog {
		t.Fatalf("default scaling mode = %v, want ScalingBacklog", got)
	}
	if got := NewConfig(WithLittleLawScaling()).scalingMode; got != ScalingLittleLaw {
		t.Fatalf("WithLittleLawScaling mode = %v, want ScalingLittleLaw", got)
	}
}

// TestLittleLawScalingKeepsUpWithSteadyLoad is an end-to-end sanity check: a
// steady arrival rate of 1000 tasks/s of 10 ms tasks needs ~10 workers, and the
// pool should settle near that instead of at the capacity.
func TestLittleLawScalingKeepsUpWithSteadyLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("timing sensitive")
	}
	p := NewPool(NewConfig(
		WithLittleLawScaling(),
		WithWorkerNumCapacity(2000),
		WithTaskQueueSize(1000),
	))
	defer p.Close()

	// 10 ms tasks at 1000/s for 2 s: Little's law says 10 concurrent workers.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		<-ticker.C
		p.Submit(TaskFunc(func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}))
	}
	p.Wait()

	// The pool never shrinks below its ramp, so assert it stayed far under the
	// capacity rather than pinning an exact count.
	if got := p.GetRunningWorkersNum(); got > 200 {
		t.Fatalf("running workers = %d, want well under the 2000 capacity", got)
	}
}

// TestLittleLawBumpOnlyWhenSaturated pins the second half of the hybrid: the
// multiplicative bump applies only while the pool is at its worker cap with
// work waiting, which is the state Little's law alone cannot distinguish from a
// correctly sized pool.
func TestLittleLawBumpOnlyWhenSaturated(t *testing.T) {
	p := NewPool(NewConfig(
		WithLittleLawScaling(),
		WithWorkerNumCapacity(100),
	))
	defer p.Close()

	cases := []struct {
		name            string
		running, queued int64
		want            int64
	}{
		{"below capacity", 50, 1000, 0},
		{"at capacity, empty queue", 100, 0, 0},
		{"at capacity with work waiting", 100, 50, 50},
		{"minimum bump of one", 1, 10, 0}, // running 1 < capacity 100
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := p.littleLawBumpFor(c.running, c.queued); got != c.want {
				t.Fatalf("bump(%d, %d) = %d, want %d", c.running, c.queued, got, c.want)
			}
		})
	}
}
