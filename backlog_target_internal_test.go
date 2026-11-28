package agilepool

import (
	"testing"
	"time"
)

// window is the statistics window these tests assume: 100 ms.
const testWindow = 0.1

// TestBacklogWeightedTargetUsesWaitingTasksOnly pins the fix for the double
// count: the outstanding backlog includes the tasks already executing, and those
// are already reflected in the worker count. Counting them again made the target
// grow with tasks that are not waiting for a worker at all.
func TestBacklogWeightedTargetUsesWaitingTasksOnly(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithTaskQueueSize(2000)))
	defer p.Close()

	// 200k tasks/s of 50 ms work: Little's law needs 10k workers. The statistics
	// window is 100 ms, so a window holds 20k submissions and a worker finishes
	// 2 tasks per window.
	const (
		submitMed  = 20000.0
		consumeMed = 20000.0
		runningAvg = 10000.0
	)
	// Waiting work only.
	withWaiting := p.backlogWeightedTarget(submitMed, consumeMed, runningAvg, 2000, 0, 0)
	// The old formula used the whole backlog (10k running + 2k waiting).
	oldFormula := int64((submitMed + 0.3*float64(12000)) * runningAvg / consumeMed)

	if withWaiting >= oldFormula {
		t.Fatalf("waiting-only target %d should be below the whole-backlog target %d",
			withWaiting, oldFormula)
	}
	// The waiting-only target stays within a few percent of the steady need.
	if withWaiting > 11000 {
		t.Fatalf("target = %d, want close to the 10k steady need", withWaiting)
	}
}

// TestBacklogWeightedTargetMeasuresPressureFromTheActiveStagingArea checks the
// mode split: with the default backpressure the overflow buffer is never
// written, so pressure has to come from the handoff queue; with the buffer
// enabled it comes from the buffer, as before.
func TestBacklogWeightedTargetMeasuresPressureFromTheActiveStagingArea(t *testing.T) {
	strict := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithTaskQueueSize(2000)))
	defer strict.Close()
	buffered := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithTaskQueueSize(2000), WithOverflowBuffer()))
	defer buffered.Close()

	const (
		submitMed  = 1000.0
		consumeMed = 1000.0
		runningAvg = 100.0
		waiting    = 1000 // one window of work at consumeMed = 1000
	)
	// Under strict backpressure the buffer is never written, so the queue depth
	// must drive the pressure: a deep queue lifts the decay even though bufDepth
	// is 0 in both cases.
	strictShallow := strict.backlogWeightedTarget(submitMed, consumeMed, runningAvg, waiting, 0, 0)
	strictDeep := strict.backlogWeightedTarget(submitMed, consumeMed, runningAvg, 50*waiting, 0, 0)
	if strictDeep <= strictShallow {
		t.Fatalf("strict: deep queue target %d should exceed shallow %d", strictDeep, strictShallow)
	}

	// With the overflow buffer the pressure comes from the buffer instead: the
	// same waiting work is discounted when the buffer is empty and counted in
	// full when the buffer is deep.
	bufEmpty := buffered.backlogWeightedTarget(submitMed, consumeMed, runningAvg, waiting, 0, 0)
	bufDeep := buffered.backlogWeightedTarget(submitMed, consumeMed, runningAvg, waiting, 50*int64(waiting), 0)
	if bufDeep <= bufEmpty {
		t.Fatalf("buffered: buf pressure %d should exceed buffer-empty %d", bufDeep, bufEmpty)
	}
	// The waiting term is always the queue plus the buffer; only the pressure is
	// mode specific, so with the buffer empty the answer stays at the floor even
	// for a deep queue.
	if bufEmpty != 130 {
		t.Fatalf("buffered with an empty buffer = %d, want the floor 130", bufEmpty)
	}
}

// TestBacklogWeightedTargetWithNoPressure checks the floor: with nothing
// waiting the decay is the configured factor and only the submit rate counts.
func TestBacklogWeightedTargetWithNoPressure(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithBacklogDecayFactor(0.3)))
	defer p.Close()

	got := p.backlogWeightedTarget(2000, 2000, 100, 0, 0, 0)
	// (2000 + 0) * 100 / 2000 = 100
	if got != 100 {
		t.Fatalf("target = %d, want 100", got)
	}
}

// TestBacklogWeightedTargetIgnoresMissingMeasurements checks the guards.
func TestBacklogWeightedTargetIgnoresMissingMeasurements(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(30000)))
	defer p.Close()

	for _, c := range []struct {
		name                  string
		submit, consume, pool float64
	}{
		{"no consume", 100, 0, 10},
		{"no workers", 100, 100, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := p.backlogWeightedTarget(c.submit, c.consume, c.pool, 100, 0, 0); got != 0 {
				t.Fatalf("target = %d, want 0", got)
			}
		})
	}
}

// TestBacklogDecayRisesWithPressure pins the interpolation between the configured
// factor and 1.0.
func TestBacklogDecayRisesWithPressure(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithBacklogDecayFactor(0.3)))
	defer p.Close()
	_ = testWindow

	// 1 window of waiting work -> slight lift; 10 windows -> saturates at 1.0.
	one := p.backlogWeightedTarget(0, 1000, 100, 1000, 0, 0)
	ten := p.backlogWeightedTarget(0, 1000, 100, 10000, 0, 0)
	if ten <= one {
		t.Fatalf("10 windows of work (%d) should exceed 1 window (%d)", ten, one)
	}
	// At saturation the decay is 1.0: target = waiting * workersPerWindow.
	if want := int64(10000 * 100 / 1000); ten != want {
		t.Fatalf("saturated target = %d, want %d", ten, want)
	}
}

// TestBacklogWeightedTargetStateless checks it does not depend on pool state
// beyond the configuration, so the same inputs always produce the same answer.
func TestBacklogWeightedTargetStateless(t *testing.T) {
	a := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithBacklogDecayFactor(0.3)))
	defer a.Close()
	b := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithBacklogDecayFactor(0.3)))
	defer b.Close()

	x := a.backlogWeightedTarget(500, 400, 50, 300, 0, 0)
	y := b.backlogWeightedTarget(500, 400, 50, 300, 0, 0)
	if x != y {
		t.Fatalf("targets differ between pools: %d vs %d", x, y)
	}
	_ = time.Now
}

// TestBacklogPressureSaturatesOnBlockedSubmitters covers the short-queue case:
// the staging area is capped by its capacity, so a small queue can never look
// "several windows deep" however far behind the pool is. A waiting submitter is
// the evidence that does not depend on the queue size.
func TestBacklogPressureSaturatesOnBlockedSubmitters(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(30000), WithTaskQueueSize(100), WithBacklogDecayFactor(0.3)))
	defer p.Close()

	// 100 waiting tasks against 2000 completions per window: 0.05 windows of
	// work, i.e. the floor decay of 0.3.
	const (
		submitMed  = 2000.0
		consumeMed = 2000.0
		runningAvg = 100.0
	)
	noBlocked := p.backlogWeightedTarget(submitMed, consumeMed, runningAvg, 100, 0, 0)
	// One blocked submitter proves the pool cannot accept work at all, so the
	// pressure -- and with it the decay -- goes to its ceiling.
	oneBlocked := p.backlogWeightedTarget(submitMed, consumeMed, runningAvg, 100, 0, 1)

	if oneBlocked <= noBlocked {
		t.Fatalf("blocked submitter did not raise the target: %d vs %d", oneBlocked, noBlocked)
	}
	// Floor: (2000 + 100*0.3) * 100/2000 = 101.
	if noBlocked != 101 {
		t.Fatalf("target with no blocked submitter = %d, want 101", noBlocked)
	}
	// Ceiling: (2000 + 100*1.0) * 100/2000 = 105.
	if oneBlocked != 105 {
		t.Fatalf("target with a blocked submitter = %d, want 105", oneBlocked)
	}
	// The blocked tasks themselves also count as waiting work.
	manyBlocked := p.backlogWeightedTarget(submitMed, consumeMed, runningAvg, 100, 0, 400)
	// (2000 + 500*1.0) * 100/2000 = 125.
	if manyBlocked != 125 {
		t.Fatalf("target with 400 blocked submitters = %d, want 125", manyBlocked)
	}
}
