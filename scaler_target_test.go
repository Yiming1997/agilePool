package agilepool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScalerTargetPublished checks that the scaler publishes its decision on
// every tick, including ticks that decide that no new worker is needed.
//
// The published value is what an external autoscaler consumes instead of
// re-deriving the pool's scaling formula from raw queue depth.
func TestScalerTargetPublished(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(64),
		WithTaskQueueSize(1024),
	))
	defer p.Close()

	// An idle pool has no work, so the scaler has no reason to want workers.
	// The target must still be published (as 0) rather than left unset.
	waitFor(t, time.Second, func() bool {
		return p.GetScalerTarget() == 0
	}, "idle pool target should be 0")

	// Submit enough work for the scaler to want more than one worker.
	const tasks = 200
	for i := 0; i < tasks; i++ {
		p.Submit(TaskFunc(func() error {
			time.Sleep(20 * time.Millisecond)
			return nil
		}))
	}

	// The target must rise above zero and never exceed the capacity.
	var peak int64
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		target := p.GetScalerTarget()
		if target > peak {
			peak = target
		}
		if target > p.GetCapacity() {
			t.Fatalf("target %d exceeds capacity %d", target, p.GetCapacity())
		}
		if target > 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if peak <= 1 {
		t.Fatalf("expected the scaler to want more than one worker, peak target was %d", peak)
	}
	t.Logf("peak scaler target: %d, capacity: %d", peak, p.GetCapacity())

	p.Wait()
}

// TestScalerTargetNeverExceedsBacklog checks the clamp the scaler applies
// before publishing: a pool never wants more workers than it has outstanding
// tasks, because a worker runs at most one task at a time.
func TestScalerTargetNeverExceedsBacklog(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1024),
		WithTaskQueueSize(1024),
	))
	defer p.Close()

	// A single long task: the backlog never exceeds one, so neither may the
	// published target.
	p.Submit(TaskFunc(func() error {
		time.Sleep(300 * time.Millisecond)
		return nil
	}))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		target := p.GetScalerTarget()
		pending := p.GetPendingTaskCount()
		if pending > 0 && target > pending {
			t.Fatalf("target %d exceeds the pending backlog %d", target, pending)
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.Wait()
}

// TestScalerTargetReflectsDemand checks that the published target tracks the
// pool's actual demand: a pool given a deep queue of slow tasks wants more
// workers than a pool with one quick task.
func TestScalerTargetReflectsDemand(t *testing.T) {
	measure := func(taskDelay time.Duration, tasks int) int64 {
		p := NewPool(NewConfig(
			WithWorkerNumCapacity(512),
			WithTaskQueueSize(4096),
		))
		defer p.Close()

		for i := 0; i < tasks; i++ {
			p.Submit(TaskFunc(func() error {
				time.Sleep(taskDelay)
				return nil
			}))
		}

		var peak int64
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if target := p.GetScalerTarget(); target > peak {
				peak = target
			}
			time.Sleep(5 * time.Millisecond)
		}
		p.Wait()
		return peak
	}

	slow := measure(50*time.Millisecond, 200)
	fast := measure(time.Millisecond, 200)

	t.Logf("slow tasks peak target: %d, fast tasks peak target: %d", slow, fast)
	if slow <= fast {
		t.Fatalf("expected slow tasks to need more workers, slow=%d fast=%d", slow, fast)
	}
}

// TestScalerTargetConcurrentRead exercises the getter while the scaler is
// writing, so the race detector can confirm the publication is safe.
func TestScalerTargetConcurrentRead(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(128),
		WithTaskQueueSize(1024),
	))
	defer p.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = p.GetScalerTarget()
				}
			}
		}()
	}

	// Writers keep the scaler busy.
	var submitted int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				p.Submit(TaskFunc(func() error {
					time.Sleep(time.Millisecond)
					return nil
				}))
				atomic.AddInt64(&submitted, 1)
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	p.Wait()

	if atomic.LoadInt64(&submitted) == 0 {
		t.Fatal("expected tasks to be submitted")
	}
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}
