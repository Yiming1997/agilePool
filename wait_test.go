package agilepool_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// TestAgilePoolWaitConcurrentPostCloseSubmit is a regression test for the
// WaitGroup misuse that occurred when a submission raced with Wait after
// Close: submit incremented the WaitGroup before checking closed, so an Add
// could coincide with Wait observing a zero counter and panic with
// "sync: WaitGroup is reused before previous Wait has returned".
//
// The race window is narrow, so the scenario is repeated; the test fails (with
// the runtime panic) as soon as one iteration loses the race.
func TestAgilePoolWaitConcurrentPostCloseSubmit(t *testing.T) {
	const (
		iterations = 100
		producers  = 4
		submits    = 500
	)

	for i := 0; i < iterations; i++ {
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithWorkerNumCapacity(producers),
			agilepool.WithTaskQueueSize(16),
		))

		// Keep one task in flight so Wait has a reason to block while the
		// producers submit into the already-closed pool.
		release := make(chan struct{})
		pool.Submit(agilepool.TaskFunc(func() error {
			<-release
			return nil
		}))

		waited := make(chan struct{})
		go func() {
			pool.Wait()
			close(waited)
		}()

		pool.Close()

		var wg sync.WaitGroup
		start := make(chan struct{})
		for j := 0; j < producers; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for k := 0; k < submits; k++ {
					pool.Submit(agilepool.TaskFunc(func() error { return nil }))
				}
			}()
		}
		close(start)

		// Give the producers time to enter their Submit loops before the
		// in-flight task completes and drives the pending count to zero.
		time.Sleep(100 * time.Microsecond)
		close(release)

		wg.Wait()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Wait did not return", i)
		}
	}
}

// TestAgilePoolWaitReusable verifies Wait can be called once per batch on a
// long-lived pool: the completion signal must be re-created after every drain.
func TestAgilePoolWaitReusable(t *testing.T) {
	pool := agilepool.NewPool(agilepool.NewConfig())
	defer pool.Close()

	for round := 0; round < 3; round++ {
		var count atomic.Int64
		for i := 0; i < 100; i++ {
			pool.Submit(agilepool.TaskFunc(func() error {
				count.Add(1)
				return nil
			}))
		}
		pool.Wait()
		if got := count.Load(); got != 100 {
			t.Fatalf("round %d: executed %d tasks after Wait, want 100", round, got)
		}
	}
}

// TestAgilePoolConcurrentWaiters verifies several goroutines may wait for the
// same drain and are all released when the pool becomes idle.
func TestAgilePoolConcurrentWaiters(t *testing.T) {
	pool := agilepool.NewPool(agilepool.NewConfig())
	defer pool.Close()

	const tasks = 200
	for i := 0; i < tasks; i++ {
		pool.Submit(agilepool.TaskFunc(func() error {
			time.Sleep(10 * time.Microsecond)
			return nil
		}))
	}

	var waiters sync.WaitGroup
	for i := 0; i < 8; i++ {
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			pool.Wait()
		}()
	}

	done := make(chan struct{})
	go func() {
		waiters.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Wait calls did not all return")
	}
}
