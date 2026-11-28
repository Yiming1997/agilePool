package agilepool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// latch signals completion of one task; the channel is created up front so the
// worker and the test can never wait on different channels.
type bpLatch struct {
	ch   chan struct{}
	once sync.Once
}

func newBPLatch() *bpLatch { return &bpLatch{ch: make(chan struct{})} }

func (l *bpLatch) done() { l.once.Do(func() { close(l.ch) }) }
func (l *bpLatch) wait() { <-l.ch }

// TestStrictBackpressureRunsEveryTask checks that strict backpressure still
// delivers every accepted task.
func TestStrictBackpressureRunsEveryTask(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(8),
		WithTaskQueueSize(8),
	))
	defer p.Close()

	const n = 2000
	var ran int64
	for i := 0; i < n; i++ {
		p.Submit(TaskFunc(func() error {
			atomic.AddInt64(&ran, 1)
			return nil
		}))
	}
	p.Wait()

	if got := atomic.LoadInt64(&ran); got != n {
		t.Fatalf("ran %d tasks, want %d", got, n)
	}
}

// TestStrictBackpressureBlocksUntilRoom checks the core behaviour: once the
// handoff queue is full the submitter waits for a worker instead of spilling
// into the overflow buffer, and it is released when room appears.
func TestStrictBackpressureBlocksUntilRoom(t *testing.T) {
	// One worker, a one-slot queue: the first task occupies the worker, the
	// second fills the queue and the third has nowhere to go.
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1),
		WithTaskQueueShards(1),
	))
	defer p.Close()

	release := newBPLatch()
	started := newBPLatch()
	p.Submit(TaskFunc(func() error {
		started.done()
		release.wait()
		return nil
	}))
	started.wait()

	// Fill the single queue slot.
	p.Submit(TaskFunc(func() error { return nil }))

	// This submission must block: give it a moment and confirm it has not
	// returned, then release the worker and confirm it does.
	done := make(chan struct{})
	go func() {
		p.Submit(TaskFunc(func() error { return nil }))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("submit returned while the queue was full")
	case <-time.After(50 * time.Millisecond):
	}

	release.done()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("submit did not return after room appeared")
	}
	p.Wait()
}

// TestStrictBackpressureUnblocksOnClose checks that a submitter blocked on a
// full queue is released when its context expires, and that Close still works.
func TestStrictBackpressureUnblocksOnClose(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1),
		WithTaskQueueShards(1),
	))

	release := newBPLatch()
	started := newBPLatch()
	p.Submit(TaskFunc(func() error {
		started.done()
		release.wait()
		return nil
	}))
	started.wait()
	p.Submit(TaskFunc(func() error { return nil })) // fills the single slot

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		p.SubmitCtx(ctx, TaskFunc(func() error { return nil }))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled submit did not return")
	}

	release.done()
	p.Close()
}
// TestTrySubmitRejectsWhenQueueIsFull checks that TrySubmit reports saturation
// instead of waiting, even on the blocking submit path.
func TestTrySubmitRejectsWhenQueueIsFull(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1),
		WithTaskQueueShards(1),
	))
	defer p.Close()

	release := newBPLatch()
	started := newBPLatch()
	if !p.TrySubmit(TaskFunc(func() error {
		started.done()
		release.wait()
		return nil
	})) {
		t.Fatal("first TrySubmit was rejected")
	}
	started.wait()

	// Fill the single queue slot, then the next TrySubmit has nowhere to go.
	if !p.TrySubmit(TaskFunc(func() error { return nil })) {
		t.Fatal("second TrySubmit was rejected")
	}
	if p.TrySubmit(TaskFunc(func() error { return nil })) {
		t.Fatal("TrySubmit accepted a task with a full queue")
	}

	release.done()
	p.Wait()
}

// TestBlockedSubmitterUnblockedByClose checks that a submitter waiting for
// queue room is released by Close rather than being left behind.
func TestBlockedSubmitterUnblockedByClose(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1),
		WithTaskQueueShards(1),
	))

	release := newBPLatch()
	started := newBPLatch()
	p.Submit(TaskFunc(func() error {
		started.done()
		release.wait()
		return nil
	}))
	started.wait()
	p.Submit(TaskFunc(func() error { return nil })) // fills the one slot

	blocked := make(chan struct{})
	go func() {
		p.Submit(TaskFunc(func() error { return nil }))
		close(blocked)
	}()

	// Give the submitter a moment to reach the wait, then close.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-blocked:
		t.Fatal("submit returned before Close while the queue was full")
	default:
	}
	p.Close()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release the blocked submitter")
	}
	release.done()
}