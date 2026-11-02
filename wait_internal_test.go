package agilepool

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestWaitBlocksWhileFirstSubmitterPreempted is a regression test for Wait
// deciding from the presence of drainCh rather than from pendingTasks. When
// the drain channel was published only by the submitter that performed the
// 0→1 increment, a later submitter whose Submit had already returned could
// call Wait before that channel existed, and Wait returned with the accepted
// task still running.
//
// The preempted first submitter is simulated by incrementing pendingTasks
// directly, which reproduces the window deterministically.
func TestWaitBlocksWhileFirstSubmitterPreempted(t *testing.T) {
	p := NewPool(NewConfig())
	defer p.Close()

	// First submitter: 0→1, then preempted before it could publish anything.
	atomic.AddInt64(&p.pendingTasks, 1)

	release := make(chan struct{})
	p.Submit(TaskFunc(func() error {
		<-release
		return nil
	}))

	returned := make(chan struct{})
	go func() {
		p.Wait()
		close(returned)
	}()

	select {
	case <-returned:
		close(release)
		t.Fatal("Wait returned while an accepted task was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	p.done() // The first submitter resumes and its task finishes.

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the pool drained")
	}
}
