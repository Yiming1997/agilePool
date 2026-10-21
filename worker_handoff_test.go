package agilepool_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
)

// TestSubmitAfterWorkerParks pauses a submitter before enqueueing, lets the
// last worker mark itself parked, then enqueues the task.
// The scaler is deliberately slow so it cannot hide a missed wake-up.
func TestSubmitAfterWorkerParks(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(1),
		agilepool.WithScalerPeriod(time.Hour),
	))
	defer p.Close()

	var submissions int
	beforeEnqueue := make(chan struct{})
	resumeSubmit := make(chan struct{})
	h := hook.NewHooks()
	h.AddTaskSubmitted(func(context.Context) {
		submissions++
		if submissions == 2 {
			close(beforeEnqueue)
			<-resumeSubmit
		}
	})
	p.SetHook(h)

	firstStarted := make(chan struct{})
	finishFirst := make(chan struct{})
	p.Submit(agilepool.TaskFunc(func() error {
		close(firstStarted)
		<-finishFirst
		return nil
	}))
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("the first task did not start")
	}

	secondDone := make(chan struct{})
	accepted := make(chan bool, 1)
	go func() {
		accepted <- p.TrySubmit(agilepool.TaskFunc(func() error {
			close(secondDone)
			return nil
		}))
	}()
	select {
	case <-beforeEnqueue:
	case <-time.After(2 * time.Second):
		close(finishFirst)
		t.Fatal("the second submitter did not reach the hook")
	}
	close(finishFirst)

	deadline := time.Now().Add(2 * time.Second)
	for p.GetRunningWorkersNum() != 0 {
		if time.Now().After(deadline) {
			close(resumeSubmit)
			t.Fatal("the first worker did not park")
		}
		runtime.Gosched()
	}
	// Close stops the scaler before this in-flight submission reaches the queue.
	p.Close()
	close(resumeSubmit)
	select {
	case ok := <-accepted:
		if !ok {
			t.Fatal("the second task was rejected")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the second submission did not return")
	}

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("task remained queued after the last worker parked")
	}
	p.Wait()
}
