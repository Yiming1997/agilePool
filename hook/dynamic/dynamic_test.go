package dynamic

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

func TestHooksDispatchesAllEvents(t *testing.T) {
	hooks := NewHooks()
	hooks.logger = log.New(io.Discard, "", 0)

	var submitted, enqueued, rejected, started, completed, closed atomic.Int64
	hooks.AddTaskSubmitted(func(context.Context) { submitted.Add(1) })
	hooks.AddTaskEnqueued(func(context.Context) { enqueued.Add(1) })
	hooks.AddTaskRejected(func(context.Context) { rejected.Add(1) })
	hooks.AddTaskStarted(func(context.Context) { started.Add(1) })
	hooks.AddTaskCompleted(func(context.Context, any) { completed.Add(1) })
	hooks.AddPoolClosed(func(*agilepool.Pool) { closed.Add(1) })

	pool := agilepool.NewPool(agilepool.NewConfig())
	defer pool.Close()

	hooks.DispatchTaskSubmitted(context.Background())
	hooks.DispatchTaskEnqueued(context.Background())
	hooks.DispatchTaskRejected(context.Background())
	hooks.DispatchTaskStarted(context.Background())
	hooks.DispatchTaskCompleted(context.Background(), nil)
	hooks.DispatchPoolClosed(pool)

	if got := submitted.Load(); got != 1 {
		t.Fatalf("submitted callbacks = %d, want 1", got)
	}
	if got := enqueued.Load(); got != 1 {
		t.Fatalf("enqueued callbacks = %d, want 1", got)
	}
	if got := rejected.Load(); got != 1 {
		t.Fatalf("rejected callbacks = %d, want 1", got)
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("started callbacks = %d, want 1", got)
	}
	if got := completed.Load(); got != 1 {
		t.Fatalf("completed callbacks = %d, want 1", got)
	}
	if got := closed.Load(); got != 1 {
		t.Fatalf("closed callbacks = %d, want 1", got)
	}
}

func TestHooksRegistrationDuringDispatchAppliesToNextEvent(t *testing.T) {
	hooks := NewHooks()
	hooks.logger = log.New(io.Discard, "", 0)

	var initialCalls, addedCalls atomic.Int64
	hooks.AddTaskStarted(func(context.Context) {
		initialCalls.Add(1)
		hooks.AddTaskStarted(func(context.Context) {
			addedCalls.Add(1)
		})
	})

	hooks.DispatchTaskStarted(context.Background())
	if got := initialCalls.Load(); got != 1 {
		t.Fatalf("initial callbacks after first dispatch = %d, want 1", got)
	}
	if got := addedCalls.Load(); got != 0 {
		t.Fatalf("new callbacks after first dispatch = %d, want 0", got)
	}

	hooks.DispatchTaskStarted(context.Background())
	if got := initialCalls.Load(); got != 2 {
		t.Fatalf("initial callbacks after second dispatch = %d, want 2", got)
	}
	if got := addedCalls.Load(); got != 1 {
		t.Fatalf("new callbacks after second dispatch = %d, want 1", got)
	}
}

func TestHooksCallbackPanicDoesNotStopRemainingCallbacks(t *testing.T) {
	hooks := NewHooks()
	hooks.logger = log.New(io.Discard, "", 0)

	var calls atomic.Int64
	hooks.AddTaskCompleted(func(context.Context, any) { panic("boom") })
	hooks.AddTaskCompleted(func(context.Context, any) { calls.Add(1) })
	hooks.AddTaskRejected(func(context.Context) { panic("boom") })
	hooks.AddTaskRejected(func(context.Context) { calls.Add(1) })

	hooks.DispatchTaskCompleted(context.Background(), nil)
	hooks.DispatchTaskRejected(context.Background())
	if got := calls.Load(); got != 2 {
		t.Fatalf("remaining callbacks = %d, want 2", got)
	}
}

func TestRejectionRegistrationDuringDispatchAppliesToNextEvent(t *testing.T) {
	hooks := NewHooks()
	var initial, added atomic.Int64
	hooks.AddTaskRejected(func(context.Context) {
		initial.Add(1)
		hooks.AddTaskRejected(func(context.Context) { added.Add(1) })
	})
	hooks.DispatchTaskRejected(context.Background())
	if initial.Load() != 1 || added.Load() != 0 {
		t.Fatalf("first rejection callbacks = %d/%d, want 1/0", initial.Load(), added.Load())
	}
	hooks.DispatchTaskRejected(context.Background())
	if initial.Load() != 2 || added.Load() != 1 {
		t.Fatalf("second rejection callbacks = %d/%d, want 2/1", initial.Load(), added.Load())
	}
}

func TestRejectionContextThroughPool(t *testing.T) {
	hooks := NewHooks()
	pool := agilepool.NewPool(nil)
	if err := pool.SetHook(hooks); err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "trace"))
	cancel()
	var calls atomic.Int64
	hooks.AddTaskRejected(func(got context.Context) {
		if got != ctx || got.Value(contextKey{}) != "trace" {
			t.Error("rejection event lost task context")
		}
		calls.Add(1)
	})
	pool.SubmitCtx(ctx, agilepool.NewTask(func() error { t.Error("rejected task executed"); return nil }))
	if calls.Load() != 1 {
		t.Fatalf("rejection events = %d, want 1", calls.Load())
	}
}

func TestHooksCanRegisterWhilePoolRuns(t *testing.T) {
	hooks := NewHooks()
	hooks.logger = log.New(io.Discard, "", 0)

	pool := agilepool.NewPool(agilepool.NewConfig())
	pool.SetLogger(log.New(io.Discard, "", 0))
	if err := pool.SetHook(hooks); err != nil {
		t.Fatalf("SetHook() error = %v", err)
	}
	defer pool.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	pool.Submit(agilepool.TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}

	var completed atomic.Int64
	hooks.AddTaskCompleted(func(context.Context, any) { completed.Add(1) })
	close(release)
	pool.Wait()

	if got := completed.Load(); got != 1 {
		t.Fatalf("completed callbacks = %d, want 1", got)
	}
}

func TestHooksConcurrentRegistrationAndDispatch(t *testing.T) {
	hooks := NewHooks()
	hooks.logger = log.New(io.Discard, "", 0)

	const (
		registrars  = 4
		dispatchers = 4
		iterations  = 100
	)

	var registered, rejectionCalls atomic.Int64
	var callbacks sync.WaitGroup
	for i := 0; i < registrars; i++ {
		callbacks.Add(1)
		go func() {
			defer callbacks.Done()
			for j := 0; j < iterations; j++ {
				hooks.AddTaskEnqueued(func(context.Context) {})
				hooks.AddTaskRejected(func(context.Context) { rejectionCalls.Add(1) })
				registered.Add(1)
			}
		}()
	}
	for i := 0; i < dispatchers; i++ {
		callbacks.Add(1)
		go func() {
			defer callbacks.Done()
			for j := 0; j < iterations; j++ {
				hooks.DispatchTaskEnqueued(context.Background())
				hooks.DispatchTaskRejected(context.Background())
			}
		}()
	}
	callbacks.Wait()

	if got, want := registered.Load(), int64(registrars*iterations); got != want {
		t.Fatalf("registered callbacks = %d, want %d", got, want)
	}
	before := rejectionCalls.Load()
	hooks.DispatchTaskRejected(context.Background())
	if got, want := rejectionCalls.Load()-before, int64(registrars*iterations); got != want {
		t.Fatalf("callbacks retained after concurrent registration = %d, want %d", got, want)
	}
}
