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
	hooks.logger = *log.New(io.Discard, "", 0)

	var submitted, enqueued, started, completed, closed atomic.Int64
	hooks.AddTaskSubmitted(func(context.Context) { submitted.Add(1) })
	hooks.AddTaskEnqueued(func(context.Context) { enqueued.Add(1) })
	hooks.AddTaskStarted(func(context.Context) { started.Add(1) })
	hooks.AddTaskCompleted(func(context.Context, any) { completed.Add(1) })
	hooks.AddPoolClosed(func(*agilepool.Pool) { closed.Add(1) })

	pool := agilepool.NewPool(agilepool.NewConfig())
	defer pool.Close()

	hooks.DispatchTaskSubmitted(context.Background())
	hooks.DispatchTaskEnqueued(context.Background())
	hooks.DispatchTaskStarted(context.Background())
	hooks.DispatchTaskCompleted(context.Background(), nil)
	hooks.DispatchPoolClosed(pool)

	if got := submitted.Load(); got != 1 {
		t.Fatalf("submitted callbacks = %d, want 1", got)
	}
	if got := enqueued.Load(); got != 1 {
		t.Fatalf("enqueued callbacks = %d, want 1", got)
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
	hooks.logger = *log.New(io.Discard, "", 0)

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
	hooks.logger = *log.New(io.Discard, "", 0)

	var calls atomic.Int64
	hooks.AddTaskCompleted(func(context.Context, any) { panic("boom") })
	hooks.AddTaskCompleted(func(context.Context, any) { calls.Add(1) })

	hooks.DispatchTaskCompleted(context.Background(), nil)
	if got := calls.Load(); got != 1 {
		t.Fatalf("remaining callbacks = %d, want 1", got)
	}
}

func TestHooksCanRegisterWhilePoolRuns(t *testing.T) {
	hooks := NewHooks()
	hooks.logger = *log.New(io.Discard, "", 0)

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
	hooks.logger = *log.New(io.Discard, "", 0)

	const (
		registrars  = 4
		dispatchers = 4
		iterations  = 100
	)

	var registered atomic.Int64
	var callbacks sync.WaitGroup
	for i := 0; i < registrars; i++ {
		callbacks.Add(1)
		go func() {
			defer callbacks.Done()
			for j := 0; j < iterations; j++ {
				hooks.AddTaskEnqueued(func(context.Context) {})
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
			}
		}()
	}
	callbacks.Wait()

	if got, want := registered.Load(), int64(registrars*iterations); got != want {
		t.Fatalf("registered callbacks = %d, want %d", got, want)
	}
}
