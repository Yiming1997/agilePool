package agilepool_test

import (
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/stretchr/testify/assert"
)

func TestAgilePoolSubmitNilTaskDoesNotBlockWait(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	agilePool.Submit(nil)
	agilePool.Wait()
}

func TestAgilePoolSubmitTypedNilTaskDoesNotBlockWait(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	var task agilepool.TaskFunc
	agilePool.Submit(task)
	agilePool.Wait()
}

func TestAgilePoolTrySubmitAcceptsTask(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	var executed int64
	ok := agilePool.TrySubmit(agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))

	assert.True(t, ok)
	agilePool.Wait()
	assert.Equal(t, int64(1), atomic.LoadInt64(&executed))
}

func TestAgilePoolTrySubmitRejectsNilTask(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	ok := agilePool.TrySubmit(nil)

	assert.False(t, ok)
	agilePool.Wait()
}

func TestAgilePoolTrySubmitRejectsTaskAfterClose(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	agilePool.Close()

	ok := agilePool.TrySubmit(agilepool.TaskFunc(func() error {
		return nil
	}))

	assert.False(t, ok)
}

func TestAgilePoolSubmitBeforeNilTaskDoesNotBlockWait(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	agilePool.SubmitBefore(nil, time.Second)
	agilePool.Wait()
}

func TestAgilePoolSubmitCtxCanceledBeforeSubmitDoesNotExecute(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var executed int64
	agilePool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))
	agilePool.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&executed))
}

func TestAgilePoolUpdateTaskNilContextRunsTask(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	agilePool.SetLogger(log.New(io.Discard, "", 0))
	defer agilePool.Close()

	var executed int64
	agilePool.Submit(agilepool.UpdateTask(nil, agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	})))
	agilePool.Wait()

	assert.Equal(t, int64(1), atomic.LoadInt64(&executed))
}

func TestAgilePoolSubmitCtxCanceledWhileQueuedSkipsTask(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(10),
	))
	defer agilePool.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	agilePool.Submit(agilepool.TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	var executed int64
	agilePool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))
	cancel()
	close(release)
	agilePool.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&executed))
}

func TestAgilePoolSubmitCtxRunningTaskObservesCancel(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var canceled int64

	agilePool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
		close(started)
		<-ctx.Done()
		atomic.AddInt64(&canceled, 1)
		return ctx.Err()
	}))
	<-started
	cancel()
	agilePool.Wait()

	assert.Equal(t, int64(1), atomic.LoadInt64(&canceled))
}

func TestAgilePoolSubmitCtxCancelsWhileWaitingForQueueSpace(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(1),
	))
	defer agilePool.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	agilePool.Submit(agilepool.TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	agilePool.Submit(agilepool.TaskFunc(func() error {
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	var executed int64
	go func() {
		agilePool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
			atomic.AddInt64(&executed, 1)
			return nil
		}))
		close(returned)
	}()

	cancel()
	select {
	case <-returned:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("SubmitCtx did not return after context cancellation")
	}

	close(release)
	agilePool.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&executed))
}

func TestAgilePoolSubmitCtxCancelsAtBackpressureLimit(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(1),
	))

	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		agilePool.Close()
	}()

	agilePool.Submit(agilepool.TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	// Fill the handoff channel, then overflow the chunked buffer up to its
	// capacity (maxChunkLen = 100_000, internal constant). Submitting the
	// tasks synchronously while the worker is blocked guarantees the buffer
	// is saturated before the SubmitCtx goroutine starts, so it must hit the
	// backpressure-limit branch.
	agilePool.Submit(agilepool.TaskFunc(func() error { return nil }))
	noop := agilepool.TaskFunc(func() error { return nil })
	for i := 0; i < 100_000; i++ {
		agilePool.Submit(noop)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan struct{})
	go func() {
		agilePool.SubmitCtx(ctx, agilepool.TaskFunc(func() error { return nil }))
		close(returned)
	}()

	cancel()
	select {
	case <-returned:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("SubmitCtx did not return after cancellation at the backpressure limit")
	}

	close(release)
	agilePool.Wait()
}

func TestAgilePoolWorkerCapacityLimit(t *testing.T) {
	taskCount := 10000000
	workerCapacity := int64(10000)
	if testing.Short() {
		taskCount = 20000
		workerCapacity = 100
	}

	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(workerCapacity),
		agilepool.WithIdleContainerType(agilepool.MinHeapType),
	))
	defer agilePool.Close()

	var maxWorkerNum int64
	var submitWG sync.WaitGroup

	for i := 0; i < taskCount; i++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			agilePool.Submit(
				agilepool.TaskFunc(func() error {
					running := agilePool.GetRunningWorkersNum()
					for {
						currentMax := atomic.LoadInt64(&maxWorkerNum)
						if running <= currentMax ||
							atomic.CompareAndSwapInt64(&maxWorkerNum, currentMax, running) {
							break
						}
					}
					time.Sleep(10 * time.Millisecond)
					return nil
				}),
			)
		}()
	}
	submitWG.Wait()
	agilePool.Wait()
	assert.LessOrEqual(t, maxWorkerNum, workerCapacity)
}

func TestAgilePoolWorkerCompletion(t *testing.T) {
	taskCount := 1000000
	if testing.Short() {
		taskCount = 20000
	}

	var sum int64
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(10000),
		agilepool.WithIdleContainerType(agilepool.MinHeapType),
	))
	defer agilePool.Close()

	var submitWG sync.WaitGroup
	for i := 0; i < taskCount; i++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			agilePool.Submit(
				agilepool.TaskFunc(func() error {
					atomic.AddInt64(&sum, int64(1))
					return nil
				}),
			)
		}()
	}

	submitWG.Wait()
	agilePool.Wait()

	assert.Equal(t, int64(taskCount), sum)
}

func TestAgilePoolSubmitBeforeCompletion(t *testing.T) {
	taskCount := 1000000
	if testing.Short() {
		taskCount = 20000
	}

	var sum int64
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(10000),
		agilepool.WithIdleContainerType(agilepool.MinHeapType),
	))
	defer agilePool.Close()

	var submitWG sync.WaitGroup
	for i := 0; i < taskCount; i++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			agilePool.SubmitBefore(
				agilepool.TaskFunc(func() error {
					time.Sleep(10 * time.Millisecond)
					atomic.AddInt64(&sum, int64(1))
					return nil
				}), 10*time.Second,
			)
		}()
	}

	submitWG.Wait()
	agilePool.Wait()
	assert.Equal(t, int64(taskCount), sum)
}

func TestAgilePoolTaskRetryTimes(t *testing.T) {
	var times int64 = 0
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(10),
		agilepool.WithIdleContainerType(agilepool.MinHeapType),
	))

	agilePool.Submit(&agilepool.TaskWithRetry{
		MinBackOff: 1 * time.Second,
		MaxBackOff: 200 * time.Second,
		RetryNum:   3,
		Task: func() error {
			times++
			log.Println("getting err over here")
			return errors.New("err")
		},
	})

	agilePool.Wait()
	assert.Equal(t, times, int64(4))
}

func TestAgilePoolTaskWithRetryBackOffStrategyRetryNum(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig())
	defer agilePool.Close()

	var received []uint
	task := &agilepool.TaskWithRetry{
		MinBackOff: 1 * time.Millisecond,
		MaxBackOff: 5 * time.Millisecond,
		RetryNum:   3,
		BackOffStrategy: func(min, max time.Duration, retryNum uint) time.Duration {
			received = append(received, retryNum)
			return 1 * time.Millisecond
		},
		Task: func() error {
			return errors.New("always fail")
		},
	}

	agilePool.Submit(task)
	agilePool.Wait()

	assert.Equal(t, 3, len(received))
	assert.Equal(t, []uint{1, 2, 3}, received)
}

func TestAgilePoolTaskPanicDoesNotBreakPool(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(10),
	))
	agilePool.SetLogger(log.New(io.Discard, "", 0))
	defer agilePool.Close()

	var executed int64

	agilePool.Submit(agilepool.TaskFunc(func() error {
		panic("boom")
	}))
	agilePool.Submit(agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))

	done := make(chan struct{})
	go func() {
		agilePool.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool.Wait() timed out after a task panic")
	}

	assert.Equal(t, int64(1), atomic.LoadInt64(&executed))

	agilePool.Submit(agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))
	agilePool.Wait()

	assert.Equal(t, int64(2), atomic.LoadInt64(&executed))
}

// TestAgilePoolCustomPanicHandler verifies that a handler registered via
// WithPanicHandler receives the panicking task, the recovered value and the
// captured stack trace, and that the worker keeps servicing subsequent tasks.
func TestAgilePoolCustomPanicHandler(t *testing.T) {
	var (
		calls    int64
		gotTask  atomic.Value
		gotValue atomic.Value
		gotStack atomic.Value
	)

	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(10),
		agilepool.WithPanicHandler(func(task agilepool.Task, recovered any, stack []byte) {
			atomic.AddInt64(&calls, 1)
			gotTask.Store(task)
			gotValue.Store(recovered)
			gotStack.Store(string(stack))
		}),
	))
	defer agilePool.Close()

	panicked := agilepool.TaskFunc(func() error {
		panic("custom-handler-boom")
	})
	agilePool.Submit(panicked)

	var executed int64
	agilePool.Submit(agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))

	done := make(chan struct{})
	go func() {
		agilePool.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool.Wait() timed out after a task panic")
	}

	assert.Equal(t, int64(1), atomic.LoadInt64(&calls))
	assert.Equal(t, "custom-handler-boom", gotValue.Load())
	assert.Equal(t, int64(1), atomic.LoadInt64(&executed))

	// The exact task that panicked must be forwarded, not a wrapper.
	assert.Equal(t,
		reflect.ValueOf(panicked).Pointer(),
		reflect.ValueOf(gotTask.Load()).Pointer(),
	)

	// A non-empty, formatted stack trace must be provided.
	stack, _ := gotStack.Load().(string)
	assert.Contains(t, stack, "worker.go")
}

// TestAgilePoolPanicHandlerPanicIsRecovered guards Pool.handlePanic: a
// PanicHandler that itself panics must be recovered so the worker goroutine
// survives and Wait() still returns.
func TestAgilePoolPanicHandlerPanicIsRecovered(t *testing.T) {
	agilePool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1),
		agilepool.WithTaskQueueSize(10),
		agilepool.WithPanicHandler(func(agilepool.Task, any, []byte) {
			panic("handler boom")
		}),
	))
	agilePool.SetLogger(log.New(io.Discard, "", 0))
	defer agilePool.Close()

	var executed int64
	agilePool.Submit(agilepool.TaskFunc(func() error { panic("task boom") }))
	agilePool.Submit(agilepool.TaskFunc(func() error {
		atomic.AddInt64(&executed, 1)
		return nil
	}))

	done := make(chan struct{})
	go func() {
		agilePool.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() blocked after the panic handler panicked")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&executed))
}

// TestAgilePoolBatchWithScaler verifies that the scaler spawns workers to
// drain tasks submitted in a burst, even when no worker goroutines existed
// at submission time (replaces the old safety-net / race-stuck-task test
// which is no longer applicable under the scaler-based design).
func TestAgilePoolBatchWithScaler(t *testing.T) {
	const (
		batchSize = 200
		capacity  = int64(1)
		deadline  = 3 * time.Second
	)

	iterations := 200
	if testing.Short() {
		iterations = 10
	}

	tests := []struct {
		name          string
		containerType agilepool.IdleContainerType
	}{
		{name: "linked_list", containerType: agilepool.LinkedListType},
		{name: "min_heap", containerType: agilepool.MinHeapType},
		{name: "slice", containerType: agilepool.SliceType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for iter := 0; iter < iterations; iter++ {
				p := agilepool.NewPool(agilepool.NewConfig(
					agilepool.WithWorkerNumCapacity(capacity),
					agilepool.WithTaskQueueSize(1000),
					agilepool.WithIdleContainerType(tt.containerType),
				))

				var executed int64
				var submitWG sync.WaitGroup

				for i := 0; i < batchSize; i++ {
					submitWG.Add(1)
					go func() {
						defer submitWG.Done()
						p.Submit(agilepool.TaskFunc(func() error {
							atomic.AddInt64(&executed, 1)
							return nil
						}))
					}()
				}

				submitWG.Wait()

				done := make(chan struct{})
				go func() {
					p.Wait()
					close(done)
				}()

				select {
				case <-done:
					p.Close()
				case <-time.After(deadline):
					p.Close()
					t.Fatalf("iter %d: timed out after %v, executed=%d/%d, runningWorkers=%d",
						iter, deadline, atomic.LoadInt64(&executed), batchSize,
						p.GetRunningWorkersNum())
				}
			}
		})
	}
}

// TestTaskQueueSizeConfiguresHandoffCapacity verifies that WithTaskQueueSize
// configures the capacity of the internal handoff channel.
// (Moved from queue_config_test.go)
func TestTaskQueueSizeConfiguresHandoffCapacity(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want int
	}{
		{name: "default", want: 10000},
		{name: "custom", size: 7, want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []agilepool.ConfigOption{}
			if tt.size > 0 {
				opts = append(opts, agilepool.WithTaskQueueSize(tt.size))
			}
			pool := agilepool.NewPool(agilepool.NewConfig(opts...))
			defer pool.Close()

			if got := pool.GetTaskQueueCapacity(); got != tt.want {
				t.Fatalf("task queue capacity = %d, want %d", got, tt.want)
			}
		})
	}
}
