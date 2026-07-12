package agilepool_test

import (
	"context"
	"errors"
	"io"
	"log"
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

// ---- SampleRate / SetSampleRate ----

func TestPool_SetSampleRate(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()

	p.SetSampleRate(0.5)
	// sampleRate is unexported; verify behaviour instead by submitting a
	// contextTask and confirming no panic.
	ctx := context.Background()
	p.SubmitCtx(ctx, agilepool.TaskFunc(func() error { return nil }))
	p.Wait()
}

func TestPool_WithSampleRateConfig(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithSampleRate(0.3),
	))
	defer p.Close()

	// Pool with sampleRate set via Config must still submit and execute
	// contextTask normally.
	var executed int64
	p.SubmitCtx(context.Background(), agilepool.TaskFunc(func() error {
		executed = 1
		return nil
	}))
	p.Wait()
	assert.Equal(t, int64(1), executed)
}

func TestPool_WithSampleRateDefault(t *testing.T) {
	// Without WithSampleRate, sampleRate defaults to 0.
	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()

	var executed int64
	p.SubmitCtx(context.Background(), agilepool.TaskFunc(func() error {
		executed = 1
		return nil
	}))
	p.Wait()
	assert.Equal(t, int64(1), executed)
}

func TestPool_SetSampleRateChainable(t *testing.T) {
	// Set the rate, then submit.
	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()

	p.SetSampleRate(1.0) // 100% sampling

	var executed int64
	p.SubmitCtx(context.Background(), agilepool.TaskFunc(func() error {
		executed = 1
		return nil
	}))
	p.Wait()
	assert.Equal(t, int64(1), executed)
}
