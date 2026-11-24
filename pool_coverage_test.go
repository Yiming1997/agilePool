package agilepool

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigOptionsApply(t *testing.T) {
	cfg := NewConfig(
		WithCleanPeriod(2*time.Second),
		WithTaskQueueSize(2048),
		WithTaskQueueShards(8),
		WithWorkerNumCapacity(16),
		WithBlockMode(NONBLOCK),
		WithIdleContainerType(RingQueueType),
		WithStatsSamplePeriod(3*time.Second),
		WithStatsWindowSize(7),
		WithScalerPeriod(4*time.Second),
		WithLockType(SpinLock),
		WithBacklogDecayFactor(0.25),
	)

	if got := cfg.cleanPeriod; got != 2*time.Second {
		t.Fatalf("cleanPeriod = %v, want 2s", got)
	}
	if got := cfg.taskQueueSize; got != 2048 {
		t.Fatalf("taskQueueSize = %d, want 2048", got)
	}
	if got := cfg.taskQueueShards; got != 8 {
		t.Fatalf("taskQueueShards = %d, want 8", got)
	}
	if got := cfg.workerNumCapacity; got != 16 {
		t.Fatalf("workerNumCapacity = %d, want 16", got)
	}
	if got := cfg.workMode; got != NONBLOCK {
		t.Fatalf("workMode = %v, want NONBLOCK", got)
	}
	if got := cfg.idleContainerType; got != RingQueueType {
		t.Fatalf("idleContainerType = %v, want RingQueueType", got)
	}
	if got := cfg.statsSamplePeriod; got != 3*time.Second {
		t.Fatalf("statsSamplePeriod = %v, want 3s", got)
	}
	if got := cfg.statsWindowSize; got != 7 {
		t.Fatalf("statsWindowSize = %d, want 7", got)
	}
	if got := cfg.scalerPeriod; got != 4*time.Second {
		t.Fatalf("scalerPeriod = %v, want 4s", got)
	}
	if got := cfg.lockType; got != SpinLock {
		t.Fatalf("lockType = %v, want SpinLock", got)
	}
	if got := cfg.backlogDecayFactor; got != 0.25 {
		t.Fatalf("backlogDecayFactor = %v, want 0.25", got)
	}
}

func TestConfigOptionsIgnoreOutOfRangeValues(t *testing.T) {
	defaults := NewConfig()
	cfg := NewConfig(
		WithCleanPeriod(0),
		WithCleanPeriod(-time.Second),
		WithTaskQueueSize(-1),
		WithWorkerNumCapacity(0),
		WithStatsSamplePeriod(0),
		WithStatsWindowSize(-2),
		WithScalerPeriod(-1),
		WithBacklogDecayFactor(-0.5),
		WithBacklogDecayFactor(1.5),
	)

	if cfg.cleanPeriod != defaults.cleanPeriod {
		t.Fatalf("cleanPeriod = %v, want default %v", cfg.cleanPeriod, defaults.cleanPeriod)
	}
	if cfg.taskQueueSize != defaults.taskQueueSize {
		t.Fatalf("taskQueueSize = %d, want default %d", cfg.taskQueueSize, defaults.taskQueueSize)
	}
	if cfg.workerNumCapacity != defaults.workerNumCapacity {
		t.Fatalf("workerNumCapacity = %d, want default %d", cfg.workerNumCapacity, defaults.workerNumCapacity)
	}
	if cfg.statsSamplePeriod != defaults.statsSamplePeriod {
		t.Fatalf("statsSamplePeriod = %v, want default %v", cfg.statsSamplePeriod, defaults.statsSamplePeriod)
	}
	if cfg.statsWindowSize != defaults.statsWindowSize {
		t.Fatalf("statsWindowSize = %d, want default %d", cfg.statsWindowSize, defaults.statsWindowSize)
	}
	if cfg.scalerPeriod != defaults.scalerPeriod {
		t.Fatalf("scalerPeriod = %v, want default %v", cfg.scalerPeriod, defaults.scalerPeriod)
	}
	if cfg.backlogDecayFactor != defaults.backlogDecayFactor {
		t.Fatalf("backlogDecayFactor = %v, want default %v", cfg.backlogDecayFactor, defaults.backlogDecayFactor)
	}

	if got := NewConfig(WithBacklogDecayFactor(0)).backlogDecayFactor; got != 0 {
		t.Fatalf("backlogDecayFactor = %v, want 0", got)
	}
	if got := NewConfig(WithBacklogDecayFactor(1)).backlogDecayFactor; got != 1 {
		t.Fatalf("backlogDecayFactor = %v, want 1", got)
	}
}

func TestResolveTaskQueueShards(t *testing.T) {
	tests := []struct {
		name   string
		size   int64
		shards int
		want   int
	}{
		{name: "small queue stays single", size: 100, want: 1},
		{name: "explicit count rounds up to power of two", size: 10000, shards: 3, want: 4},
		{name: "explicit count capped by capacity", size: 2, shards: 64, want: 2},
		{name: "explicit one", size: 10000, shards: 1, want: 1},
		{name: "non positive shards fall back", size: 100, shards: -1, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveTaskQueueShards(&Config{
				taskQueueSize:   tt.size,
				taskQueueShards: tt.shards,
			})
			if got != tt.want {
				t.Fatalf("resolveTaskQueueShards(size=%d, shards=%d) = %d, want %d",
					tt.size, tt.shards, got, tt.want)
			}
		})
	}

	// Automatic sharding for a large queue must be a power of two, bounded by
	// GOMAXPROCS (capped) and by the configured capacity.
	want := runtime.GOMAXPROCS(0)
	if want > maxAutoShards {
		want = maxAutoShards
	}
	got := resolveTaskQueueShards(&Config{taskQueueSize: 10000})
	if got < 1 || got&(got-1) != 0 {
		t.Fatalf("auto shards = %d, want a power of two", got)
	}
	if got < want || got >= 2*want {
		t.Fatalf("auto shards = %d, want in [%d, %d)", got, want, 2*want)
	}
	if int64(got) > 10000 {
		t.Fatalf("auto shards = %d, exceeds queue capacity", got)
	}
}

func TestNewPoolSupportsAllContainerAndLockTypes(t *testing.T) {
	containers := []IdleContainerType{
		LinkedListType,
		MinHeapType,
		SliceType,
		RingQueueType,
		TreapType,
	}
	locks := []LockType{MutexLock, SpinLock}

	for _, container := range containers {
		for _, lock := range locks {
			p := NewPool(NewConfig(
				WithIdleContainerType(container),
				WithLockType(lock),
				WithWorkerNumCapacity(4),
			))

			var executed atomic.Int64
			p.Submit(TaskFunc(func() error {
				executed.Add(1)
				return nil
			}))
			p.Wait()
			p.Close()

			if got := executed.Load(); got != 1 {
				t.Fatalf("container=%v lock=%v executed = %d, want 1", container, lock, got)
			}
		}
	}
}

func TestPoolGettersAndDefaultPool(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(7),
		WithTaskQueueSize(2048),
		WithTaskQueueShards(2),
		WithIdleContainerType(RingQueueType),
		WithLockType(SpinLock),
	))

	if got := GetDefaultPool(); got != p {
		t.Fatalf("GetDefaultPool() = %p, want %p", got, p)
	}
	if got := p.GetCapacity(); got != 7 {
		t.Fatalf("GetCapacity() = %d, want 7", got)
	}
	if got := p.GetTaskQueueCapacity(); got != 2048 {
		t.Fatalf("GetTaskQueueCapacity() = %d, want 2048", got)
	}
	if got := p.GetTaskQueueLen(); got != 0 {
		t.Fatalf("GetTaskQueueLen() = %d, want 0", got)
	}
	if got := p.GetRunningWorkersNum(); got != 0 {
		t.Fatalf("GetRunningWorkersNum() = %d, want 0", got)
	}
	if got := p.GetIdleWorkerCount(); got != 0 {
		t.Fatalf("GetIdleWorkerCount() = %d, want 0", got)
	}
	if got := p.GetWorkerCreateCount(); got != 0 {
		t.Fatalf("GetWorkerCreateCount() = %d, want 0", got)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	p.Submit(TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	if got := p.GetRunningWorkersNum(); got < 1 {
		t.Fatalf("GetRunningWorkersNum() = %d, want >= 1", got)
	}
	if got := p.GetWorkerCreateCount(); got < 1 {
		t.Fatalf("GetWorkerCreateCount() = %d, want >= 1", got)
	}

	for i := 0; i < 3; i++ {
		p.Submit(TaskFunc(func() error { return nil }))
	}
	if got := p.GetTaskQueueLen(); got < 1 {
		t.Fatalf("GetTaskQueueLen() = %d, want >= 1 while the worker is busy", got)
	}

	close(release)
	p.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for p.GetIdleWorkerCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.GetIdleWorkerCount(); got < 1 {
		t.Fatalf("GetIdleWorkerCount() = %d, want >= 1 after draining", got)
	}
	if got := p.GetTaskQueueLen(); got != 0 {
		t.Fatalf("GetTaskQueueLen() = %d, want 0 after draining", got)
	}

	p.Close()
	if got := GetDefaultPool(); got != nil {
		t.Fatalf("GetDefaultPool() after Close = %p, want nil", got)
	}
	p.Close() // idempotent
}

func TestPoolNilConfigUsesDefaults(t *testing.T) {
	p := NewPool(nil)
	defer p.Close()

	if got := p.GetTaskQueueCapacity(); got != defaultTaskQueueSize {
		t.Fatalf("GetTaskQueueCapacity() = %d, want %d", got, defaultTaskQueueSize)
	}
	if got := p.GetCapacity(); got != defaultMaxWorkerNumCapacity {
		t.Fatalf("GetCapacity() = %d, want %d", got, defaultMaxWorkerNumCapacity)
	}
}

func TestSubmitCtxNilContextAndAcceptedPath(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(2)))
	defer p.Close()

	var executed atomic.Int64
	p.SubmitCtx(nil, nil)
	p.SubmitCtx(nil, TaskFunc(func() error {
		executed.Add(1)
		return nil
	}))
	p.SubmitCtx(context.Background(), TaskFunc(func() error {
		executed.Add(1)
		return nil
	}))
	p.Wait()

	if got := executed.Load(); got != 2 {
		t.Fatalf("executed = %d, want 2", got)
	}
}

func TestSubmitAfterCloseIsDropped(t *testing.T) {
	p := NewPool(NewConfig(WithWorkerNumCapacity(2)))
	p.Close()
	p.Close() // idempotent

	var executed atomic.Int64
	task := TaskFunc(func() error {
		executed.Add(1)
		return nil
	})
	p.Submit(task)
	p.SubmitCtx(context.Background(), task)
	if ok := p.TrySubmit(task); ok {
		t.Fatal("TrySubmit accepted a task after Close")
	}
	p.Wait()

	if got := executed.Load(); got != 0 {
		t.Fatalf("executed = %d, want 0 after Close", got)
	}
}

func TestNonBlockSubmitRejectsWhenHandoffQueueFull(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1),
		WithTaskQueueShards(1),
		WithBlockMode(NONBLOCK),
	))
	defer p.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	p.Submit(TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	if ok := p.TrySubmit(TaskFunc(func() error { return nil })); !ok {
		t.Fatal("second task should fit in the empty handoff queue")
	}
	if ok := p.TrySubmit(TaskFunc(func() error { return nil })); ok {
		t.Fatal("third task should be rejected while the handoff queue is full")
	}

	close(release)
	p.Wait()
}

func TestSubmitBeforeTimeoutSkipsTask(t *testing.T) {
	p := NewPool(NewConfig(
		WithWorkerNumCapacity(1),
		WithTaskQueueSize(1),
		WithTaskQueueShards(1),
	))
	defer p.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	p.Submit(TaskFunc(func() error {
		close(started)
		<-release
		return nil
	}))
	<-started

	var executed atomic.Int64
	p.SubmitBefore(TaskFunc(func() error {
		executed.Add(1)
		return nil
	}), 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond) // let the deadline expire while queued

	close(release)
	p.Wait()

	if got := executed.Load(); got != 0 {
		t.Fatalf("executed = %d, want 0 because the deadline expired", got)
	}
}

func TestIsNilTaskVariants(t *testing.T) {
	var nilFunc TaskFunc
	var nilRetry *TaskWithRetry
	var nilCtxTask *contextTask

	if !isNilTask(nil) {
		t.Fatal("isNilTask(nil) = false")
	}
	if !isNilTask(nilFunc) {
		t.Fatal("isNilTask(nil TaskFunc) = false")
	}
	if !isNilTask(nilRetry) {
		t.Fatal("isNilTask(nil *TaskWithRetry) = false")
	}
	if !isNilTask(nilCtxTask) {
		t.Fatal("isNilTask(nil *contextTask) = false")
	}
	if !isNilTask(&contextTask{ctx: context.Background()}) {
		t.Fatal("isNilTask(*contextTask) with nil inner task = false")
	}
	if isNilTask(TaskFunc(func() error { return nil })) {
		t.Fatal("isNilTask(non-nil TaskFunc) = true")
	}
	if isNilTask(&TaskWithRetry{}) {
		t.Fatal("isNilTask(non-nil *TaskWithRetry) = true")
	}
	if isNilTask(UpdateTask(context.Background(), TaskFunc(func() error { return nil }))) {
		t.Fatal("isNilTask(UpdateTask(...)) = true")
	}
}

func TestNewTaskAndNewWorker(t *testing.T) {
	var executed atomic.Int64
	task := NewTask(func() error {
		executed.Add(1)
		return nil
	})
	if task == nil {
		t.Fatal("NewTask() returned nil")
	}
	task.process()
	if got := executed.Load(); got != 1 {
		t.Fatalf("task executions = %d, want 1", got)
	}

	p := NewPool(NewConfig())
	defer p.Close()

	w := newWorker(p)
	if w == nil {
		t.Fatal("newWorker() returned nil")
	}
	if w.pool != p {
		t.Fatal("newWorker() did not attach the pool")
	}
	if !w.DatedTime().IsZero() {
		t.Fatalf("new worker DatedTime() = %v, want zero", w.DatedTime())
	}
}

func TestMedianWindowKeepsExactSamples(t *testing.T) {
	w := newMedianWindow(3)

	if got := w.median(); got != 0 {
		t.Fatalf("empty window median = %v, want 0", got)
	}

	w.add(10)
	w.add(20)
	if got := w.median(); got != 20 {
		t.Fatalf("median = %v, want 20 (upper middle of two samples)", got)
	}

	w.add(30)
	if got := w.median(); got != 20 {
		t.Fatalf("median = %v, want 20", got)
	}

	// The window is full: the next sample evicts the oldest one.
	w.add(60)
	if got := w.median(); got != 30 {
		t.Fatalf("median after eviction = %v, want 30", got)
	}
	if w.count != 3 {
		t.Fatalf("count = %d, want 3", w.count)
	}
}

func TestMedianWindowReportsExactLargeValues(t *testing.T) {
	// The bucketed histogram reported 10001 for every rate above 10000, which
	// is what made the scaler overshoot on burst traffic.
	w := newMedianWindow(2)
	w.add(40000)
	w.add(40000)
	if got := w.median(); got != 40000 {
		t.Fatalf("median = %v, want the exact value 40000", got)
	}
}

func TestMedianWindowZeroSize(t *testing.T) {
	w := newMedianWindow(0)
	w.add(7)
	if got := w.median(); got != 7 {
		t.Fatalf("median = %v, want 7", got)
	}
	w.add(9)
	if got := w.median(); got != 9 {
		t.Fatalf("median = %v, want 9", got)
	}
}
