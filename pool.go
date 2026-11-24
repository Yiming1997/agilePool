package agilepool

import (
	"context"
	"log"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yiming1997/agilePool/v2/internal/idle"
)

const (
	defaultCleanPeriod          = 500 * time.Millisecond
	defaultTaskQueueSize        = 10000
	defaultMaxWorkerNumCapacity = math.MaxInt64
	defaultWorkMode             = BLOCK
	defaultIdleContainerType    = LinkedListType
	defaultStatsSamplePeriod    = 100 * time.Millisecond
	defaultStatsWindowSize      = 10
	defaultScalerPeriod         = 10 * time.Millisecond
	defaultBacklogDecayFactor   = 0.3

	// defaultZeroConsumeScaleInterval rate-limits the zero-consume growth
	// fallback in scaleIfNeeded unless Config overrides it.
	defaultZeroConsumeScaleInterval = 100 * time.Millisecond

	// defaultMaxZeroConsumeScaleStep caps how many workers one zero-consume
	// growth step may add, so a deep backlog ramps up instead of spawning in
	// one burst.
	defaultMaxZeroConsumeScaleStep = int64(64)

	// defaultDrainHorizon is the default time the scaler aims to spend
	// clearing the current backlog. With a 30ms horizon a pool that measured a
	// 1ms service time needs 40 000 * 1ms / 30ms = 1333 workers to finish
	// 40 000 queued tasks, while a pool that measured a 10us service time
	// needs only 13.
	defaultDrainHorizon = 30 * time.Millisecond

	// taskChunkSize is the number of Task slots per chunk in the linked-list
	// buffer. Small, fixed-size chunks avoid the ~2× memory overhead of a
	// single dynamically-growing slice and reduce GC pressure.
	taskChunkSize = 4096

	// maxChunkLen caps the total number of tasks in the chunked buffer.
	// When reached, submitters block on the handoff channel instead of
	// growing the buffer further, bounding peak memory.
	maxChunkLen = 100_000

	// minShardableQueueSize is the smallest handoff capacity that is split
	// into multiple shards. Smaller queues stay single-channel so the exact
	// backpressure behaviour of tiny configurations is preserved.
	minShardableQueueSize = 1024

	// maxAutoShards caps the shard count chosen automatically from GOMAXPROCS.
	maxAutoShards = 32
)

// WorkMode controls how submission behaves when the pool cannot accept a task
// immediately.
type WorkMode int8

const (
	// BLOCK makes the submitter wait until the task is accepted or the pool
	// closes.
	BLOCK WorkMode = iota
	// NONBLOCK rejects the task immediately when it cannot be accepted; use
	// TrySubmit to observe the rejection.
	NONBLOCK
)

// Logger defines the logging interface used by the pool.
// Both the standard library's *log.Logger and structured loggers
// (e.g. zap.SugaredLogger) satisfy this interface.
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// defaultPool holds the most recently created Pool instance.
// It allows users to retrieve the pool object conveniently without
// keeping a reference themselves.
var defaultPool atomic.Pointer[Pool]

// GetDefaultPool returns the most recently created Pool, or nil if
// no pool has been created yet or the pool has been closed.
func GetDefaultPool() *Pool {
	p := defaultPool.Load()
	if p != nil && atomic.LoadInt32(&p.closed) == 1 {
		return nil
	}
	return p
}

// Pool is a goroutine pool with adaptive worker scaling, bounded concurrency,
// idle-worker reuse and an overflow task buffer. Create one with NewPool and
// release its resources with Close; call Wait for graceful shutdown.
type Pool struct {
	taskQueues        []chan Task // sharded handoff queues; len is a power of two
	shardMask         uint64      // len(taskQueues)-1, enables mask-based shard selection
	sendSeq           uint64      // atomic round-robin cursor picking the target shard
	workerSeq         uint64      // atomic cursor assigning distinct scan start shards
	taskQueueSize     int         // configured total handoff capacity (GetTaskQueueCapacity)
	closePoolCn       chan struct{}
	capacity          int64 // The maximum number of workers in the pool.
	runningWorkersNum int64
	closed            int32       // 1 once Close has been called, otherwise 0
	muIdle            sync.Locker // idle container lock: sync.Mutex for MutexLock, spin lock for SpinLock
	workerPool        sync.Pool   // Worker object pool
	idleWorks         idle.IdleContainer[*worker]
	config            *Config
	lock              *sync.Mutex

	// drainMu guards drainCh, the completion signal used by Wait. It is only
	// taken by Wait and by done on the 1→0 transition, so the per-task hot
	// path remains a single atomic operation. Wait creates the channel when it
	// observes a non-zero pendingTasks under drainMu, and the next done that
	// reaches zero closes it. A channel generation is used instead of a sync.WaitGroup
	// because it cannot panic when a rejected submission races with Wait:
	// there is no internal zero-waiter state for Add to violate.
	drainMu sync.Mutex
	drainCh chan struct{} // non-nil while a Wait caller is blocked on a pending drain; guarded by drainMu
	// logger is read by every goroutine that reports a panic (workers, the
	// submitting goroutine, Close), so it is stored atomically: SetLogger may
	// be called while the pool is running.
	logger atomic.Pointer[Logger]
	// workerCreateCount counts the total allocations from sync.Pool.New
	// over the pool lifetime. For the number of currently active workers,
	// use GetRunningWorkersNum().
	workerCreateCount int64
	// taskBuf stores tasks in a chunked linked-list buffer when taskQueue is full.
	taskBuf *chunkedTaskBuffer

	// pendingTasks counts submitted tasks that have not completed yet: queued,
	// executing inside a worker, or still being handed off by a submitter.
	// Unlike buf+channel occupancy it includes submitters blocked on
	// p.taskQueue <- task, giving the scaler full visibility into demand
	// regardless of buffer saturation. It is therefore NOT the queue depth:
	// use queuedTasks for the tasks that still need a worker.
	pendingTasks int64

	// ---- rate statistics for adaptive scaling ----
	submitCount  int64 // atomic, tasks submitted per window
	consumeCount int64 // atomic, tasks picked up by a worker per window
	exitCount    int64 // atomic, goroutines exited per window
	// doneCount is the monotonic number of tasks that finished. The scaler
	// uses it to measure how long a task takes: a task that was picked up but
	// has not returned yet says nothing about the service time.
	doneCount int64

	// lastSampleConsume is the consumeCount value at the last statistics
	// sample. Only the stats sampler touches it.
	lastSampleConsume int64

	statMu       sync.Mutex
	submitWindow *medianWindow // submit count per window
	consumeWin   *medianWindow // consume count per window
	exitWindow   *medianWindow // exit count per window
	// runningWindow keeps the running worker count sampled once per stats
	// window. The scaler reads its mean instead of the instantaneous count so
	// that scaling up does not immediately raise the pool's own target.
	runningWindow *int64Window

	// Tick-level state of the scaler goroutine: the doneCount value and the
	// time at the previous tick. They give the completion rate of a single
	// tick, which is what the backlog-driven ramp-up target uses to learn how
	// long a task takes without waiting for a whole statistics window.
	lastTickDone int64
	lastTickAt   time.Time

	// lastZeroConsumeScale is the timestamp of the last growth step taken by
	// the zero-consume fallback in scaleIfNeeded; accessed only by the scaler
	// goroutine.
	lastZeroConsumeScale time.Time

	hooks        Hooks        // lifecycle callbacks registered via SetHook
	panicHandler PanicHandler // custom task-panic handler (nil keeps default logging)
}

// NewPool creates a pool from c and starts its background cleaner, sampler and
// scaler goroutines. A nil Config uses the NewConfig defaults.
func NewPool(c *Config) *Pool {
	if c == nil {
		c = NewConfig()
	}
	p := &Pool{
		closePoolCn:  make(chan struct{}),
		config:       c,
		lock:         &sync.Mutex{},
		muIdle:       &sync.Mutex{}, // default value, overridden below based on config
		capacity:     c.workerNumCapacity,
		taskBuf:      newChunkedTaskBuffer(),
		panicHandler: c.panicHandler,
	}
	defaultLogger := Logger(log.Default())
	p.logger.Store(&defaultLogger)

	// Shard the handoff queue so a very large worker count does not serialise
	// on a single channel lock. Each shard gets at least one slot and the
	// total capacity stays >= the configured value.
	shardCount := resolveTaskQueueShards(c)
	perShardCap := (int(c.taskQueueSize) + shardCount - 1) / shardCount
	if perShardCap < 1 {
		perShardCap = 1
	}
	p.taskQueues = make([]chan Task, shardCount)
	for i := range p.taskQueues {
		p.taskQueues[i] = make(chan Task, perShardCap)
	}
	p.shardMask = uint64(shardCount - 1)
	p.taskQueueSize = int(c.taskQueueSize)

	// Select muIdle lock implementation based on config: SpinLock or MutexLock (sync.Mutex)
	if c.lockType == SpinLock {
		p.muIdle = newSpinLock()
	}

	switch c.idleContainerType {
	case idle.MinHeapType:
		p.idleWorks = idle.NewMinHeap[*worker]()
	case idle.SliceType:
		p.idleWorks = idle.NewSlice[*worker]()
	case idle.RingQueueType:
		p.idleWorks = idle.NewRingQueue[*worker]()
	case idle.TreapType:
		p.idleWorks = idle.NewTreap[*worker]()
	default:
		p.idleWorks = idle.NewLinkedList[*worker]()
	}

	atomic.StoreInt64(&p.workerCreateCount, 0)

	p.submitWindow = newMedianWindow(c.statsWindowSize)
	p.consumeWin = newMedianWindow(c.statsWindowSize)
	p.exitWindow = newMedianWindow(c.statsWindowSize)
	p.runningWindow = newInt64Window(c.statsWindowSize)

	p.workerPool.New = func() interface{} {
		atomic.AddInt64(&p.workerCreateCount, 1)
		w := &worker{
			pool: p,
		}
		return w
	}

	go p.expiredWorkerCleaner()
	go p.statsSampler()
	go p.scaler()
	defaultPool.Store(p)
	return p
}

// resolveTaskQueueShards returns the number of handoff-queue shards to use.
// The result is always a power of two so the round-robin cursor can be masked
// cheaply. An explicit Config value wins; otherwise small queues stay
// single-channel and large queues scale with GOMAXPROCS (capped).
func resolveTaskQueueShards(c *Config) int {
	want := c.taskQueueShards
	if want <= 0 {
		if c.taskQueueSize < minShardableQueueSize {
			return 1
		}
		want = runtime.GOMAXPROCS(0)
		if want > maxAutoShards {
			want = maxAutoShards
		}
	}
	if want < 1 {
		want = 1
	}
	shards := 1
	for shards < want {
		shards <<= 1
	}
	// Each shard needs at least one slot; never exceed the configured capacity.
	for int64(shards) > c.taskQueueSize && shards > 1 {
		shards >>= 1
	}
	return shards
}

// SetLogger replaces the default standard-library logger.
// Pass the same logger instance used elsewhere in your application
// (e.g. zap.SugaredLogger) so pool output appears in the same log stream.
//
// It is safe to call SetLogger while the pool is running tasks: the logger is
// read atomically by every logging path (task panics, hook panics and panic
// handlers). Passing nil restores the standard-library logger.
func (p *Pool) SetLogger(l Logger) {
	p.logger.Store(&l)
}

// getLogger returns the currently configured logger, falling back to the
// standard-library logger when none is installed (including after
// SetLogger(nil)).
func (p *Pool) getLogger() Logger {
	if lp := p.logger.Load(); lp != nil && *lp != nil {
		return *lp
	}
	return log.Default()
}

// Submit enqueues task for asynchronous execution. A nil task is ignored.
// Whether a saturated pool makes the caller wait depends on the configured
// WorkMode.
func (p *Pool) Submit(task Task) {
	if isNilTask(task) {
		return
	}

	_ = p.submit(context.Background(), task)
}

// TrySubmit submits a task and reports whether the pool accepted it.
// It follows the configured WorkMode: in BLOCK mode it may wait until
// the task is accepted, while in NONBLOCK mode it returns false when
// the task cannot be accepted immediately.
func (p *Pool) TrySubmit(task Task) bool {
	if isNilTask(task) {
		return false
	}
	return p.submit(context.Background(), task)
}

// SubmitCtx enqueues task together with ctx. A task whose ctx is already done
// is dropped, and a caller waiting on a saturated pool stops waiting when ctx
// is cancelled.
func (p *Pool) SubmitCtx(ctx context.Context, task Task) {
	if isNilTask(task) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		// SubmitCtx's explicit context takes precedence over any context
		// already carried by task; no outer contextTask is created on this path.
		p.dispatchTaskRejectedFor(ctx, nil)
		return
	}

	_ = p.submit(ctx, &contextTask{
		ctx:  ctx,
		task: task,
	})
}

func (p *Pool) submit(ctx context.Context, task Task) bool {
	// Register the task before the closed check so the scaler cannot observe
	// an empty pool and stop while this submission is still in flight.
	p.addPending()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.done() // Balance the addPending above because the closed pool rejects this task.
		p.dispatchTaskRejectedFor(ctx, task)
		return false
	}
	atomic.AddInt64(&p.submitCount, 1)

	// Cold start: if no goroutine is running, spawn one immediately.
	if atomic.LoadInt64(&p.runningWorkersNum) == 0 {
		if atomic.CompareAndSwapInt64(&p.runningWorkersNum, 0, 1) {
			w := p.workerPool.Get().(*worker)
			p.startWorker(w)
		}
	}
	hookCtx := context.Background()
	if wrapped, ok := task.(*contextTask); ok {
		hookCtx = wrapped.ctx
	}
	p.dispatchHook(func(h Hooks) {
		h.DispatchTaskSubmitted(hookCtx)
	})
	if p.config.workMode == NONBLOCK {
		q := p.nextShard()
		select {
		case q <- task:
			p.dispatchTaskEnqueuedFor(task)
			return true
		default:
			p.done()
			p.dispatchTaskRejectedFor(ctx, task)
			return false
		}
	}

	q := p.nextShard()
	select {
	case q <- task:
		p.dispatchTaskEnqueuedFor(task)
		return true
	default:
	}

	result := p.taskBuf.PushAndForward(task, func(t Task) bool {
		select {
		case p.nextShard() <- t:
			return true
		default:
			return false
		}
	})

	switch result {
	case taskBufferClosed:
		p.done()
		p.dispatchTaskRejectedFor(ctx, task)
		return false
	case taskBufferFull:
		// Once both queues are saturated, do not use a bare channel send:
		// SubmitCtx promises that cancellation interrupts this wait. Listening
		// for pool shutdown also prevents a blocked submitter from being left
		// behind after Close.
		q = p.nextShard()
		select {
		case q <- task:
			p.dispatchTaskEnqueuedFor(task)
			return true
		case <-ctx.Done():
			p.done()
			p.dispatchTaskRejectedFor(ctx, task)
			return false
		case <-p.closePoolCn:
			p.done()
			p.dispatchTaskRejectedFor(ctx, task)
			return false
		}
	default:
		// taskBufferAccepted: the task now lives in the overflow buffer, so
		// it is enqueued in the pool's sense and must fire Enqueued exactly
		// once. Dispatch after PushAndForward returned, i.e. outside the
		// buffer lock, so a slow or reentrant callback cannot stall buffer
		// access or self-deadlock on taskMu. The trade off is ordering: a
		// worker may already have consumed the task, so Enqueued can be
		// observed after Started (and, rarely, after Completed).
		p.dispatchTaskEnqueuedFor(task)
		return true
	}
}

func (p *Pool) dispatchTaskEnqueuedFor(task Task) {
	ctx := context.Background()
	if wrapped, ok := task.(*contextTask); ok {
		ctx = wrapped.ctx
	}
	p.dispatchHook(func(h Hooks) {
		h.DispatchTaskEnqueued(ctx)
	})
}

// dispatchTaskRejectedFor runs after admission bookkeeping has been balanced,
// so a rejected callback cannot strand this submission in Wait. Existing hook
// implementations need not implement the optional rejection event.
func (p *Pool) dispatchTaskRejectedFor(ctx context.Context, task Task) {
	if p.hooks == nil {
		return
	}
	rejected, ok := p.hooks.(RejectedHooks)
	if !ok {
		return
	}
	if wrapped, ok := task.(*contextTask); ok {
		ctx = wrapped.ctx
	}
	p.dispatchHook(func(Hooks) {
		rejected.DispatchTaskRejected(ctx)
	})
}

type contextTask struct {
	ctx  context.Context
	task Task
}

// UpdateTask wraps task with ctx so it can be submitted through Submit or
// TrySubmit. A nil ctx uses context.Background, matching SubmitCtx.
func UpdateTask(ctx context.Context, task Task) *contextTask {
	if ctx == nil {
		ctx = context.Background()
	}
	return &contextTask{ctx: ctx, task: task}
}

func (t *contextTask) process() {
	if t.ctx.Err() != nil {
		return
	}
	t.task.process()
}

// SubmitBefore submits a task with a start timeout. If the deadline expires
// while waiting for queue space or before execution, the task is skipped.
func (p *Pool) SubmitBefore(task Task, timeout time.Duration) {
	if isNilTask(task) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	accepted := p.submit(ctx,
		TaskFunc(func() error {
			defer cancel() // Ensures context is released after task completes to avoid resource leak
			select {
			case <-ctx.Done():
				return nil // Timeout reached, exit early
			default:
				task.process() // Execute the task
			}
			return nil
		}),
	)
	if !accepted {
		cancel()
	}
}

func isNilTask(task Task) bool {
	if task == nil {
		return true
	}

	switch t := task.(type) {
	case TaskFunc:
		return t == nil
	case *TaskWithRetry:
		return t == nil
	case *contextTask:
		return t == nil || isNilTask(t.task)
	default:
		return false
	}
}

func (p *Pool) addToIdle(w *worker) {
	p.muIdle.Lock()
	defer p.muIdle.Unlock()
	p.idleWorks.Add(w)
}

func (p *Pool) addRunningWorkersNum(num int64) {
	atomic.AddInt64(&p.runningWorkersNum, num)
}

// nextShard returns the handoff queue for the next submission using a lock-free
// round-robin cursor, spreading producer traffic across shards so that no
// single channel lock becomes a contention hotspot.
func (p *Pool) nextShard() chan Task {
	seq := atomic.AddUint64(&p.sendSeq, 1)
	return p.taskQueues[seq&p.shardMask]
}

// startWorker launches w's run loop, assigning it a round-robin scan start
// shard so freshly spawned workers do not all probe the same shard first.
func (p *Pool) startWorker(w *worker) {
	w.cursor = atomic.AddUint64(&p.workerSeq, 1) & p.shardMask
	go w.run(nil)
}

func (p *Pool) expiredWorkerCleaner() {
	ticker := time.NewTicker(p.config.cleanPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.muIdle.Lock()
			p.idleWorks.RemoveExpired(time.Now(), 1*time.Second)
			p.muIdle.Unlock()
			runtime.Gosched()
		case <-p.closePoolCn:
			return
		}
	}
}

// statsSampler samples the rate counters periodically and records them into
// the sliding-window histories. It outlives Close so the scaler keeps fresh
// medians while draining.
func (p *Pool) statsSampler() {
	ticker := time.NewTicker(p.config.statsSamplePeriod)
	defer ticker.Stop()

	for range ticker.C {
		p.sampleRates()
		// Keep sampling until the pool is closed and drained.
		if atomic.LoadInt32(&p.closed) == 1 && atomic.LoadInt64(&p.pendingTasks) == 0 {
			return
		}
	}
}

func (p *Pool) sampleRates() {
	sub := atomic.SwapInt64(&p.submitCount, 0)
	ext := atomic.SwapInt64(&p.exitCount, 0)

	// consumeCount is a monotonic total: the scaler reads it as well, to
	// measure the completion rate of a single tick, so it must not be reset
	// here. Each reader keeps its own baseline instead.
	cur := atomic.LoadInt64(&p.consumeCount)
	con := cur - p.lastSampleConsume
	p.lastSampleConsume = cur

	p.statMu.Lock()
	defer p.statMu.Unlock()

	p.submitWindow.add(sub)
	p.consumeWin.add(con)
	p.exitWindow.add(ext)
	// Sample the running worker count in the same window as the rates, so the
	// scaler reasons about one consistent snapshot of (arrivals, departures,
	// concurrency) instead of mixing a windowed rate with a live counter.
	p.runningWindow.add(atomic.LoadInt64(&p.runningWorkersNum))
}

// queuedTasks returns the number of tasks waiting for a worker: the handoff
// queue shards plus the chunked overflow buffer. Tasks already executing are
// not included, unlike pendingTasks.
func (p *Pool) queuedTasks() int64 {
	return int64(p.GetTaskQueueLen()) + p.taskBuf.Len()
}

func (p *Pool) scaleIfNeeded() {
	submitMed, consumeMed, exitMed := p.getMedianRates()
	running := atomic.LoadInt64(&p.runningWorkersNum)

	// serviceTime is how long one worker needs for one task, measured from
	// the completions of the previous scaler tick. It is a property of the
	// workload, not of the queue depth: a pool serving slow tasks needs many
	// workers to drain a queue, a pool serving fast tasks needs few.
	//
	// The measurement is only trusted while the scaler runs on schedule. A
	// long gap means the tick covered a period that is not representative of
	// the current load, and the drain target is skipped instead.
	serviceTime := p.measureServiceTime(running)

	bufDepth := p.taskBuf.Len()

	// totalBacklog uses pendingTasks (atomic), covering all submitted
	// tasks that have not completed yet, including submitters blocked on
	// the handoff channel that are invisible to bufDepth + len(taskQueue).
	totalBacklog := atomic.LoadInt64(&p.pendingTasks)

	var target int64

	// runningAvg is the mean running worker count over the same statistics
	// window as submitMed and consumeMed. Using the windowed average instead
	// of the live counter keeps the target from feeding back on itself: a
	// scale-up changes `running` immediately, but the windowed snapshot stays
	// fixed until the next sample, so the target cannot compound within one
	// stale window.
	runningAvg := p.getRunningAverage()

	// Rate-based target: target = b / a = submitMed * runningAvg / consumeMed.
	// The implied service time (runningAvg / consumeMed) is a property of the
	// observed window, not of the pool's current size.
	if runningAvg > 0 && consumeMed > 0 && submitMed > 0 {
		target = int64(submitMed * runningAvg / consumeMed)
	}

	// Backlog-weighted target: treat backlog as additional incoming tasks.
	// decayFactor is dynamically adjusted by bufPressure — when the overflow
	// buffer is deep relative to the drain rate, the scaler becomes more
	// aggressive; when it's shallow, the configured decayFactor dominates.
	if totalBacklog > 0 {
		switch {
		case running == 0:
			// Cold start: spawn enough to drain the backlog (up to capacity).
			target = totalBacklog
		case consumeMed > 0:
			// bufPressure ∈ [0,1]: how many 100ms cycles needed to drain buf alone.
			bufCycles := float64(bufDepth) / consumeMed
			bufPressure := min(1.0, bufCycles*0.15)

			// dynamicDecay ∈ [decayFactor, 1.0].
			dynamicDecay := p.config.backlogDecayFactor +
				(1-p.config.backlogDecayFactor)*bufPressure

			effectiveSubmit := submitMed + float64(totalBacklog)*dynamicDecay
			qTarget := int64(effectiveSubmit * runningAvg / consumeMed)
			if qTarget > target {
				target = qTarget
			}
		default:
			// Zero-consume fallback: while the pool idles its sliding window
			// fills with zeros, so a one-shot burst of long-running tasks
			// cannot raise either median. Both branches above then stay blind
			// although work is queued, and only the single cold-start worker
			// ever runs — the pool never grows even though capacity allows it.
			// Fall back to the real queue depth (excluding executing tasks)
			// and ramp up in bounded steps.
			queued := p.queuedTasks()
			room := p.capacity - running
			now := time.Now()
			if queued > 0 && room > 0 &&
				now.Sub(p.lastZeroConsumeScale) >= p.config.zeroConsumeScaleInterval {
				growth := min(queued, max(int64(1), running), p.config.maxZeroConsumeScaleStep, room)
				target = running + growth
				p.lastZeroConsumeScale = now
			}
		}
	}

	// Backlog-driven ramp-up target.
	//
	// The rate target above only moves once per statistics window, so a cold
	// pool that receives a burst stays at one worker for a whole window and
	// then grows one bounded step per window. Under IO workloads the burst is
	// over before the target reacts and the throughput is lost.
	//
	// The target below is the number of workers that can finish the queued
	// work within the drain horizon:
	//
	//	drainTarget = ceil(queuedTasks * serviceTime / drainHorizon)
	//
	// It contains no worker count, so it cannot feed back on the pool's own
	// scaling decision, and it shrinks with the queue, so it cannot run away.
	// Because it is derived from the measured service time, a pool serving
	// fast tasks keeps a small pool while a pool serving slow tasks opens a
	// large one for the same queue depth.
	if p.config.drainHorizon > 0 && serviceTime > 0 {
		queued := p.queuedTasks()
		if queued > 0 {
			need := float64(queued) * float64(serviceTime) / float64(p.config.drainHorizon)
			drainTarget := int64(math.Ceil(need))
			if drainTarget > target {
				target = drainTarget
			}
		}
	}

	// A pool never needs more workers than there are pending tasks: a worker
	// runs at most one task at a time, so any extra worker would find nothing
	// to do and park immediately. This is also the brake on the backlog target
	// above: qTarget scales with the current worker count, so with the default
	// unbounded capacity a deep queue would otherwise multiply the running
	// count on every scaler tick until it runs away.
	if target > totalBacklog {
		target = totalBacklog
	}

	if target > p.capacity {
		target = p.capacity
	}
	if target <= running {
		return
	}

	toSpawn := target - running

	// Compensate for goroutines that will exit during the next scaler tick.
	// Scale exitMed (per-sample-window) to scaler-period units.
	exitPerTick := int64(exitMed * float64(p.config.scalerPeriod) / float64(p.config.statsSamplePeriod))
	if exitPerTick > 0 {
		toSpawn += exitPerTick
	}

	// Bounding checks
	maxSpawn := p.capacity - running
	if toSpawn > maxSpawn {
		toSpawn = maxSpawn
	}
	if toSpawn <= 0 {
		return
	}

	p.lock.Lock()
	// Re-check under lock for thread safety
	currentRunning := atomic.LoadInt64(&p.runningWorkersNum)
	actualSpawn := p.capacity - currentRunning
	if toSpawn < actualSpawn {
		actualSpawn = toSpawn
	}
	if actualSpawn <= 0 {
		p.lock.Unlock()
		return
	}
	p.addRunningWorkersNum(actualSpawn)
	p.lock.Unlock()

	p.muIdle.Lock()
	for i := int64(0); i < actualSpawn; i++ {
		// Reuse idle workers before allocating new ones.
		w := p.idleWorks.Pop()
		if w == nil {
			w = p.workerPool.Get().(*worker)
		}
		p.muIdle.Unlock()
		p.startWorker(w)
		p.muIdle.Lock()
	}
	p.muIdle.Unlock()
}

// measureServiceTime returns how long one worker needs for one task, based on
// the completions of the previous scaler tick.
//
// It is called only by the scaler goroutine, which owns the tick state.
// The measurement is skipped when the scaler has fallen behind schedule: the
// tick then covers a period that is not representative of the current load,
// and a zero result disables the backlog-driven target instead of sizing it
// from a stale rate.
func (p *Pool) measureServiceTime(running int64) time.Duration {
	now := time.Now()
	cur := atomic.LoadInt64(&p.doneCount)
	done := cur - p.lastTickDone
	elapsed := now.Sub(p.lastTickAt)
	p.lastTickDone = cur
	p.lastTickAt = now

	if done <= 0 || running <= 0 || elapsed <= 0 {
		return 0
	}
	if elapsed > 10*p.config.scalerPeriod {
		return 0
	}
	// done tasks were completed by `running` workers in `elapsed`, so one
	// worker completes one task every elapsed/(done/running).
	perWorker := float64(elapsed) * float64(running) / float64(done)
	return time.Duration(perWorker)
}

// getMedianRates returns the median value of each counters over the last
// statsWindowSize windows (one sample per statsSamplePeriod).
// Returns (submitMed, consumeMed, exitMed).
func (p *Pool) getMedianRates() (submitMed, consumeMed, exitMed float64) {
	p.statMu.Lock()
	defer p.statMu.Unlock()
	return p.submitWindow.median(), p.consumeWin.median(), p.exitWindow.median()
}

// getRunningAverage returns the mean running worker count over the statistics
// window. It is sampled once per stats window, so it trails the live counter
// by up to statsWindowSize windows; that lag is the point: the scaler decides
// how many workers to run from a snapshot that its own recent scaling actions
// cannot change.
func (p *Pool) getRunningAverage() float64 {
	p.statMu.Lock()
	defer p.statMu.Unlock()
	return p.runningWindow.mean()
}

// scaler periodically checks whether the pool needs more goroutines to keep up
// with the submission rate, and spawns them proactively. It outlives Close:
// accepted tasks still need workers to drain.
func (p *Pool) scaler() {
	ticker := time.NewTicker(p.config.scalerPeriod)
	defer ticker.Stop()

	for range ticker.C {
		p.scaleIfNeeded()
		// Keep scaling until the pool is closed and drained; stopping at Close
		// would leave stranded tasks without workers and hang Wait.
		if atomic.LoadInt32(&p.closed) == 1 && atomic.LoadInt64(&p.pendingTasks) == 0 {
			return
		}
	}
}

// Close marks the pool as closed and stops its background cleaner goroutine.
// After Close:
//   - new Submit calls become no-ops (the task is dropped, no goroutine is started)
//   - in-flight tasks already submitted continue to run to completion
//   - Wait() returns once all in-flight tasks are done, enabling graceful shutdown
//
// Close is idempotent and safe to call from any goroutine, including from
// within a running task.
func (p *Pool) Close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	defaultPool.CompareAndSwap(p, nil)

	p.taskBuf.Close()

	close(p.closePoolCn)
	p.dispatchHook(func(h Hooks) {
		h.DispatchPoolClosed(p) // fire OnPoolClosed hooks
	})
}

// Wait blocks until every accepted task has completed. It does not stop the
// pool; call Close afterwards to release the background goroutines.
//
// Wait may be called concurrently with Submit, including a Submit that is
// rejected because the pool is already closed; such a rejected submission is
// still accounted for and cannot panic the waiter.
func (p *Pool) Wait() {
	p.drainMu.Lock()
	// Decide from the counter, not from whether a channel exists: a submitter
	// that has already incremented pendingTasks is pending, regardless of
	// whether anyone has published a channel for it yet.
	if atomic.LoadInt64(&p.pendingTasks) == 0 {
		p.drainMu.Unlock()
		return
	}
	if p.drainCh == nil {
		p.drainCh = make(chan struct{})
	}
	ch := p.drainCh
	p.drainMu.Unlock()
	<-ch
}

// addPending registers a task in the pending count before it can be rejected
// or executed. The drain channel is created lazily by Wait, so submission is
// a single atomic with no lock or allocation.
func (p *Pool) addPending() {
	atomic.AddInt64(&p.pendingTasks, 1)
}

// done marks one task complete and wakes Wait callers when the pending count
// reaches zero. The zero check is repeated under drainMu because a concurrent
// addPending may have made the pool non-empty again after the decrement, and
// a channel Wait created for that new batch must not be closed early.
func (p *Pool) done() {
	if atomic.AddInt64(&p.pendingTasks, -1) != 0 {
		return
	}
	p.drainMu.Lock()
	if atomic.LoadInt64(&p.pendingTasks) == 0 && p.drainCh != nil {
		close(p.drainCh)
		p.drainCh = nil
	}
	p.drainMu.Unlock()
}

// GetRunningWorkersNum returns the number of workers currently executing
// tasks, excluding workers parked in the idle container.
func (p *Pool) GetRunningWorkersNum() int64 {
	return atomic.LoadInt64(&p.runningWorkersNum)
}

// GetWorkerCreateCount returns the total number of worker structs that have
// been allocated from sync.Pool.New over the pool's lifetime.
func (p *Pool) GetWorkerCreateCount() int64 {
	return atomic.LoadInt64(&p.workerCreateCount)
}

// GetPendingTaskCount returns unfinished tasks, including queued tasks,
// worker-held tasks and submissions still waiting for queue space.
func (p *Pool) GetPendingTaskCount() int64 {
	return atomic.LoadInt64(&p.pendingTasks)
}

// GetQueuedTaskCount returns a snapshot of the handoff shards plus the
// overflow buffer. It excludes tasks already fetched by workers (including
// prefetched batches) and blocked submitters. Reading the buffer takes its
// mutex; the shard and buffer readings are not one atomic snapshot.
func (p *Pool) GetQueuedTaskCount() int64 {
	return p.queuedTasks()
}

// GetTaskQueueLen returns the number of tasks currently queued in the
// handoff channel (taskQueue), i.e. tasks submitted but not yet picked
// up by a worker. This is a snapshot of len(taskQueue) and does not
// include tasks waiting in the chunked overflow buffer.
func (p *Pool) GetTaskQueueLen() int {
	// Returns the number of tasks across all handoff-queue shards that have
	// been submitted but not yet picked up by a worker.
	total := 0
	for _, q := range p.taskQueues {
		total += len(q)
	}
	return total
}

// GetTaskQueueCapacity returns the capacity of the handoff channel
// (taskQueue) configured for this pool.
func (p *Pool) GetTaskQueueCapacity() int {
	return p.taskQueueSize
}

// GetIdleWorkerCount returns the number of workers currently parked in
// the idle container, waiting to be reused. These workers are not
// actively processing tasks.
func (p *Pool) GetIdleWorkerCount() int64 {
	// Returns the current number of idle workers available for task assignment.
	return p.idleWorks.Len()
}

// GetCapacity returns the maximum number of workers that the pool can
// create and maintain concurrently. This value is set during pool
// initialization and remains constant throughout the pool's lifecycle.
// Using a getter function provides a more idiomatic and professional API.
func (p *Pool) GetCapacity() int64 {
	return p.capacity
}

// SetHook installs the lifecycle callback dispatcher. Register callbacks
// before the pool starts processing tasks: the hook set is read without
// synchronization by every dispatch path, so replacing it while tasks are in
// flight is a data race. A nil set disables hook dispatch.
func (p *Pool) SetHook(hooks Hooks) error {
	p.hooks = hooks
	return nil
}

// dispatchHook invokes fn with the pool's hook set and absorbs any panic a
// hooks implementation raises. hook.Hooks already recovers each registered
// callback; this guard additionally keeps a custom implementation that panics
// mid-dispatch from crashing the submitting goroutine, a worker, or Close,
// and from skipping pool bookkeeping such as done().
func (p *Pool) dispatchHook(fn func(h Hooks)) {
	if p.hooks == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			p.getLogger().Printf("hook dispatch panicked: %v\n%s\n", r, Stack(1))
		}
	}()
	fn(p.hooks)
}

// handlePanic reports a task panic that was recovered by the worker's runTask
// defer. When a PanicHandler is configured it takes over; otherwise the
// recovered value and stack trace are written to the pool logger, preserving
// the historical default. A panicking handler is itself recovered and logged
// so it cannot crash the worker goroutine or skip the task-completion hook
// that runTask dispatches afterwards.
func (p *Pool) handlePanic(task Task, recovered any, stack []byte) {
	handler := p.panicHandler
	if handler == nil {
		p.getLogger().Printf("worker exits from panic: %v\n%s\n", recovered, stack)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			p.getLogger().Printf("panic handler panicked: %v\n%s\n", r, Stack(1))
		}
	}()
	handler(task, recovered, stack)
}
