package agilepool

import (
	"context"
	"log"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCleanPeriod          = 500 * time.Millisecond
	defaultTaskQueueSize        = 200000
	defaultMaxWorkerNumCapacity = math.MaxInt64
	defaultWorkMode             = BLOCK
	defaultIdleContainerType    = LinkedListType
	defaultStatsSamplePeriod    = 100 * time.Millisecond
	defaultStatsWindowSize      = 10
	defaultScalerPeriod         = 10 * time.Millisecond
	defaultBacklogDecayFactor   = 0.3

	// internalTaskQueueCap is the fixed capacity of the internal handoff
	// channel. It is small and NOT user-configurable — workers pull from
	// taskBuf for the bulk of queued tasks, so channel capacity does not
	// couple memory usage or scaler behaviour to the queue-size setting.
	internalTaskQueueCap = 10000

	// taskChunkSize is the number of Task slots per chunk in the linked-list
	// buffer. Small, fixed-size chunks avoid the ~2× memory overhead of a
	// single dynamically-growing slice and reduce GC pressure.
	taskChunkSize = 4096

	// maxChunkLen caps the total number of tasks in the chunked buffer.
	// When reached, submitters block on the handoff channel instead of
	// growing the buffer further, bounding peak memory.
	maxChunkLen = 100_000
)

type WorkMode int8

const (
	BLOCK WorkMode = iota
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

// taskChunk is a fixed-size node in the linked-list task buffer.
// Each chunk holds up to taskChunkSize Task values. Using small,
// fixed-size nodes avoids the doubling overhead of a single slice
// and makes GC work with smaller, independently collectable objects.
type taskChunk struct {
	tasks [taskChunkSize]Task
	next  *taskChunk
}

type Pool struct {
	taskQueue         chan Task
	closePoolCn       chan struct{}
	capacity          int64 // The maximum number of workers in the pool.
	runningWorkersNum int64
	closed            int32 // 1 once Close has been called, otherwise 0
	muIdle            sync.Mutex
	workerPool        sync.Pool // Worker object pool
	idleWorks         IdleWorkerContainer
	config            *Config
	lock              *sync.Mutex
	wg                sync.WaitGroup
	logger            Logger
	// workerCreateCount counts the total allocations from sync.Pool.New
	// over the pool lifetime. For the number of currently active workers,
	// use GetRunningWorkersNum().
	workerCreateCount int64

	// ---- task queue (small handoff channel + chunked linked-list buffer) ----
	headChunk      *taskChunk // head of linked-list buffer (where workers pop)
	tailChunk      *taskChunk // tail of linked-list buffer (where submitters push)
	headIdx        int        // pop index within headChunk
	tailIdx        int        // push index within tailChunk
	chunkLen       int64      // total tasks across all chunks (replaces len(taskBuf))
	taskMu         sync.Mutex // protects the chunked buffer
	overflowClosed bool       // prevents spool after Close
	chunkPool      sync.Pool  // recycles consumed taskChunk nodes

	// pendingTasks counts all submitted tasks that have not yet started
	// processing. Unlike buf+channel occupancy, it includes submitters
	// blocked on p.taskQueue <- task, giving the scaler full visibility
	// into true demand regardless of buffer saturation.
	pendingTasks int64

	// ---- rate statistics for adaptive scaling ----
	submitCount  int64 // atomic, tasks submitted per window
	consumeCount int64 // atomic, tasks consumed per window
	exitCount    int64 // atomic, goroutines exited per window

	statMu      sync.Mutex
	submitHist  *histogram // submit count distribution per window
	consumeHist *histogram // consume count distribution per window
	exitHist    *histogram // exit count distribution per window
}

func NewPool(c *Config) *Pool {
	if c == nil {
		c = NewConfig()
	}
	p := &Pool{
		closePoolCn: make(chan struct{}),
		config:      c,
		lock:        &sync.Mutex{},
		logger:      log.Default(),
		capacity:    c.workerNumCapacity,
		taskQueue:   make(chan Task, internalTaskQueueCap),
	}

	switch c.idleContainerType {
	case MinHeapType:
		p.idleWorks = newMinHeap()
	case SliceType:
		p.idleWorks = newSlice()
	case RingQueueType:
		p.idleWorks = newRingQueue()
	default:
		p.idleWorks = newLinkedList()
	}

	atomic.StoreInt64(&p.workerCreateCount, 0)

	p.submitHist = newHistogram(submitBuckets, c.statsWindowSize)
	p.consumeHist = newHistogram(consumeBuckets, c.statsWindowSize)
	p.exitHist = newHistogram(exitBuckets, c.statsWindowSize)

	p.chunkPool.New = func() interface{} { return &taskChunk{} }

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

// SetLogger replaces the default standard-library logger.
// Pass the same logger instance used elsewhere in your application
// (e.g. zap.SugaredLogger) so pool output appears in the same log stream.
func (p *Pool) SetLogger(l Logger) {
	p.logger = l
}

func (p *Pool) Submit(task Task) {
	if isNilTask(task) {
		return
	}

	p.submit(context.Background(), task)
}

func (p *Pool) SubmitCtx(ctx context.Context, task Task) {
	if isNilTask(task) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}

	p.submit(ctx, &contextTask{
		ctx:  ctx,
		task: task,
	})
}

func (p *Pool) submit(ctx context.Context, task Task) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return
	}
	atomic.AddInt64(&p.submitCount, 1)
	p.wg.Add(1)
	atomic.AddInt64(&p.pendingTasks, 1)

	// Cold start: if no goroutine is running, spawn one immediately.
	if atomic.LoadInt64(&p.runningWorkersNum) == 0 {
		if atomic.CompareAndSwapInt64(&p.runningWorkersNum, 0, 1) {
			w := p.workerPool.Get().(*worker)
			go w.run(nil)
		}
	}

	if p.config.workMode == NONBLOCK {
		select {
		case p.taskQueue <- task:
		default:
			p.done()
		}
		return
	}

	// Try fast path: push to channel directly.
	select {
	case p.taskQueue <- task:
		return
	default:
	}

	// Channel full -> spool to chunked buffer.
	p.taskMu.Lock()
	if p.overflowClosed {
		p.taskMu.Unlock()
		p.done()
		return
	}

	// Backpressure: when the chunked buffer exceeds maxChunkLen, block
	// on the handoff channel instead of growing the buffer further.
	// This gives workers time to drain and bounds peak memory.
	if p.chunkLen >= maxChunkLen {
		p.taskMu.Unlock()
		p.taskQueue <- task // block until a worker picks up
		return
	}

	p.pushTail(task)
	// Forward: pop from head and try to send to handoff channel.
	// This wakes a polling worker immediately without waiting for
	// the next scaler tick. If the channel is full the task is
	// re-queued at the tail — workers drain chunks anyway.
	if t, ok := p.popHead(); ok {
		select {
		case p.taskQueue <- t:
		default:
			p.pushTail(t) // channel full, return to queue
		}
	}
	p.taskMu.Unlock()
}

// pushTail appends a task to the tail of the chunked buffer.
// Must be called with taskMu held.
func (p *Pool) pushTail(task Task) {
	if p.tailChunk == nil {
		c := p.chunkPool.Get().(*taskChunk)
		p.tailChunk = c
		p.headChunk = c
		p.tailIdx = 0
		p.headIdx = 0
	} else if p.tailIdx >= taskChunkSize {
		c := p.chunkPool.Get().(*taskChunk)
		p.tailChunk.next = c
		p.tailChunk = c
		p.tailIdx = 0
	}
	p.tailChunk.tasks[p.tailIdx] = task
	p.tailIdx++
	p.chunkLen++
}

// popHead removes and returns the task at the head of the chunked buffer.
// Returns nil, false if the buffer is empty.
// Must be called with taskMu held.
func (p *Pool) popHead() (Task, bool) {
	if p.headChunk == nil {
		return nil, false
	}
	if p.headIdx >= taskChunkSize {
		// Advance to next chunk; recycle the exhausted one.
		next := p.headChunk.next
		p.headChunk.next = nil
		p.chunkPool.Put(p.headChunk)
		p.headChunk = next
		p.headIdx = 0
		if p.headChunk == nil {
			p.tailChunk = nil
			p.tailIdx = 0
			return nil, false
		}
	}
	// Head caught up to tail in the same chunk — queue drained.
	if p.headChunk == p.tailChunk && p.headIdx >= p.tailIdx {
		p.headChunk.next = nil
		p.chunkPool.Put(p.headChunk)
		p.headChunk = nil
		p.tailChunk = nil
		p.headIdx = 0
		p.tailIdx = 0
		return nil, false
	}
	task := p.headChunk.tasks[p.headIdx]
	p.headChunk.tasks[p.headIdx] = nil // help GC
	p.headIdx++
	p.chunkLen--
	return task, true
}

type contextTask struct {
	ctx  context.Context
	task Task
}

func (t *contextTask) Process() {
	if t.ctx.Err() != nil {
		return
	}
	t.task.Process()
}

// Submits a task before the specified timeout. If timeout is reached during execution, the task is canceled.
func (p *Pool) SubmitBefore(task Task, timeout time.Duration) {
	if isNilTask(task) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	p.Submit(
		TaskFunc(func() error {
			defer cancel() // Ensures context is released after task completes to avoid resource leak
			select {
			case <-ctx.Done():
				return nil // Timeout reached, exit early
			default:
				task.Process() // Execute the task
			}
			return nil
		}),
	)
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
// the sliding-window histories.
func (p *Pool) statsSampler() {
	ticker := time.NewTicker(p.config.statsSamplePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.sampleRates()
		case <-p.closePoolCn:
			return
		}
	}
}

func (p *Pool) sampleRates() {
	sub := atomic.SwapInt64(&p.submitCount, 0)
	con := atomic.SwapInt64(&p.consumeCount, 0)
	ext := atomic.SwapInt64(&p.exitCount, 0)

	p.statMu.Lock()
	defer p.statMu.Unlock()

	p.submitHist.add(sub)
	p.consumeHist.add(con)
	p.exitHist.add(ext)
}

func (p *Pool) scaleIfNeeded() {
	submitMed, consumeMed, exitMed := p.getMedianRates()
	running := atomic.LoadInt64(&p.runningWorkersNum)

	// Read chunkLen under its lock -- still needed for bufPressure below.
	p.taskMu.Lock()
	bufDepth := p.chunkLen
	p.taskMu.Unlock()

	// totalBacklog uses pendingTasks (atomic), covering all submitted
	// tasks not yet started, including submitters blocked on the
	// handoff channel that are invisible to bufDepth + len(taskQueue).
	totalBacklog := atomic.LoadInt64(&p.pendingTasks)

	var target int64

	// Rate-based target: target = b / a = submitMed * running / consumeMed
	if running > 0 && consumeMed > 0 && submitMed > 0 {
		target = int64(submitMed * float64(running) / consumeMed)
	}

	// Backlog-weighted target: treat backlog as additional incoming tasks.
	// decayFactor is dynamically adjusted by bufPressure — when the overflow
	// buffer is deep relative to the drain rate, the scaler becomes more
	// aggressive; when it's shallow, the configured decayFactor dominates.
	if totalBacklog > 0 {
		if running == 0 {
			// Cold start: spawn enough to drain the backlog (up to capacity).
			target = totalBacklog
		} else if consumeMed > 0 {
			// bufPressure ∈ [0,1]: how many 100ms cycles needed to drain buf alone.
			bufCycles := float64(bufDepth) / consumeMed
			bufPressure := min(1.0, bufCycles*0.15)

			// dynamicDecay ∈ [decayFactor, 1.0].
			dynamicDecay := p.config.backlogDecayFactor +
				(1-p.config.backlogDecayFactor)*bufPressure

			effectiveSubmit := submitMed + float64(totalBacklog)*dynamicDecay
			qTarget := int64(effectiveSubmit * float64(running) / consumeMed)
			if qTarget > target {
				target = qTarget
			}
		}
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
		go w.run(nil)
		p.muIdle.Lock()
	}
	p.muIdle.Unlock()
}

// getMedianRates returns the median value of each counters from the histogram
// (per-statsSamplePeriod). Returns (submitMed, consumeMed, exitMed).
func (p *Pool) getMedianRates() (submitMed, consumeMed, exitMed float64) {
	p.statMu.Lock()
	defer p.statMu.Unlock()
	return p.submitHist.median(), p.consumeHist.median(), p.exitHist.median()
}

// scaler periodically checks whether the pool needs more goroutines to keep up
// with the submission rate, and spawns them proactively.
func (p *Pool) scaler() {
	ticker := time.NewTicker(p.config.scalerPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.scaleIfNeeded()
		case <-p.closePoolCn:
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

	p.taskMu.Lock()
	p.overflowClosed = true
	p.taskMu.Unlock()

	close(p.closePoolCn)
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) done() {
	atomic.AddInt64(&p.pendingTasks, -1)
	p.wg.Done()
}

func (p *Pool) GetRunningWorkersNum() int64 {
	return atomic.LoadInt64(&p.runningWorkersNum)
}

// GetWorkerCreateCount returns the total number of worker structs that have
// been allocated from sync.Pool.New over the pool's lifetime.
func (p *Pool) GetWorkerCreateCount() int64 {
	return atomic.LoadInt64(&p.workerCreateCount)
}

// GetTaskQueueLen returns the number of tasks currently queued in the
// handoff channel (taskQueue), i.e. tasks submitted but not yet picked
// up by a worker. This is a snapshot of len(taskQueue) and does not
// include tasks waiting in the chunked overflow buffer.
func (p *Pool) GetTaskQueueLen() int {
	// Returns the number of tasks that have been submitted but not yet enqueued for execution.
	return len(p.taskQueue)
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
