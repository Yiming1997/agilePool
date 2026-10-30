package agilepool

import (
	"time"

	"github.com/Yiming1997/agilePool/v2/internal/idle"
)

// IdleContainerType is an alias for the idle container selector defined in
// internal/idle. It is re-exported here so that library consumers can keep
// using agilepool.LinkedListType, agilepool.MinHeapType, etc.
type IdleContainerType = idle.IdleContainerType

// The idle container type constants are re-exported from internal/idle to
// preserve the public API of the package.
const (
	LinkedListType = idle.LinkedListType
	MinHeapType    = idle.MinHeapType
	SliceType      = idle.SliceType
	RingQueueType  = idle.RingQueueType
	TreapType      = idle.TreapType
)

// LockType defines the lock implementation for muIdle (idle worker container lock).
//
//	MutexLock  = sync.Mutex, suitable for longer or unpredictable hold times
//	SpinLock   = spin lock, suitable for very short hold times under high contention (recommended with RingQueue)
type LockType int8

// Supported lock implementations for the idle-worker container.
const (
	MutexLock LockType = iota // sync.Mutex (default, compatible with existing behavior)
	SpinLock                  // spin lock (CAS + exponential backoff, no kernel involvement)
)

// Config holds the tunable parameters of a Pool. Build one with NewConfig and
// the With* options; the zero value is not intended for direct use.
type Config struct {
	cleanPeriod        time.Duration
	taskQueueSize      int64 // total capacity of the internal handoff queues
	taskQueueShards    int   // number of handoff-queue shards; 0 = auto
	workerNumCapacity  int64
	workMode           WorkMode
	idleContainerType  IdleContainerType
	lockType           LockType      // muIdle lock type: MutexLock or SpinLock
	statsSamplePeriod  time.Duration // sampling interval for rate stats (e.g. 100ms)
	statsWindowSize    int           // number of windows for median calculation
	scalerPeriod       time.Duration // scaler tick interval (e.g. 50ms)
	backlogDecayFactor float64       // queue backlog weight in scaler target (0-1)
	// zeroConsumeScaleInterval and maxZeroConsumeScaleStep tune the fallback the
	// scaler uses when queued work exists but consumption is observed as zero,
	// for example right after an idle period (see Pool.scaleIfNeeded).
	zeroConsumeScaleInterval time.Duration
	maxZeroConsumeScaleStep  int64
	panicHandler             PanicHandler // custom task-panic handler (nil keeps default logging)
}

// ConfigOption mutates a Config while NewConfig is building it.
type ConfigOption func(*Config)

// PanicHandler is invoked when a task panics while running inside a worker.
// Setting one with WithPanicHandler replaces the pool's default behaviour of
// logging the recovered value and stack trace, letting callers route panics
// to their own error reporting.
//
//   - task:      the task whose execution panicked
//   - recovered: the value passed to panic
//   - stack:     formatted call stack captured at the recovery point
//
// A nil handler keeps the default logging behaviour. A handler that panics is
// itself recovered and logged, so it can never crash the worker goroutine or
// skip task-completion bookkeeping; see Pool.handlePanic.
type PanicHandler func(task Task, recovered any, stack []byte)

// NewConfig returns a Config populated with defaults and applies opts in order.
func NewConfig(opts ...ConfigOption) *Config {
	config := &Config{
		cleanPeriod:        defaultCleanPeriod,
		taskQueueSize:      defaultTaskQueueSize,
		workerNumCapacity:  defaultMaxWorkerNumCapacity,
		workMode:           defaultWorkMode,
		idleContainerType:  defaultIdleContainerType,
		lockType:           MutexLock, // default to sync.Mutex, compatible with existing behavior
		statsSamplePeriod:  defaultStatsSamplePeriod,
		statsWindowSize:    defaultStatsWindowSize,
		scalerPeriod:       defaultScalerPeriod,
		backlogDecayFactor: defaultBacklogDecayFactor,

		zeroConsumeScaleInterval: defaultZeroConsumeScaleInterval,
		maxZeroConsumeScaleStep:  defaultMaxZeroConsumeScaleStep,
	}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// WithCleanPeriod sets how often the idle-worker cleaner runs. Non-positive
// values are ignored.
func WithCleanPeriod(duration time.Duration) ConfigOption {
	return func(c *Config) {
		if duration > 0 {
			c.cleanPeriod = duration
		}
	}
}

// WithTaskQueueSize sets the capacity of the internal handoff channel.
// Tasks beyond this channel capacity are stored in the dynamic chunked buffer
// according to the configured work mode and backpressure rules.
func WithTaskQueueSize(size int64) ConfigOption {
	return func(c *Config) {
		if size > 0 {
			c.taskQueueSize = size
		}
	}
}

// WithTaskQueueShards sets how many shards the internal handoff queue is split
// into. Sharding spreads channel-lock contention across multiple channel locks,
// which matters when the pool runs a very large number of workers that would
// otherwise serialise on a single channel. The value is rounded up to a power
// of two and each shard gets at least one slot. n <= 0 selects a default: a
// single shard for small queues and GOMAXPROCS (capped) for large ones.
func WithTaskQueueShards(n int) ConfigOption {
	return func(c *Config) {
		c.taskQueueShards = n
	}
}

// WithWorkerNumCapacity caps the maximum number of concurrent workers.
// Non-positive values are ignored, leaving the default unbounded capacity.
func WithWorkerNumCapacity(capacity int64) ConfigOption {
	return func(c *Config) {
		if capacity > 0 {
			c.workerNumCapacity = capacity
		}
	}
}

// WithBlockMode selects the submission policy when the pool is saturated:
// BLOCK waits for capacity, NONBLOCK rejects tasks that cannot be accepted
// immediately.
func WithBlockMode(workMode WorkMode) ConfigOption {
	return func(c *Config) {
		c.workMode = workMode
	}
}

// WithIdleContainerType selects the idle-worker container implementation:
// LinkedListType, MinHeapType, SliceType, RingQueueType or TreapType.
func WithIdleContainerType(containerType IdleContainerType) ConfigOption {
	return func(c *Config) {
		c.idleContainerType = containerType
	}
}

// WithStatsSamplePeriod sets the interval at which submit, consume and exit
// rates are sampled for adaptive scaling. Non-positive values are ignored.
func WithStatsSamplePeriod(d time.Duration) ConfigOption {
	return func(c *Config) {
		if d > 0 {
			c.statsSamplePeriod = d
		}
	}
}

// WithStatsWindowSize sets how many recent rate samples the scaler keeps in
// its histograms. Non-positive values are ignored.
func WithStatsWindowSize(n int) ConfigOption {
	return func(c *Config) {
		if n > 0 {
			c.statsWindowSize = n
		}
	}
}

// WithScalerPeriod sets how often the scaler adjusts the worker count.
// Non-positive values are ignored.
func WithScalerPeriod(d time.Duration) ConfigOption {
	return func(c *Config) {
		if d > 0 {
			c.scalerPeriod = d
		}
	}
}

// WithLockType sets the lock type for muIdle.
//
//	MutexLock — sync.Mutex (default), better performance for long hold times (avoids CPU spinning)
//	SpinLock  — spin lock, higher throughput for very short hold times under high contention (eliminates kernel switching overhead)
//
// If not called, defaults to MutexLock.
func WithLockType(lockType LockType) ConfigOption {
	return func(c *Config) {
		c.lockType = lockType
	}
}

// WithBacklogDecayFactor weights the queued backlog in the scaler's target
// worker count. Valid values are in [0, 1]; anything else is ignored.
func WithBacklogDecayFactor(factor float64) ConfigOption {
	return func(c *Config) {
		if factor >= 0 && factor <= 1 {
			c.backlogDecayFactor = factor
		}
	}
}

// WithZeroConsumeScaleInterval sets the minimum interval between two growth
// steps of the zero-consume fallback. The fallback applies when the consume
// median over the statistics window is zero while tasks are queued — typically
// a burst of long-running tasks submitted after an idle period — and grows the
// pool by one bounded step per interval instead of staying at a single worker.
// Non-positive values are ignored.
func WithZeroConsumeScaleInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		if d > 0 {
			c.zeroConsumeScaleInterval = d
		}
	}
}

// WithMaxZeroConsumeScaleStep caps how many workers a single zero-consume growth
// step may add, bounding the ramp-up of the fallback described in
// WithZeroConsumeScaleInterval. Non-positive values are ignored.
func WithMaxZeroConsumeScaleStep(n int64) ConfigOption {
	return func(c *Config) {
		if n > 0 {
			c.maxZeroConsumeScaleStep = n
		}
	}
}

// WithPanicHandler installs a custom handler invoked when a task panics inside
// a worker, replacing the default logger-based reporting. Pass nil to keep the
// default behaviour.
func WithPanicHandler(handler PanicHandler) ConfigOption {
	return func(c *Config) {
		c.panicHandler = handler
	}
}
