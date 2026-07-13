package agilepool

import "time"

// LockType defines the lock implementation for muIdle (idle worker container lock).
//
//   MutexLock  = sync.Mutex, suitable for longer or unpredictable hold times
//   SpinLock   = spin lock, suitable for very short hold times under high contention (recommended with RingQueue)
type LockType int8

const (
	MutexLock LockType = iota // sync.Mutex (default, compatible with existing behavior)
	SpinLock                  // spin lock (CAS + exponential backoff, no kernel involvement)
)

type Config struct {
	cleanPeriod        time.Duration
	taskQueueSize      int64 // capacity of the internal handoff channel
	workerNumCapacity  int64
	workMode           WorkMode
	idleContainerType  IdleContainerType
	lockType           LockType      // muIdle lock type: MutexLock or SpinLock
	statsSamplePeriod  time.Duration // sampling interval for rate stats (e.g. 100ms)
	statsWindowSize    int           // number of windows for median calculation
	scalerPeriod       time.Duration // scaler tick interval (e.g. 50ms)
	backlogDecayFactor float64       // queue backlog weight in scaler target (0-1)
}

type ConfigOption func(*Config)

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
	}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

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

func WithWorkerNumCapacity(capacity int64) ConfigOption {
	return func(c *Config) {
		if capacity > 0 {
			c.workerNumCapacity = capacity
		}
	}
}

func WithBlockMode(workMode WorkMode) ConfigOption {
	return func(c *Config) {
		c.workMode = workMode
	}
}

func WithIdleContainerType(containerType IdleContainerType) ConfigOption {
	return func(c *Config) {
		c.idleContainerType = containerType
	}
}

func WithStatsSamplePeriod(d time.Duration) ConfigOption {
	return func(c *Config) {
		if d > 0 {
			c.statsSamplePeriod = d
		}
	}
}

func WithStatsWindowSize(n int) ConfigOption {
	return func(c *Config) {
		if n > 0 {
			c.statsWindowSize = n
		}
	}
}

func WithScalerPeriod(d time.Duration) ConfigOption {
	return func(c *Config) {
		if d > 0 {
			c.scalerPeriod = d
		}
	}
}

// WithLockType sets the lock type for muIdle.
//   MutexLock — sync.Mutex (default), better performance for long hold times (avoids CPU spinning)
//   SpinLock  — spin lock, higher throughput for very short hold times under high contention (eliminates kernel switching overhead)
// If not called, defaults to MutexLock.
func WithLockType(lockType LockType) ConfigOption {
	return func(c *Config) {
		c.lockType = lockType
	}
}

func WithBacklogDecayFactor(factor float64) ConfigOption {
	return func(c *Config) {
		if factor >= 0 && factor <= 1 {
			c.backlogDecayFactor = factor
		}
	}
}

