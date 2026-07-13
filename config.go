package agilepool

import "time"

// LockType 定义 muIdle（空闲 worker 容器锁）的锁实现类型。
//
//   MutexLock  = sync.Mutex，适用于持锁时间较长或不确定的场景
//   SpinLock   = 自旋锁，适用于持锁极短但争用频繁的场景（推荐搭配 RingQueue）
type LockType int8

const (
	MutexLock LockType = iota // sync.Mutex（默认，兼容现有行为）
	SpinLock                  // 自旋锁（CAS + 指数退避，不经过内核）
)

type Config struct {
	cleanPeriod        time.Duration
	taskQueueSize      int64 // retained for API compat; internal channel cap is fixed at 64
	workerNumCapacity  int64
	workMode           WorkMode
	idleContainerType  IdleContainerType
	lockType           LockType      // muIdle 锁类型：MutexLock 或 SpinLock
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
		lockType:           MutexLock, // 默认用 sync.Mutex，兼容现有行为
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

// WithTaskQueueSize is retained for backward compatibility.
// The internal handoff channel capacity is now fixed (64 slots) and the
// primary queue (taskBuf) grows dynamically on demand, so the queue-size
// setting has no effect on pre-allocated memory or scaler behaviour.
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

// WithLockType 设置 muIdle 的锁类型。
//   MutexLock — sync.Mutex（默认），持锁时间长时性能更好（避免 CPU 空转）
//   SpinLock  — 自旋锁，持锁极短 + 高竞争时吞吐更高（消除内核切换开销）
// 如果不调用此函数，默认使用 MutexLock。
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

