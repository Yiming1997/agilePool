package agilepool

import "time"

type Config struct {
	cleanPeriod        time.Duration
	taskQueueSize      int64 // retained for API compat; internal channel cap is fixed at 64
	workerNumCapacity  int64
	workMode           WorkMode
	idleContainerType  IdleContainerType
	statsSamplePeriod  time.Duration // sampling interval for rate stats (e.g. 100ms)
	statsWindowSize    int           // number of windows for median calculation
	scalerPeriod       time.Duration // scaler tick interval (e.g. 50ms)
	backlogDecayFactor float64       // queue backlog weight in scaler target (0-1)
	sampleRate         float32       // metrics 采样率, 0.0~1.0; 1.0=100%记录
}

type ConfigOption func(*Config)

func NewConfig(opts ...ConfigOption) *Config {
	config := &Config{
		cleanPeriod:        defaultCleanPeriod,
		taskQueueSize:      defaultTaskQueueSize,
		workerNumCapacity:  defaultMaxWorkerNumCapacity,
		workMode:           defaultWorkMode,
		idleContainerType:  defaultIdleContainerType,
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

func WithBacklogDecayFactor(factor float64) ConfigOption {
	return func(c *Config) {
		if factor >= 0 && factor <= 1 {
			c.backlogDecayFactor = factor
		}
	}
}

// WithSampleRate 设置 metrics 采样率。
// sampleRate 取值范围 (0.0, 1.0], 1.0 表示 100% 采样, 0.5 表示约 50% 采样。
// 在 worker 执行完任务后,会按此比例将耗时指标记录到 Prometheus Histogram 中。
func WithSampleRate(sampleRate float32) ConfigOption {
	return func(c *Config) {
		if sampleRate > 0 {
			c.sampleRate = sampleRate
		}
	}
}
