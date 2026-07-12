package agilepool

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	SubmittedAt = "agile_submitted_at"
	EnqueuedAt  = "agile_enqueued_at"
	StartedAt   = "agile_started_at"
	CompletedAt = "agile_completed_at"
)

// BuiltinMetrics groups all built-in Prometheus metrics for convenient
// batch access or registration. Users may define their own metrics and
// combine them via the Collect* callbacks below.
type BuiltinMetrics struct {
	// ---- Gauges: pool runtime state ----
	CapacityGauge      *prometheus.GaugeVec
	RunningGauge       prometheus.Gauge
	IdleGauge          prometheus.Gauge
	QueueLenGauge      prometheus.Gauge
	WorkerCreatedGauge prometheus.Gauge

	// ---- Histograms: per-task latency ----
	TaskDurationHistogram prometheus.Histogram
	QueueWaitHistogram    prometheus.Histogram
	EnqueueWaitHistogram  prometheus.Histogram
	TotalTimeHistogram    prometheus.Histogram
}

var builtinMetrics = newBuiltinMetrics()

func newBuiltinMetrics() *BuiltinMetrics {
	return &BuiltinMetrics{
		CapacityGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "agilepool_pool_capacity",
				Help: "Pool capacity (max workers), labeled by pool address.",
			},
			[]string{"pool"},
		),
		RunningGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "agilepool_running_workers",
				Help: "Current number of running (active) workers.",
			},
		),
		IdleGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "agilepool_idle_workers",
				Help: "Current number of idle (parked) workers.",
			},
		),
		QueueLenGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "agilepool_task_queue_length",
				Help: "Current number of tasks waiting in the handoff channel.",
			},
		),
		WorkerCreatedGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "agilepool_workers_created_total",
				Help: "Total number of worker structs allocated since pool creation.",
			},
		),
		TaskDurationHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "agilepool_task_duration_seconds",
				Help:    "Distribution of task execution durations (start to completion).",
				Buckets: prometheus.DefBuckets,
			},
		),
		QueueWaitHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "agilepool_task_queue_wait_seconds",
				Help:    "Distribution of time tasks spend waiting in the queue before execution.",
				Buckets: prometheus.DefBuckets,
			},
		),
		EnqueueWaitHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "agilepool_task_enqueue_wait_seconds",
				Help:    "Distribution of time from Submit() call to entering the handoff channel.",
				Buckets: prometheus.DefBuckets,
			},
		),
		TotalTimeHistogram: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "agilepool_task_total_time_seconds",
				Help:    "Distribution of end-to-end task time (Submit() to completed execution).",
				Buckets: prometheus.DefBuckets,
			},
		),
	}
}

// GetBuiltinMetrics returns the built-in Prometheus metric set for direct
// access or registration.
func GetBuiltinMetrics() *BuiltinMetrics {
	return builtinMetrics
}

// RegisterBuiltinMetrics registers all built-in metrics with the given
// registerer (typically prometheus.DefaultRegisterer or a custom Registry).
func RegisterBuiltinMetrics(reg prometheus.Registerer) {
	reg.MustRegister(
		builtinMetrics.CapacityGauge,
		builtinMetrics.RunningGauge,
		builtinMetrics.IdleGauge,
		builtinMetrics.QueueLenGauge,
		builtinMetrics.WorkerCreatedGauge,
		builtinMetrics.TaskDurationHistogram,
		builtinMetrics.QueueWaitHistogram,
		builtinMetrics.EnqueueWaitHistogram,
		builtinMetrics.TotalTimeHistogram,
	)
}

// UnregisterBuiltinMetrics removes all built-in metrics from the given
// registerer to avoid duplicate registration on reload.
func UnregisterBuiltinMetrics(reg prometheus.Registerer) {
	reg.Unregister(builtinMetrics.CapacityGauge)
	reg.Unregister(builtinMetrics.RunningGauge)
	reg.Unregister(builtinMetrics.IdleGauge)
	reg.Unregister(builtinMetrics.QueueLenGauge)
	reg.Unregister(builtinMetrics.WorkerCreatedGauge)
	reg.Unregister(builtinMetrics.TaskDurationHistogram)
	reg.Unregister(builtinMetrics.QueueWaitHistogram)
	reg.Unregister(builtinMetrics.EnqueueWaitHistogram)
	reg.Unregister(builtinMetrics.TotalTimeHistogram)
}

// PrometheusCollect returns an OnCollect callback that refreshes the
// built-in Prometheus Gauges from the given pool. CapacityGauge is
// additionally labeled with the pool's address.
//
//	opt.OnCollect = append(opt.OnCollect, agilepool.PrometheusCollect(pool))
func PrometheusCollect(p *Pool) func(p2 *Pool) {
	return func(_ *Pool) {
		builtinMetrics.RunningGauge.Set(float64(p.GetRunningWorkersNum()))
		builtinMetrics.IdleGauge.Set(float64(p.GetIdleWorkerCount()))
		builtinMetrics.QueueLenGauge.Set(float64(p.GetTaskQueueLen()))
		builtinMetrics.WorkerCreatedGauge.Set(float64(p.GetWorkerCreateCount()))
		builtinMetrics.CapacityGauge.WithLabelValues(
			fmt.Sprintf("%p", p),
		).Set(float64(p.GetCapacity()))
	}
}

// Option configures a Taker's periodic collection.
type Option struct {
	LoopTime time.Duration

	Pool []*Pool

	// OnCollect is invoked once per cycle for each non-nil pool.
	// If nil, Taker still runs to clean up nil entries.
	OnCollect []func(p *Pool)
}

// CollectStats is a point-in-time snapshot of all pool metrics.
type CollectStats struct {
	Capacity          int64
	RunningWorkersNum int64
	IdleWorkerCount   int64
	TaskQueueLen      int
	WorkerCreateCount int64
}

// CollectRunningWorkers returns an OnCollect callback that reports the
// number of currently running workers via fn. Useful for worker-level
// monitoring.
func CollectRunningWorkers(fn func(running int64)) func(p *Pool) {
	return func(p *Pool) {
		fn(p.GetRunningWorkersNum())
	}
}

// CollectQueueLen returns an OnCollect callback that reports the current
// task queue length via fn. Useful for backlog monitoring.
//
//	opt.OnCollect = append(opt.OnCollect, agilepool.CollectQueueLen(func(n int) {
//	    backlogGauge.Set(float64(n))
//	}))
func CollectQueueLen(fn func(queueLen int)) func(p *Pool) {
	return func(p *Pool) {
		fn(p.GetTaskQueueLen())
	}
}

// CollectIdleWorkers returns an OnCollect callback that reports the
// current number of idle workers via fn. Useful for spotting idle waste.
//
//	opt.OnCollect = append(opt.OnCollect, agilepool.CollectIdleWorkers(func(n int64) {
//	    idleGauge.Set(float64(n))
//	}))
func CollectIdleWorkers(fn func(idle int64)) func(p *Pool) {
	return func(p *Pool) {
		fn(p.GetIdleWorkerCount())
	}
}

// CollectUtilization returns an OnCollect callback that reports the pool
// utilization (running / capacity) in [0.0, 1.0] via fn. 1.0 means fully
// loaded; capacity==0 reports 0 to avoid division by zero.
//
//	opt.OnCollect = append(opt.OnCollect, agilepool.CollectUtilization(func(r float64) {
//	    utilGauge.Set(r)
//	}))
func CollectUtilization(fn func(utilization float64)) func(p *Pool) {
	return func(p *Pool) {
		cap := p.GetCapacity()
		if cap == 0 {
			fn(0)
			return
		}
		fn(float64(p.GetRunningWorkersNum()) / float64(cap))
	}
}

// LoggerCollect returns an OnCollect callback that prints pool metrics
// via the given Logger. Useful for quick debugging without a metrics stack.
//
//	opt.OnCollect = append(opt.OnCollect, agilepool.LoggerCollect(poolLogger))
func LoggerCollect(logger Logger) func(p *Pool) {
	return func(p *Pool) {
		logger.Printf("[pool collect] capacity=%d running=%d idle=%d queue=%d created=%d",
			p.GetCapacity(), p.GetRunningWorkersNum(),
			p.GetIdleWorkerCount(), p.GetTaskQueueLen(),
			p.GetWorkerCreateCount())
	}
}

// Taker periodically invokes OnCollect on every non-nil pool in option.Pool
// and automatically drops nil entries from the slice. LoopTime controls
// the interval between cycles.
//
//	opt := agilepool.Option{
//	    LoopTime: 5 * time.Second,
//	    Pool:     []*agilepool.Pool{myPool},
//	    OnCollect: []func(p *agilepool.Pool){
//	        agilepool.PrometheusCollect(myPool),
//	    },
//	}
//	go agilepool.Taker(opt)
func Taker(option Option) {
	for {
		for i := 0; i < len(option.Pool); {
			if len(option.Pool) == 0 {
				return
			}
			p := option.Pool[i]
			if p == nil {
				option.Pool = append(option.Pool[:i], option.Pool[i+1:]...)
				continue
			}
			for _, collect := range option.OnCollect {
				collect(p)
			}
			i++
		}
		time.Sleep(option.LoopTime)
	}
}

// SendByContext reads four timestamps (SubmittedAt, EnqueuedAt, StartedAt,
// CompletedAt) from ctx, computes per-stage latencies, and records them
// into the built-in Prometheus Histograms with the given sample rate.
// sampleRate >= 1.0 means record every task.
func SendByContext(ctx context.Context, sampleRate float32) {
	if sampleRate < 1.0 && rand.Float32() >= sampleRate {
		return
	}

	submitted, ok := ctx.Value(SubmittedAt).(time.Time)
	if !ok || submitted.IsZero() {
		return
	}
	completed, ok := ctx.Value(CompletedAt).(time.Time)
	if !ok || completed.IsZero() {
		return
	}
	enqueued, enqueuedOk := ctx.Value(EnqueuedAt).(time.Time)
	started, startedOk := ctx.Value(StartedAt).(time.Time)

	// Total: Submit -> Complete.
	builtinMetrics.TotalTimeHistogram.Observe(completed.Sub(submitted).Seconds())

	// Enqueue wait: Submit -> Enqueue.
	if enqueuedOk && !enqueued.IsZero() {
		builtinMetrics.EnqueueWaitHistogram.Observe(enqueued.Sub(submitted).Seconds())
	}

	// Queue wait: Enqueue -> Start.
	if enqueuedOk && !enqueued.IsZero() && startedOk && !started.IsZero() {
		builtinMetrics.QueueWaitHistogram.Observe(started.Sub(enqueued).Seconds())
	}

	// Run time: Start -> Complete.
	if startedOk && !started.IsZero() {
		builtinMetrics.TaskDurationHistogram.Observe(completed.Sub(started).Seconds())
	}
}

// NOTE: send, Send and SendWithRand are PRELIMINARY APIs and are not
// yet ready for use. They will be revised once the timing API stabilizes.
// send records the elapsed time since startTime into the built-in
// TaskDurationHistogram at the given sample rate. Internal helper.
func send(startTime time.Time, sampleRate float32) {
	if sampleRate >= 1.0 || rand.Float32() < sampleRate {
		builtinMetrics.TaskDurationHistogram.Observe(time.Since(startTime).Seconds())
	}
}

// NOTE: send, Send and SendWithRand are PRELIMINARY APIs and are not
// yet ready for use. They will be revised once the timing API stabilizes.
// Send records the elapsed time since startTime into the built-in
// TaskDurationHistogram (100% sampling). Typical use via defer:
//
//	func myTask() {
//	    defer agilepool.Send(time.Now())
//	    // task body...
//	}
func Send(startTime time.Time) {
	send(startTime, 1.0)
}

// NOTE: send, Send and SendWithRand are PRELIMINARY APIs and are not
// yet ready for use. They will be revised once the timing API stabilizes.
// SendWithRand records the elapsed time since startTime into the built-in
// TaskDurationHistogram with the given sample rate in (0.0, 1.0].
// Typical use via defer:
//
//	func myTask() {
//	    defer agilepool.SendWithRand(time.Now(), 0.32)
//	    // task body...
//	}
func SendWithRand(startTime time.Time, sampleRate float32) {
	send(startTime, sampleRate)
}
