package agilepool_test

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- BuiltinMetrics register / unregister ----

func TestGetBuiltinMetrics_ReturnsNonNil(t *testing.T) {
	m := agilepool.GetBuiltinMetrics()
	require.NotNil(t, m)
	require.NotNil(t, m.CapacityGauge)
	require.NotNil(t, m.RunningGauge)
	require.NotNil(t, m.IdleGauge)
	require.NotNil(t, m.QueueLenGauge)
	require.NotNil(t, m.WorkerCreatedGauge)
	require.NotNil(t, m.TaskDurationHistogram)
	require.NotNil(t, m.QueueWaitHistogram)
	require.NotNil(t, m.EnqueueWaitHistogram)
	require.NotNil(t, m.TotalTimeHistogram)
}

func TestRegisterAndUnregisterBuiltinMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	agilepool.RegisterBuiltinMetrics(reg)
	_, err := reg.Gather()
	assert.NoError(t, err, "Gather should succeed after registration")

	agilepool.UnregisterBuiltinMetrics(reg)
	mfs, err := reg.Gather()
	assert.NoError(t, err)
	assert.Empty(t, mfs, "no metric families should remain after unregister")
}

func TestRegisterBuiltinMetrics_IdempotentPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)
	// MustRegister rejects duplicates, so a second call must panic.
	assert.Panics(t, func() {
		agilepool.RegisterBuiltinMetrics(reg)
	})
}

// ---- Collect callbacks ----

func TestPrometheusCollect_UpdatesGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)

	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(10),
	))
	defer p.Close()

	collect := agilepool.PrometheusCollect(p)
	collect(p)

	err := testutil.CollectAndCompare(agilepool.GetBuiltinMetrics().RunningGauge,
		bytes.NewBufferString(`
# HELP agilepool_running_workers Current number of running (active) workers.
# TYPE agilepool_running_workers gauge
agilepool_running_workers 0
`))
	assert.NoError(t, err)
}

func TestCollectRunningWorkers(t *testing.T) {
	var got int64
	fn := agilepool.CollectRunningWorkers(func(running int64) {
		got = running
	})

	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()
	fn(p)

	assert.Equal(t, int64(0), got, "new pool should have 0 running workers")
}

func TestCollectQueueLen(t *testing.T) {
	var got int
	fn := agilepool.CollectQueueLen(func(queueLen int) {
		got = queueLen
	})

	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()
	fn(p)

	assert.Equal(t, 0, got, "new pool should have empty queue")
}

func TestCollectIdleWorkers(t *testing.T) {
	var got int64
	fn := agilepool.CollectIdleWorkers(func(idle int64) {
		got = idle
	})

	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()
	fn(p)

	assert.Equal(t, int64(0), got, "new pool should have 0 idle workers")
}

func TestCollectUtilization_NewPool(t *testing.T) {
	var ratio float64
	fn := agilepool.CollectUtilization(func(u float64) {
		ratio = u
	})

	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(10),
	))
	defer p.Close()
	fn(p)

	assert.Equal(t, float64(0), ratio, "utilization of idle pool should be 0")
}

func TestCollectUtilization_ZeroCapacity(t *testing.T) {
	var ratio float64
	fn := agilepool.CollectUtilization(func(u float64) {
		ratio = u
	})

	// capacity=0 -> ratio must be 0 (avoid division by zero).
	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(0),
	))
	defer p.Close()
	fn(p)

	assert.Equal(t, float64(0), ratio)
}

func TestLoggerCollect(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	fn := agilepool.LoggerCollect(logger)
	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()
	fn(p)

	assert.Contains(t, buf.String(), "[pool collect]")
	assert.Contains(t, buf.String(), "capacity=")
	assert.Contains(t, buf.String(), "running=")
}

// ---- Send / SendWithRand ----

func TestSend_RecordsDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)

	start := time.Now()
	agilepool.Send(start)

	cnt, err := testutil.GatherAndCount(reg)
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0, "should have at least one metric after Send")
}

func TestSendWithRand_SampleRate1_AlwaysRecords(t *testing.T) {
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)

	start := time.Now()
	// sampleRate=1.0 -> 100% sampling.
	for i := 0; i < 100; i++ {
		agilepool.SendWithRand(start, 1.0)
	}

	cnt, err := testutil.GatherAndCount(reg)
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0, "sampleRate=1 should always record")
}

// ---- SendByContext ----

func TestSendByContext_MissingTimestamps(t *testing.T) {
	// Context without any timestamps must return safely without panicking.
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)

	assert.NotPanics(t, func() {
		agilepool.SendByContext(context.Background(), 1.0)
	}, "SendByContext with empty context should not panic")
}

func TestSendByContext_OnlySubmittedTimestamp(t *testing.T) {
	// Only SubmittedAt set, no CompletedAt -> safe no-op.
	ctx := context.WithValue(context.Background(), agilepool.SubmittedAt, time.Now())

	assert.NotPanics(t, func() {
		agilepool.SendByContext(ctx, 1.0)
	})
}

func TestSendByContext_FullTimestamps(t *testing.T) {
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)

	now := time.Now()
	ctx := context.WithValue(context.Background(), agilepool.SubmittedAt, now)
	ctx = context.WithValue(ctx, agilepool.EnqueuedAt, now.Add(10*time.Millisecond))
	ctx = context.WithValue(ctx, agilepool.StartedAt, now.Add(20*time.Millisecond))
	ctx = context.WithValue(ctx, agilepool.CompletedAt, now.Add(100*time.Millisecond))

	assert.NotPanics(t, func() {
		agilepool.SendByContext(ctx, 1.0)
	})

	cnt, err := testutil.GatherAndCount(reg)
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0)
}

func TestSendByContext_SampleRateZero(t *testing.T) {
	// sampleRate=0 must not panic and must return safely. Whether anything
	// is recorded depends on rand, so we only assert no crash here.
	now := time.Now()
	ctx := context.WithValue(context.Background(), agilepool.SubmittedAt, now)
	ctx = context.WithValue(ctx, agilepool.CompletedAt, now.Add(100*time.Millisecond))

	assert.NotPanics(t, func() {
		agilepool.SendByContext(ctx, 0)
	})
}

// ---- Taker ----

func TestTaker_CollectsMetrics(t *testing.T) {
	var mu sync.Mutex
	var collected int

	collectFn := func(p *agilepool.Pool) {
		mu.Lock()
		collected++
		mu.Unlock()
	}

	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()

	opt := agilepool.Option{
		LoopTime: 10 * time.Millisecond,
		Pool:     []*agilepool.Pool{p},
		OnCollect: []func(p *agilepool.Pool){
			collectFn,
		},
	}

	go agilepool.Taker(opt)
	time.Sleep(25 * time.Millisecond) // at least 2 collection cycles

	mu.Lock()
	count := collected
	mu.Unlock()
	assert.GreaterOrEqual(t, count, 2, "Taker should collect at least 2 times")
}

func TestTaker_NilPoolCleanup(t *testing.T) {
	var mu sync.Mutex
	var collected int

	collectFn := func(p *agilepool.Pool) {
		mu.Lock()
		collected++
		mu.Unlock()
	}

	p := agilepool.NewPool(agilepool.NewConfig())
	defer p.Close()

	opt := agilepool.Option{
		LoopTime: 10 * time.Millisecond,
		Pool:     []*agilepool.Pool{p, nil},
		OnCollect: []func(p *agilepool.Pool){
			collectFn,
		},
	}

	go agilepool.Taker(opt)
	time.Sleep(25 * time.Millisecond)

	// Only the non-nil pool should be collected.
	mu.Lock()
	count := collected
	mu.Unlock()
	assert.GreaterOrEqual(t, count, 2)
}

// ---- SendWithRand sampling ----

func TestSendWithRand_FullCoverage(t *testing.T) {
	reg := prometheus.NewRegistry()
	agilepool.RegisterBuiltinMetrics(reg)

	start := time.Now()
	const n = 1000
	for i := 0; i < n; i++ {
		agilepool.SendWithRand(start, 1.0)
	}

	mfs, err := reg.Gather()
	assert.NoError(t, err)
	assert.NotEmpty(t, mfs)
}
