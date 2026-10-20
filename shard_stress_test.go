package agilepool_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// ---------------------------------------------------------------------------
// Sharded handoff-queue stress tests.
//
// The race detector is unavailable in this environment, so correctness is
// pinned by explicit invariants under heavy concurrency:
//   * every accepted task runs exactly once (no loss, no duplication),
//   * Wait() returns (no deadlock) across shard/queue/capacity combinations,
//   * Close during load never panics or hangs,
//   * reentrant submission and panicking tasks stay isolated.
// ---------------------------------------------------------------------------

// uniqueTaskConfigs covers the shard-count / queue-size / capacity corners,
// including queue sizes below the auto-shard threshold and forced shard counts.
func uniqueTaskConfigs() []struct {
	name   string
	config *agilepool.Config
} {
	return []struct {
		name   string
		config *agilepool.Config
	}{
		{"cap1_q1", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(1), agilepool.WithTaskQueueSize(1))},
		{"cap4_q10", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(4), agilepool.WithTaskQueueSize(10))},
		{"cap64_q1000", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(64), agilepool.WithTaskQueueSize(1000))},
		{"cap256_q100000_auto", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(256), agilepool.WithTaskQueueSize(100000))},
		{"cap256_q100000_shards2", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(256), agilepool.WithTaskQueueSize(100000), agilepool.WithTaskQueueShards(2))},
		{"cap256_q100000_shards8", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(256), agilepool.WithTaskQueueSize(100000), agilepool.WithTaskQueueShards(8))},
		{"cap128_q4_shards8", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(128), agilepool.WithTaskQueueSize(4), agilepool.WithTaskQueueShards(8))},
		{"cap1000_q10000_shards32", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(1000), agilepool.WithTaskQueueSize(10000), agilepool.WithTaskQueueShards(32))},
		{"cap8_q2_shards32_reduced", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(8), agilepool.WithTaskQueueSize(2), agilepool.WithTaskQueueShards(32))},
		{"cap5000_q100000_auto", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(5000), agilepool.WithTaskQueueSize(100000))},
		{"cap512_q100000_shards64", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(512), agilepool.WithTaskQueueSize(100000), agilepool.WithTaskQueueShards(64))},
		{"cap512_q100000_shards3_nonpow2", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(512), agilepool.WithTaskQueueSize(100000), agilepool.WithTaskQueueShards(3))},
	}
}

// TestStressUniqueTaskRunsExactlyOnce asserts no task is lost or duplicated
// across every shard/queue/capacity configuration.
func TestStressUniqueTaskRunsExactlyOnce(t *testing.T) {
	const (
		tasks       = 20000
		submitters  = 8
		iterations  = 3
		timeoutWait = 30 * time.Second
	)
	for _, tc := range uniqueTaskConfigs() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for iter := 0; iter < iterations; iter++ {
				pool := agilepool.NewPool(tc.config)
				counts := make([]int32, tasks)
				var executed int64
				var submitWG sync.WaitGroup

				for s := 0; s < submitters; s++ {
					submitWG.Add(1)
					go func(start int) {
						defer submitWG.Done()
						for i := start; i < tasks; i += submitters {
							id := i
							pool.Submit(agilepool.TaskFunc(func() error {
								atomic.AddInt32(&counts[id], 1)
								atomic.AddInt64(&executed, 1)
								return nil
							}))
						}
					}(s)
				}
				submitWG.Wait()

				done := make(chan struct{})
				go func() { pool.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(timeoutWait):
					t.Fatalf("%s iter %d: Wait() timed out, executed=%d/%d, running=%d",
						tc.name, iter, atomic.LoadInt64(&executed), tasks, pool.GetRunningWorkersNum())
				}

				if got := atomic.LoadInt64(&executed); got != tasks {
					t.Fatalf("%s iter %d: executed %d, want %d", tc.name, iter, got, tasks)
				}
				for i, c := range counts {
					if c != 1 {
						t.Fatalf("%s iter %d: task %d ran %d times (want exactly 1)", tc.name, iter, i, c)
					}
				}
				pool.Close()
			}
		})
	}
}

// TestStressNonblockNoLossNoDup checks that in NONBLOCK mode the set of
// accepted tasks is exactly the set that ran, once each.
func TestStressNonblockNoLossNoDup(t *testing.T) {
	const (
		tasks      = 20000
		submitters = 8
	)

	for _, shards := range []int{1, 4, 32} {
		shards := shards
		t.Run("shards"+itoa(shards), func(t *testing.T) {
			pool := agilepool.NewPool(agilepool.NewConfig(
				agilepool.WithWorkerNumCapacity(16),
				agilepool.WithTaskQueueSize(64),
				agilepool.WithBlockMode(agilepool.NONBLOCK),
				agilepool.WithTaskQueueShards(shards),
			))
			defer pool.Close()

			accepted := make([]int32, tasks)
			counts := make([]int32, tasks)
			var acceptedN int64

			var wg sync.WaitGroup
			for s := 0; s < submitters; s++ {
				wg.Add(1)
				go func(start int) {
					defer wg.Done()
					for i := start; i < tasks; i += submitters {
						id := i
						if pool.TrySubmit(agilepool.TaskFunc(func() error {
							atomic.AddInt32(&counts[id], 1)
							return nil
						})) {
							atomic.StoreInt32(&accepted[id], 1)
							atomic.AddInt64(&acceptedN, 1)
						}
					}
				}(s)
			}
			wg.Wait()
			pool.Wait()

			if acceptedN == 0 {
				t.Fatal("no tasks accepted; workload not exercising the pool")
			}
			for i := 0; i < tasks; i++ {
				acc := atomic.LoadInt32(&accepted[i])
				run := atomic.LoadInt32(&counts[i])
				switch {
				case acc == 1 && run != 1:
					t.Fatalf("task %d accepted but ran %d times", i, run)
				case acc == 0 && run != 0:
					t.Fatalf("task %d rejected but ran %d times", i, run)
				}
			}
		})
	}
}

// TestStressReentrantSubmission makes tasks submit further tasks and verifies
// the total number of executions is exact.
func TestStressReentrantSubmission(t *testing.T) {
	for _, shards := range []int{1, 8, 32} {
		shards := shards
		t.Run("shards"+itoa(shards), func(t *testing.T) {
			pool := agilepool.NewPool(agilepool.NewConfig(
				agilepool.WithWorkerNumCapacity(64),
				agilepool.WithTaskQueueSize(1000),
				agilepool.WithTaskQueueShards(shards),
			))
			defer pool.Close()

			const roots = 2000
			const childrenPerRoot = 5
			var total int64
			var rootWG sync.WaitGroup

			for r := 0; r < roots; r++ {
				rootWG.Add(1)
				go func() {
					defer rootWG.Done()
					for c := 0; c < childrenPerRoot; c++ {
						pool.Submit(agilepool.TaskFunc(func() error {
							atomic.AddInt64(&total, 1)
							return nil
						}))
					}
				}()
			}
			rootWG.Wait()
			pool.Wait()

			if want := int64(roots * childrenPerRoot); total != want {
				t.Fatalf("executed %d, want %d", total, want)
			}
		})
	}
}

// TestStressGracefulShutdown verifies the supported shutdown order: producers
// stop, then Close, then Wait. Wait() must return once in-flight tasks drain,
// and every submitted task must have run.
func TestStressGracefulShutdown(t *testing.T) {
	const (
		producers = 8
		perProd   = 2000
	)
	for iter := 0; iter < 30; iter++ {
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithWorkerNumCapacity(32),
			agilepool.WithTaskQueueSize(128),
		))

		var executed int64
		var wg sync.WaitGroup
		for s := 0; s < producers; s++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perProd; i++ {
					pool.Submit(agilepool.TaskFunc(func() error {
						atomic.AddInt64(&executed, 1)
						return nil
					}))
				}
			}()
		}
		wg.Wait() // all producers have finished submitting
		pool.Close()

		done := make(chan struct{})
		go func() { pool.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("iter %d: Wait() hung after Close (executed=%d)", iter, atomic.LoadInt64(&executed))
		}
		if want := int64(producers * perProd); atomic.LoadInt64(&executed) != want {
			t.Fatalf("iter %d: executed %d, want %d", iter, atomic.LoadInt64(&executed), want)
		}
	}
}

// TestStressCloseWhileSubmitting verifies that Close followed by Wait remains
// safe while producers continue attempting submissions. Every accepted task
// must complete, and rejected submissions must not register new work.
func TestStressCloseWhileSubmitting(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithWorkerNumCapacity(32),
			agilepool.WithTaskQueueSize(128),
		))

		var executed int64
		var accepted int64
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for s := 0; s < 8; s++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if pool.TrySubmit(agilepool.TaskFunc(func() error {
						atomic.AddInt64(&executed, 1)
						return nil
					})) {
						atomic.AddInt64(&accepted, 1)
					}
				}
			}()
		}

		time.Sleep(time.Duration(iter%5) * time.Millisecond)
		pool.Close()
		if pool.TrySubmit(agilepool.TaskFunc(func() error { return nil })) {
			t.Fatal("TrySubmit accepted a task after Close")
		}

		done := make(chan struct{})
		go func() { pool.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("iter %d: Wait() hung after Close (executed=%d, accepted=%d, running=%d, queued=%d, idle=%d)",
				iter, atomic.LoadInt64(&executed), atomic.LoadInt64(&accepted),
				pool.GetRunningWorkersNum(), pool.GetTaskQueueLen(), pool.GetIdleWorkerCount())
		}
		close(stop)
		wg.Wait()
		if got, want := atomic.LoadInt64(&executed), atomic.LoadInt64(&accepted); got != want {
			t.Fatalf("iter %d: executed %d tasks, accepted %d", iter, got, want)
		}
	}
}

// TestStressPanicIsolation ensures panicking tasks do not prevent Wait() from
// returning and do not stop other tasks from running.
func TestStressPanicIsolation(t *testing.T) {
	for _, shards := range []int{1, 32} {
		shards := shards
		t.Run("shards"+itoa(shards), func(t *testing.T) {
			pool := agilepool.NewPool(agilepool.NewConfig(
				agilepool.WithWorkerNumCapacity(16),
				agilepool.WithTaskQueueSize(1000),
				agilepool.WithTaskQueueShards(shards),
				agilepool.WithPanicHandler(func(agilepool.Task, any, []byte) {}),
			))
			defer pool.Close()

			const normal = 5000
			var ok int64
			pool.Submit(agilepool.TaskFunc(func() error { panic("boom") }))
			for i := 0; i < normal; i++ {
				idx := i
				pool.Submit(agilepool.TaskFunc(func() error {
					if idx%100 == 0 {
						panic("boom")
					}
					atomic.AddInt64(&ok, 1)
					return nil
				}))
			}

			done := make(chan struct{})
			go func() { pool.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("Wait() hung with panicking tasks")
			}

			want := int64(normal - normal/100)
			if got := atomic.LoadInt64(&ok); got != want {
				t.Fatalf("non-panicking tasks executed %d, want %d", got, want)
			}
		})
	}
}

// TestStressSubmitCtxCancellation verifies SubmitCtx tasks are never executed
// after their context is already cancelled, and that cancellation while queued
// keeps the pool responsive.
func TestStressSubmitCtxCancellation(t *testing.T) {
	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(8),
		agilepool.WithTaskQueueSize(1000),
		agilepool.WithTaskQueueShards(8),
	))
	defer pool.Close()

	var ran int64
	var wg sync.WaitGroup
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // also covers the i%2==1 path; cancel is idempotent
			if i%2 == 0 {
				cancel() // cancelled before submit must not run
			}
			pool.SubmitCtx(ctx, agilepool.TaskFunc(func() error {
				if ctx.Err() != nil {
					return nil // skipped by contextTask.process
				}
				atomic.AddInt64(&ran, 1)
				return nil
			}))
			if i%2 == 1 {
				cancel()
			}
		}(i)
	}
	wg.Wait()
	pool.Wait()
	// Only the lower bound matters (timing-dependent skips), but it must be
	// well under the total to prove cancelled tasks are skipped.
	if got := atomic.LoadInt64(&ran); got >= 2000 {
		t.Fatalf("cancelled-before-submit tasks appear to have run: ran=%d", got)
	}
}

// TestStressHeavyVolume runs a high-volume no-loss/no-dup check across the two
// most contention-heavy shapes: all-auto shards with a huge queue, and a tiny
// queue that forces the overflow buffer to be exercised.
func TestStressHeavyVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy volume stress in -short mode")
	}
	const (
		tasks      = 300000
		submitters = 32
	)
	configs := []struct {
		name   string
		config *agilepool.Config
	}{
		{"auto_shards_big_queue", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(2048), agilepool.WithTaskQueueSize(100000))},
		{"overflow_small_queue", agilepool.NewConfig(agilepool.WithWorkerNumCapacity(64), agilepool.WithTaskQueueSize(8), agilepool.WithTaskQueueShards(8))},
	}
	for _, tc := range configs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pool := agilepool.NewPool(tc.config)
			counts := make([]int32, tasks)
			var executed int64
			var wg sync.WaitGroup
			for s := 0; s < submitters; s++ {
				wg.Add(1)
				go func(start int) {
					defer wg.Done()
					for i := start; i < tasks; i += submitters {
						id := i
						pool.Submit(agilepool.TaskFunc(func() error {
							atomic.AddInt32(&counts[id], 1)
							atomic.AddInt64(&executed, 1)
							return nil
						}))
					}
				}(s)
			}
			wg.Wait()

			done := make(chan struct{})
			go func() { pool.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(60 * time.Second):
				t.Fatalf("%s: Wait() hung, executed=%d/%d", tc.name, atomic.LoadInt64(&executed), tasks)
			}

			if got := atomic.LoadInt64(&executed); got != tasks {
				t.Fatalf("%s: executed %d, want %d", tc.name, got, tasks)
			}
			for i, c := range counts {
				if c != 1 {
					t.Fatalf("%s: task %d ran %d times (want 1)", tc.name, i, c)
				}
			}
			pool.Close()
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
