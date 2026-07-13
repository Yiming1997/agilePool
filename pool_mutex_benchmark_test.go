// Test goal: measure sync.Mutex performance gap between "long hold" vs "short hold" scenarios
//
// Background:
//   Pool has a muIdle sync.Mutex protecting idle worker container operations (add/remove/query).
//   Each worker calls addToIdle(w) after finishing its task to put itself back into the idle container (lock→Add→unlock).
//   Each time the scaler needs to wake an idle worker, it calls Pop() (lock→Pop→unlock).
//   Under high concurrency, thousands of goroutines contend for this lock, making lock contention the throughput bottleneck.
//
// How "long hold" vs "short hold" comparison is created:
//   - Slice container Pop() is O(n): the underlying s.workers = s.workers[1:] shifts the entire array.
//     When the idle container has 50k workers, each Pop shifts 50k pointers (~400KB),
//     hold time = shift cost, and all other contenders must wait.
//   - RingQueue container Pop() is O(1): only modifies the head index, hold time is extremely short.
//
// Test flow (run function):
//   1. Create a Pool with specified container type (Slice or RingQueue), capacity 50k
//   2. Start 5000 concurrent submitters, firing tasks aggressively (500k total)
//   3. Each task takes 20ms (time.Sleep), simulating real business logic
//   4. The pool auto-scales: pops idle workers / creates new ones when needed, returns to idle when over capacity
//   5. This "complete→return to idle→be popped→execute" cycle hammers muIdle, creating lock contention
//   6. Compare total elapsed time between the two container types; the difference is the extra cost of long hold times
package agilepool_test

import (
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

const (
	// pool allows at most 50k workers running concurrently
	// Set high enough to ensure many workers online → large idle container → O(n) cost is significant
	workerCap = 50000

	// Submit 500k tasks total, each taking 20ms
	// 500k × 20ms = 10000s of CPU time, with 50k workers in parallel ≈ theoretical 200ms+
	// In practice, lock contention and scheduling overhead push it to 600-800ms, enough to see the gap
	totalTasks = 500000

	// 5000 goroutines submit simultaneously, creating high-concurrency lock contention
	concurrentSubmitters = 5000

	// Single task duration — not too short (workers finish instantly, contention hasn't formed yet)
	// Not too long (test would take too long). 20ms is a reasonable middle ground
	taskDuration = 20 * time.Millisecond
)

// run runs one round of stress test, returning the total elapsed time from Submit until all tasks complete.
//
// Parameters:
//   ct   — Idle container type (Slice = O(n) Pop long hold, RingQueue = O(1) Pop short hold)
//   lt   — muIdle lock type (MutexLock = sync.Mutex, SpinLock = spin lock)
//   label — Scene name for output table
func run(tb testing.TB, ct agilepool.IdleContainerType, lt agilepool.LockType, label string) time.Duration {
	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(workerCap),
		agilepool.WithIdleContainerType(ct),
		agilepool.WithLockType(lt), // ← key: select lock type by parameter
		agilepool.WithCleanPeriod(500*time.Millisecond),
	))
	defer pool.Close()

	// Atomic counters: submitted = tasks submitted, completed = tasks completed
	var submitted, completed atomic.Int64

	// Peak sampling: record the maximum number of workers running concurrently during the test
	var peakRunning atomic.Int64
	stopSampler := make(chan struct{})
	defer close(stopSampler) // ensure goroutine exits to avoid leak
	go func() {
		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				if r := pool.GetRunningWorkersNum(); r > peakRunning.Load() {
					peakRunning.Store(r)
				}
			case <-stopSampler:
				return
			}
		}
	}()

	// ---------- Launch concurrent submitters ----------
	start := time.Now()
	for g := 0; g < concurrentSubmitters; g++ {
		go func() {
			for {
				// Atomically increment to get a "ticket number"; exit if exceeding the total
				n := submitted.Add(1)
				if n > totalTasks {
					return
				}
				// Submit a task that takes 20ms
				// ↓ This frequently triggers Submit → scaler → muIdle contention
				pool.Submit(agilepool.TaskFunc(func() error {
					time.Sleep(taskDuration)
					completed.Add(1)
					return nil
				}))
			}
		}()
	}

	// ---------- Wait for all tasks to complete (with timeout protection) ----------
	deadline := time.After(120 * time.Second)
	for completed.Load() < totalTasks {
		select {
		case <-deadline:
			tb.Fatalf("timeout: only %d/%d tasks completed", completed.Load(), totalTasks)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
	elapsed := time.Since(start)
	pool.Wait() // ensure the last task has also fully completed

	// ---------- Output one result row ----------
	tps := float64(totalTasks) / elapsed.Seconds()
	tb.Logf("%-32s | %8v | %10.0f t/s | peak=%-5d idle=%-5d created=%-7d",
		label,                             // scene name
		elapsed.Round(time.Millisecond),   // total elapsed time
		tps,                               // throughput (tasks per second)
		peakRunning.Load(),                // peak concurrent worker count
		pool.GetIdleWorkerCount(),         // idle worker count at end
		pool.GetWorkerCreateCount(),       // total workers created
	)
	return elapsed
}

// TestMuIdleContentionReport compares 4 combinations:
//   Container type (Slice = long hold / RingQueue = short hold) × Lock type (Mutex / SpinLock)
func TestMuIdleContentionReport(t *testing.T) {
	t.Log("")
	t.Log("==============================================================================")
	t.Log("  muIdle Lock Contention Stress Test: Container Type × Lock Type, Four-Quadrant Comparison")
	t.Logf("  Scale: %dk tasks × %dms | %d concurrent submitters | capacity %d",
		totalTasks/10000, taskDuration/time.Millisecond, concurrentSubmitters, workerCap)
	t.Log("----------------------------------------------------------------------")
	t.Log("  Slice.Pop() = O(n) pointer shifting (long hold) vs RingQueue.Pop() = O(1) (short hold)")
	t.Log("  sync.Mutex = kernel futex (saves CPU on long holds) vs spin lock = CAS loop (faster on short holds)")
	t.Log("==============================================================================")
	t.Log("")
	t.Logf("%-38s | %8s | %10s | %s", "Scenario", "Duration", "Throughput", "Peak/Idle/Created")
	t.Logf("%-38s-+-%-8s-+-%-10s-+-%s",
		"--------------------------------------", "--------", "----------", "---------------------")

	// ---- Four-quadrant test ----

	// Q1: Long hold + sync.Mutex   → long wait + kernel futex sleep → expected slowest
	e1 := run(t, agilepool.SliceType, agilepool.MutexLock, "Slice+Mutex   (long hold + kernel lock)")

	// Q2: Long hold + spin lock    → long wait + CPU spinning (no sleep) → expected improvement
	e2 := run(t, agilepool.SliceType, agilepool.SpinLock, "Slice+SpinLock(long hold + spin lock)")

	// Q3: Short hold + sync.Mutex  → lock released instantly + kernel futex sleep → expected faster
	e3 := run(t, agilepool.RingQueueType, agilepool.MutexLock, "RingQueue+Mutex  (short hold + kernel lock)")

	// Q4: Short hold + spin lock   → lock released instantly + CPU spinning (no sleep) → expected fastest (baseline)
	e4 := run(t, agilepool.RingQueueType, agilepool.SpinLock, "RingQueue+SpinLock(short hold + spin lock)")

	// ---- Comparison summary ----
	t.Log("")
	t.Log("----------------------------------------------------------------------")
	t.Log("  Comparison 1: Slice long-hold scenario, SpinLock vs Mutex improvement")
	t.Logf("    Duration: %v → %v  (%.0f%% improvement)",
		e1.Round(time.Millisecond), e2.Round(time.Millisecond),
		(1-float64(e2)/float64(e1))*100)
	t.Log("")
	t.Log("  Comparison 2: RingQueue short-hold scenario, SpinLock vs Mutex improvement")
	t.Logf("    Duration: %v → %v  (%.0f%% improvement)",
		e3.Round(time.Millisecond), e4.Round(time.Millisecond),
		(1-float64(e4)/float64(e3))*100)
	t.Log("")
	t.Log("  Conclusion: The longer the hold time + the more intense the contention → the greater the spin lock advantage")
	t.Log("             When hold time is extremely short, spin lock and mutex differ little (Go's mutex also spins internally)")
	t.Log("----------------------------------------------------------------------")
}
