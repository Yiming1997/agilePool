// Performance comparison of pure spin locks: sync.Mutex vs spinLock
//
// N goroutines contend for the same lock simultaneously, each performing K cycles of Lock -> atomic increment -> Unlock.
// Measure total execution time → Calculate the number of Lock/Unlock pairs completed per second.
// Compare throughput differences between the two locks under varying concurrency levels.
package agilepool_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

const (
	opsPerG = 500_000 // each goroutine performs 500k Lock→critical section→Unlock cycles
)

// benchLock performs concurrent stress testing on a single lock and returns total execution duration.
//
// Parameters:
//
//	lock       — Target lock under test (both sync.Mutex and spinLock implement sync.Locker)
//	goroutines — Number of concurrent goroutines contending for the lock
//	label      — Name used for table output
//
// Each goroutine runs opsPerG iterations: acquire lock → atomic increment counter → release lock.
// Atomic increment is used for counter++ instead of regular assignment. A normal assignment could
// execute outside the lock and cannot verify proper lock ownership. If atomic increment runs outside
// the lock, updates will be lost, resulting in a final counter value not equal to G×500000.
// Therefore, the counter acts both as a validation check (to confirm no lost lock operations)
// and a simulation of real workload inside critical sections.
func benchLock(tb testing.TB, lock sync.Locker, goroutines int, label string) time.Duration {
	var counter int64 // Shared atomic counter, incremented inside critical section protected by lock.
	// Incrementing outside the lock would cause data loss; used to verify correctness.

	// ---------- Spawn N goroutines to contend for the lock concurrently ----------
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				lock.Lock()                  // Acquire lock
				atomic.AddInt64(&counter, 1) // Critical section: atomic increment = simulates Add/Pop operations for idle containers
				lock.Unlock()                // Release lock
			}
		}()
	}
	wg.Wait() // Wait for all goroutines to finish execution
	elapsed := time.Since(start)

	// ---------- Calculate throughput statistics ----------
	// totalOps = number of goroutines × iterations per goroutine × 2 (Lock and Unlock each count as one operation)
	totalOps := int64(goroutines) * int64(opsPerG) * 2
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	// ---------- Output one result row ----------
	tb.Logf("%-28s | G=%-3d | %8v | %12.0f ops/s | counter=%d",
		label,                           // lock type name
		goroutines,                      // concurrent goroutine count
		elapsed.Round(time.Microsecond), // total elapsed time
		opsPerSec,                       // Lock+Unlock operations per second
		atomic.LoadInt64(&counter),      // validation: must equal goroutines × opsPerG
	)
	return elapsed
}

func TestSpinLockVsMutex(t *testing.T) {
	t.Log("")
	t.Log("==============================================================================")
	t.Log("  Pure Lock Contention Stress Test: sync.Mutex vs spinLock")
	t.Logf("  Each goroutine executes Lock → critical section → Unlock %d times, with multiple goroutines contending for the same lock.", opsPerG)
	t.Log("==============================================================================")
	t.Log("")
	t.Logf("%-28s | %-4s | %8s | %14s | %s", "Lock Type", "Goroutine Count", "Duration", "Throughput (ops/s)", "Counter Validation")
	t.Logf("%-28s-+-%-4s-+-%-8s-+-%-14s-+-%s",
		"----------------------------", "----", "--------", "--------------", "-----------")

	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		benchLock(t, &sync.Mutex{}, n, "sync.Mutex")
		benchLock(t, agilepool.NewSpinLock(), n, "spinLock")
	}

	t.Log("")
	t.Log("==============================================================================")
	t.Log("  Interpretation:")
	t.Log("  - G=1  no contention, compare Lock/Unlock instruction overhead (spinLock is lighter)")
	t.Log("  - G>=4 with contention, compare waiting strategies (Mutex → futex sleep, spinLock → CAS spinning)")
	t.Log("  - counter must equal G × 500000, confirming no lost operations")
	t.Log("==============================================================================")
}
