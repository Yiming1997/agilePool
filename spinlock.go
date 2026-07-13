package agilepool

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Compile-time check: spinLock implements sync.Locker interface
var _ sync.Locker = (*spinLock)(nil)

// spinLock is a user-space spin lock.
//
// Design highlights:
//
//	Two-phase waiting strategy —
//	  Phase 1 (fast path): pure CAS loop with a Gosched every 16 iterations.
//	      Suitable for very short hold times (~100ns), e.g. RingQueue.Pop() only changes an index.
//	      Yielding every 16 iterations prevents CAS storms from saturating the cache-coherency bus and slowing the lock holder.
//	  Phase 2 (slow path): CAS + runtime.Gosched() with exponential backoff (1→2→4→8→16).
//	      Retries up to 1024 rounds before degrading to pure Gosched waiting, preventing infinite spinning
//	      when the lock holder is preempted by the OS.
//
//	Compared to sync.Mutex:
//	  sync.Mutex internally spins ~30 PAUSE iterations then falls back to futex sleep.
//	  For hold times <1μs, futex sleep/wake overhead (~1-5μs) can exceed the wait time itself.
//	  spinLock never sleeps — it busy-waits with CAS; the shorter the hold time, the greater the advantage.
type spinLock struct {
	state uint32 // 0=free, 1=held
}

// newSpinLock creates a new spin lock.
func newSpinLock() *spinLock {
	return &spinLock{}
}

// NewSpinLock creates a new spin lock and returns it as a sync.Locker.
// Exposed for external testing or scenarios that need a spin lock directly.
func NewSpinLock() sync.Locker {
	return &spinLock{}
}

// Lock acquires the lock, blocking until successful.
func (s *spinLock) Lock() {
	// —— Phase 1: fast path, CAS with intermittent yield ——
	// Pure CAS is ~1ns per iteration, 100 iterations = ~100ns. Insert a Gosched every 16 iterations
	// to prevent CAS write storms from slowing the lock holder (cache line bouncing).
	const (
		fastSpin      = 100
		yieldInterval = 16 // yield P every 16 CAS iterations
	)
	for i := 0; i < fastSpin; i++ {
		if atomic.CompareAndSwapUint32(&s.state, 0, 1) {
			return
		}
		if i%yieldInterval == 0 {
			runtime.Gosched()
		}
	}

	// —— Phase 2: slow path, exponential backoff + Gosched ——
	backoff := 1
	const maxBackoff = 16
	const maxRounds = 1024 // safety limit, degrades to pure Gosched waiting beyond this

	for round := 0; round < maxRounds; round++ {
		if atomic.CompareAndSwapUint32(&s.state, 0, 1) {
			return
		}
		for i := 0; i < backoff; i++ {
			runtime.Gosched()
		}
		if backoff < maxBackoff {
			backoff <<= 1
		}
	}

	// —— Phase 3: fallback, lock holder may be preempted by the OS; degrade to pure Gosched to avoid CPU spinning ——
	for !atomic.CompareAndSwapUint32(&s.state, 0, 1) {
		runtime.Gosched()
	}
}

// Unlock releases the lock. Uses atomic store, no CAS needed (only called by the lock holder).
func (s *spinLock) Unlock() {
	atomic.StoreUint32(&s.state, 0)
}
