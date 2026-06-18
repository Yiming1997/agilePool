package agilepool

import (
	"sync/atomic"
	"time"
)

const defaultRingQueueCapacity = 64

// RingQueue implements IdleWorkerContainer using a circular buffer (ring queue).
// Add and Pop are both O(1) operations. RemoveExpired requires a full scan.
// Not safe for concurrent use; the caller (Pool) serializes access via muIdle.
type RingQueue struct {
	buf    []*worker
	head   int
	tail   int
	length int64
}

func newRingQueue() *RingQueue {
	return &RingQueue{
		buf: make([]*worker, defaultRingQueueCapacity),
	}
}

// grow doubles the buffer capacity and re-arranges elements in FIFO order.
func (rq *RingQueue) grow() {
	n := int(atomic.LoadInt64(&rq.length))
	cap := len(rq.buf)

	newCap := cap * 2
	newBuf := make([]*worker, newCap)

	// Copy elements in FIFO order from the circular buffer
	for i := 0; i < n; i++ {
		newBuf[i] = rq.buf[(rq.head+i)%cap]
	}

	rq.head = 0
	rq.tail = n
	rq.buf = newBuf
}

// Add appends a worker to the tail of the ring queue. O(1) amortized.
func (rq *RingQueue) Add(w *worker) {
	if len(rq.buf) == int(atomic.LoadInt64(&rq.length)) {
		rq.grow()
	}
	rq.buf[rq.tail] = w
	rq.tail = (rq.tail + 1) % len(rq.buf)
	atomic.AddInt64(&rq.length, 1)
}

// Pop removes and returns the worker at the head of the ring queue.
// Returns nil if the queue is empty. O(1).
func (rq *RingQueue) Pop() *worker {
	if atomic.LoadInt64(&rq.length) == 0 {
		return nil
	}
	w := rq.buf[rq.head]
	rq.buf[rq.head] = nil
	rq.head = (rq.head + 1) % len(rq.buf)
	atomic.AddInt64(&rq.length, -1)
	return w
}

// RemoveExpired removes all workers whose lastActiveAt + expiry <= now.
// Since lastActiveAt is not monotonic with insertion order, a full scan is
// required. After removal, survivors are compacted to maintain FIFO order.
// O(n) where n is the number of idle workers.
func (rq *RingQueue) RemoveExpired(now time.Time, expiry time.Duration) int {
	n := int(atomic.LoadInt64(&rq.length))
	if n == 0 {
		return 0
	}

	cutoff := now.Add(-expiry)
	cap := len(rq.buf)

	// Collect survivors into a temp slice first, then compact.
	// This avoids the read-before-write hazard that occurs when the
	// ring buffer is wrapped (head + n > cap).
	survivors := make([]*worker, 0, n)
	for i := 0; i < n; i++ {
		idx := (rq.head + i) % cap
		w := rq.buf[idx]
		rq.buf[idx] = nil
		if w.lastActiveAt.After(cutoff) {
			survivors = append(survivors, w)
		}
	}

	removed := n - len(survivors)
	for i, w := range survivors {
		rq.buf[i] = w
	}

	rq.head = 0
	rq.tail = len(survivors)
	atomic.AddInt64(&rq.length, -int64(removed))
	return removed
}

// Len returns the number of workers in the ring queue.
func (rq *RingQueue) Len() int64 {
	return atomic.LoadInt64(&rq.length)
}
