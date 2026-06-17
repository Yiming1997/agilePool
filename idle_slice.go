package agilepool

import (
	"sync/atomic"
	"time"
)

// Slice implements IdleWorkerContainer using a dynamic array (slice).
// Workers are stored in FIFO order by insertion time, matching the behavior
// of LinkedList. Add appends to the tail, Pop removes from the head.
// Not safe for concurrent use; the caller (Pool) serializes access via muIdle.
type Slice struct {
	workers []*worker
	length  int64
}

// Add appends a worker to the tail of the slice. O(1) amortized.
func (s *Slice) Add(w *worker) {
	s.workers = append(s.workers, w)
	atomic.AddInt64(&s.length, 1)
}

// Pop removes and returns the worker at the head of the slice (FIFO).
// Returns nil if the slice is empty. O(n) due to shifting elements.
func (s *Slice) Pop() *worker {
	if s.Len() == 0 {
		return nil
	}
	w := s.workers[0]
	s.workers[0] = nil
	s.workers = s.workers[1:]
	atomic.AddInt64(&s.length, -1)
	return w
}

// RemoveExpired removes all workers whose lastActiveAt + expiry <= now.
// Workers are ordered by insertion time, which does not guarantee monotonic
// lastActiveAt values, so all workers must be scanned. Survivors retain FIFO
// order. O(n) where n is the number of idle workers.
func (s *Slice) RemoveExpired(now time.Time, expiry time.Duration) int {
	if s.Len() == 0 {
		return 0
	}

	cutoff := now.Add(-expiry)
	originalLen := len(s.workers)
	survivors := s.workers[:0]
	for _, w := range s.workers {
		if w.lastActiveAt.After(cutoff) {
			survivors = append(survivors, w)
		}
	}

	removed := originalLen - len(survivors)
	if removed > 0 {
		clear(s.workers[len(survivors):])
		s.workers = survivors
		atomic.AddInt64(&s.length, -int64(removed))
	}

	return removed
}

// Len returns the number of workers in the slice.
func (s *Slice) Len() int64 {
	return atomic.LoadInt64(&s.length)
}

// newSlice creates a new empty Slice.
func newSlice() *Slice {
	return &Slice{
		workers: make([]*worker, 0),
	}
}
