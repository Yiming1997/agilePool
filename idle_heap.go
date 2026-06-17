package agilepool

import (
	"container/heap"
	"sync/atomic"
	"time"
)

// minHeapInner implements heap.Interface for []*worker.
type minHeapInner []*worker

func (h minHeapInner) Len() int           { return len(h) }
func (h minHeapInner) Less(i, j int) bool { return h[i].lastActiveAt.Before(h[j].lastActiveAt) }
func (h minHeapInner) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeapInner) Push(x any) {
	*h = append(*h, x.(*worker))
}

func (h *minHeapInner) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[:n-1]
	return x
}

// MinHeap implements IdleWorkerContainer using a binary min-heap
// ordered by worker.lastActiveAt. The worker with the smallest (oldest)
// lastActiveAt is always at the root, making expiration cleanup efficient.
type MinHeap struct {
	inner minHeapInner
	size  int64
}

func newMinHeap() *MinHeap {
	h := &MinHeap{
		inner: make(minHeapInner, 0),
	}
	heap.Init(&h.inner)
	return h
}

// Add pushes a worker into the heap. O(log n).
func (h *MinHeap) Add(w *worker) {
	heap.Push(&h.inner, w)
	atomic.AddInt64(&h.size, 1)
}

// Pop removes and returns the worker with the smallest lastActiveAt.
// Returns nil if the heap is empty. O(log n).
func (h *MinHeap) Pop() *worker {
	if h.inner.Len() == 0 {
		return nil
	}
	w := heap.Pop(&h.inner).(*worker)
	atomic.AddInt64(&h.size, -1)
	return w
}

// RemoveExpired removes all workers whose lastActiveAt + expiry <= now.
// Since the root has the smallest lastActiveAt, if the root is not expired,
// no other worker can be expired, so we can stop immediately.
// O(k log n) where k is the number of expired workers.
func (h *MinHeap) RemoveExpired(now time.Time, expiry time.Duration) int {
	removed := 0
	for h.inner.Len() > 0 {
		root := h.inner[0]
		if root.lastActiveAt.Add(expiry).After(now) {
			break
		}
		h.Pop()
		removed++
	}
	return removed
}

// Len returns the number of workers in the heap.
func (h *MinHeap) Len() int64 {
	return atomic.LoadInt64(&h.size)
}
