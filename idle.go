package agilepool

import "time"

// IdleContainerType defines the type of data structure used for idle worker management.
type IdleContainerType int8

const (
	// LinkedListType uses a doubly linked list (FIFO) for idle worker management.
	LinkedListType IdleContainerType = iota
	// MinHeapType uses a min-heap ordered by lastActiveAt for idle worker management.
	MinHeapType
	// SliceType uses a dynamic array (slice) with FIFO order for idle worker management.
	SliceType
	// RingQueueType uses a ring buffer (circular buffer) for idle worker management.
	// Add and Pop are both O(1), offering better Pop performance than SliceType.
	RingQueueType
)

// IdleWorkerContainer abstracts the data structure for managing idle workers.
// Both LinkedList and MinHeap implement this interface.
type IdleWorkerContainer interface {
	// Add adds a worker to the container.
	Add(w *worker)
	// Pop removes and returns a worker from the container.
	// Returns nil if the container is empty.
	Pop() *worker
	// RemoveExpired removes all workers whose lastActiveAt + expiry <= now.
	// Returns the number of workers removed.
	RemoveExpired(now time.Time, expiry time.Duration) int
	// Len returns the number of workers in the container.
	Len() int64
}
