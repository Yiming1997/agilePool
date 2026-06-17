package agilepool

import (
	"sync/atomic"
	"time"
)

// LinkedList implements IdleWorkerContainer using a doubly linked list (FIFO).
type LinkedList struct {
	head   *llNode
	tail   *llNode
	length int64
}

type llNode struct {
	val  *worker
	next *llNode
	prev *llNode
}

func newLLNode(val *worker) *llNode {
	return &llNode{
		val: val,
	}
}

func newLinkedList() *LinkedList {
	return &LinkedList{}
}

// Add adds a worker to the tail of the linked list.
func (ll *LinkedList) Add(w *worker) {
	node := newLLNode(w)
	if ll.head == nil && ll.tail == nil {
		ll.head = node
		ll.tail = node
		atomic.AddInt64(&ll.length, 1)
		return
	}
	prev := ll.tail
	ll.tail.next = node
	ll.tail = ll.tail.next
	ll.tail.prev = prev
	atomic.AddInt64(&ll.length, 1)
}

// Pop removes and returns the worker at the head of the linked list.
// Returns nil if the list is empty.
func (ll *LinkedList) Pop() *worker {
	if ll.head == nil {
		return nil
	}
	val := ll.head.val
	if ll.head == ll.tail {
		ll.head, ll.tail = nil, nil
	} else {
		ll.head = ll.head.next
		ll.head.prev = nil
	}
	atomic.AddInt64(&ll.length, -1)
	return val
}

// RemoveExpired removes all workers whose lastActiveAt + expiry <= now.
// The linked list is FIFO by insertion order, but lastActiveAt is not monotonic
// with insertion order (a worker finishing a long task may have a newer lastActiveAt
// than a worker inserted after it). Therefore, a full traversal is required.
func (ll *LinkedList) RemoveExpired(now time.Time, expiry time.Duration) int {
	removed := 0
	node := ll.head
	for node != nil {
		next := node.next // save next before potential removal
		if !node.val.lastActiveAt.Add(expiry).After(now) {
			// Worker is expired, remove this node
			ll.removeNode(node)
			removed++
		}
		node = next
	}
	return removed
}

// removeNode removes the given node from the linked list.
func (ll *LinkedList) removeNode(node *llNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		ll.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		ll.tail = node.prev
	}
	node.prev = nil
	node.next = nil
	atomic.AddInt64(&ll.length, -1)
}

// Len returns the number of workers in the linked list.
func (ll *LinkedList) Len() int64 {
	return atomic.LoadInt64(&ll.length)
}
