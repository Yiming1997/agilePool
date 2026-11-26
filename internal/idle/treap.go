package idle

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Treap keeps idle elements ordered by DatedTime (LRU).
// Bulk expiry cleanup is efficient because expired elements form a split subtree.
type Treap[T Dated] struct {
	root *treapNode[T]

	size int64
	pool sync.Pool
}

type treapNode[T Dated] struct {
	value    T
	left     *treapNode[T]
	right    *treapNode[T]
	priority int
}

// NewTreap creates a new empty Treap.
func NewTreap[T Dated]() *Treap[T] {
	return &Treap[T]{
		root: nil,
		size: 0,
		pool: sync.Pool{
			New: func() interface{} {
				return &treapNode[T]{}
			},
		},
	}
}

// split splits the treap into the elements that are not after k (left) and
// the ones that are (right). The cut-off is passed by value: a pointer would
// make every caller heap-allocate the time it passes in.
func (t *Treap[T]) split(u *treapNode[T], k time.Time) (left, right *treapNode[T]) {
	if u == nil {
		return
	}

	if u.value.DatedTime().After(k) {
		left, u.left = t.split(u.left, k)
		right = u
		return
	} else {
		u.right, right = t.split(u.right, k)
		left = u
		return
	}
}

func (t *Treap[T]) merge(left *treapNode[T], right *treapNode[T]) *treapNode[T] {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}

	if left.priority < right.priority {
		left.right = t.merge(left.right, right)
		return left
	} else {
		right.left = t.merge(left, right.left)
		return right
	}
}

func (t *Treap[T]) removeTree(u *treapNode[T]) {
	if u == nil {
		return
	}

	var zero T
	u.value = zero
	t.removeTree(u.left)
	u.left = nil
	t.removeTree(u.right)
	u.right = nil
	t.pool.Put(u)
	atomic.AddInt64(&t.size, -1)
}

// Add inserts an element into the treap ordered by DatedTime.
func (t *Treap[T]) Add(work T) {
	node := t.pool.Get().(*treapNode[T])
	node.value = work
	node.priority = rand.Int()

	left, right := t.split(t.root, work.DatedTime())
	t.root = t.merge(left, t.merge(node, right))
	atomic.AddInt64(&t.size, 1)
}

// Pop removes and returns the element with the smallest DatedTime.
// Returns the zero value of T if the treap is empty.
func (t *Treap[T]) Pop() (val T) {
	if t.root == nil {
		return
	}

	preNode := t.root
	node := preNode.left

	if node == nil {
		t.root = preNode.right
		val = preNode.value
		var zero T
		preNode.value = zero
		preNode.right = nil

		t.pool.Put(preNode)
		atomic.AddInt64(&t.size, -1)
		return
	}

	for {
		if node.left == nil {
			break
		}
		preNode = node
		node = preNode.left
	}

	preNode.left = node.right

	val = node.value
	node.right = nil
	var zero T
	node.value = zero

	t.pool.Put(node)
	atomic.AddInt64(&t.size, -1)
	return
}

// RemoveExpired removes all elements whose DatedTime + expiry <= now.
// O(k log n) where k is the number of expired elements.
func (t *Treap[T]) RemoveExpired(now time.Time, expiry time.Duration) int {
	oriCount := atomic.LoadInt64(&t.size)
	removeTime := now.Add(-expiry)

	var leftTree *treapNode[T]
	leftTree, t.root = t.split(t.root, removeTime)

	t.removeTree(leftTree)
	return int(oriCount - atomic.LoadInt64(&t.size))
}

// Len returns the number of elements in the treap.
func (t *Treap[T]) Len() int64 {
	return atomic.LoadInt64(&t.size)
}
