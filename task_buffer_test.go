// These white-box tests exercise chunkedTaskBuffer directly so its internal
// chunk boundaries, capacity limit, and locking behavior can be verified.
package agilepool

import (
	"sync"
	"sync/atomic"
	"testing"
)

type bufferTestTask struct {
	id int
}

// Process is intentionally empty: the buffer stores and forwards tasks but
// never executes them. The id is the only state needed by these tests.
func (bufferTestTask) Process() {}

// fillTaskBuffer constructs an exact FIFO state without exercising
// PushAndForward. pushTail requires its caller to hold taskMu.
func fillTaskBuffer(tb testing.TB, buffer *chunkedTaskBuffer, start, count int) {
	tb.Helper()

	buffer.taskMu.Lock()
	defer buffer.taskMu.Unlock()

	for i := 0; i < count; i++ {
		buffer.pushTail(bufferTestTask{id: start + i})
	}
}

// fillTaskBufferWithTask efficiently creates large states, such as a full
// buffer, when individual task identities are irrelevant.
func fillTaskBufferWithTask(tb testing.TB, buffer *chunkedTaskBuffer, task Task, count int) {
	tb.Helper()

	buffer.taskMu.Lock()
	defer buffer.taskMu.Unlock()

	for i := 0; i < count; i++ {
		buffer.pushTail(task)
	}
}

// drainTaskBuffer uses a deliberately non-chunk-sized batch so draining covers
// repeated PopBatch calls as well as chunk transitions.
func drainTaskBuffer(buffer *chunkedTaskBuffer) []Task {
	const batchSize = 257

	batch := make([]Task, batchSize)
	var tasks []Task
	for {
		n := buffer.PopBatch(batch)
		if n == 0 {
			return tasks
		}
		tasks = append(tasks, batch[:n]...)
	}
}

// bufferTaskID also verifies that the buffer returns the original concrete
// task type rather than replacing or wrapping it.
func bufferTaskID(tb testing.TB, task Task) int {
	tb.Helper()

	testTask, ok := task.(bufferTestTask)
	if !ok {
		tb.Fatalf("task has type %T, want bufferTestTask", task)
	}
	return testTask.id
}

// assertTaskIDs checks both task conservation and FIFO order.
func assertTaskIDs(tb testing.TB, tasks []Task, want []int) {
	tb.Helper()

	if len(tasks) != len(want) {
		tb.Fatalf("got %d tasks, want %d", len(tasks), len(want))
	}
	for i, task := range tasks {
		if got := bufferTaskID(tb, task); got != want[i] {
			tb.Fatalf("task %d has id %d, want %d", i, got, want[i])
		}
	}
}

// assertTaskIDSet checks task conservation without making order part of the
// contract. It is used when a failed forward is placed back into the buffer.
func assertTaskIDSet(tb testing.TB, tasks []Task, want ...int) {
	tb.Helper()

	if len(tasks) != len(want) {
		tb.Fatalf("got %d tasks, want %d", len(tasks), len(want))
	}

	wantCounts := make(map[int]int, len(want))
	for _, id := range want {
		wantCounts[id]++
	}
	for _, task := range tasks {
		id := bufferTaskID(tb, task)
		if wantCounts[id] == 0 {
			tb.Fatalf("unexpected or duplicate task id %d", id)
		}
		wantCounts[id]--
	}
	for id, count := range wantCounts {
		if count != 0 {
			tb.Fatalf("task id %d is missing %d occurrence(s)", id, count)
		}
	}
}

// TestChunkedTaskBufferPushAndForward covers the successful fast path and the
// failed-forward path, both with and without an existing backlog.
func TestChunkedTaskBufferPushAndForward(t *testing.T) {
	t.Run("empty buffer forwards the submitted task", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		callbackCalls := 0
		forwardedID := -1

		result := buffer.PushAndForward(bufferTestTask{id: 1}, func(task Task) bool {
			callbackCalls++
			forwardedID = bufferTaskID(t, task)
			return true
		})

		if result != taskBufferAccepted {
			t.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
		}
		if callbackCalls != 1 {
			t.Fatalf("forward callback called %d times, want 1", callbackCalls)
		}
		if forwardedID != 1 {
			t.Fatalf("forwarded task id = %d, want 1", forwardedID)
		}
		if got := buffer.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
	})

	t.Run("failed forward requeues the task", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		callbackCalls := 0

		result := buffer.PushAndForward(bufferTestTask{id: 2}, func(task Task) bool {
			callbackCalls++
			if got := bufferTaskID(t, task); got != 2 {
				t.Fatalf("forwarded task id = %d, want 2", got)
			}
			return false
		})

		if result != taskBufferAccepted {
			t.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
		}
		if callbackCalls != 1 {
			t.Fatalf("forward callback called %d times, want 1", callbackCalls)
		}
		if got := buffer.Len(); got != 1 {
			t.Fatalf("Len() = %d, want 1", got)
		}

		batch := make([]Task, 1)
		if n := buffer.PopBatch(batch); n != 1 {
			t.Fatalf("PopBatch() = %d, want 1", n)
		}
		assertTaskIDs(t, batch, []int{2})
	})

	t.Run("backlog forward succeeds without losing tasks", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		fillTaskBuffer(t, buffer, 1, 2)
		callbackCalls := 0
		forwardedID := -1

		result := buffer.PushAndForward(bufferTestTask{id: 3}, func(task Task) bool {
			callbackCalls++
			forwardedID = bufferTaskID(t, task)
			return true
		})

		if result != taskBufferAccepted {
			t.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
		}
		if callbackCalls != 1 {
			t.Fatalf("forward callback called %d times, want 1", callbackCalls)
		}
		if forwardedID != 1 {
			t.Fatalf("forwarded task id = %d, want oldest task 1", forwardedID)
		}
		assertTaskIDs(t, drainTaskBuffer(buffer), []int{2, 3})
	})

	t.Run("backlog forward failure preserves every task", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		fillTaskBuffer(t, buffer, 1, 2)
		callbackCalls := 0
		forwardedID := -1

		result := buffer.PushAndForward(bufferTestTask{id: 3}, func(task Task) bool {
			callbackCalls++
			forwardedID = bufferTaskID(t, task)
			return false
		})

		if result != taskBufferAccepted {
			t.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
		}
		if callbackCalls != 1 {
			t.Fatalf("forward callback called %d times, want 1", callbackCalls)
		}
		if forwardedID != 1 {
			t.Fatalf("forwarded task id = %d, want oldest task 1", forwardedID)
		}
		// A failed task must remain available, but its exact requeue position is
		// an implementation detail and is intentionally not asserted here.
		assertTaskIDSet(t, drainTaskBuffer(buffer), 1, 2, 3)
	})
}

// TestChunkedTaskBufferPopBatch verifies batch sizing, FIFO behavior across a
// chunk boundary, and reuse after all chunks have been consumed.
func TestChunkedTaskBufferPopBatch(t *testing.T) {
	t.Run("empty and empty destination", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		if n := buffer.PopBatch(nil); n != 0 {
			t.Fatalf("PopBatch(nil) = %d, want 0", n)
		}
		if n := buffer.PopBatch(make([]Task, 0)); n != 0 {
			t.Fatalf("PopBatch(empty) = %d, want 0", n)
		}
		if n := buffer.PopBatch(make([]Task, 1)); n != 0 {
			t.Fatalf("PopBatch() on empty buffer = %d, want 0", n)
		}
	})

	for _, tc := range []struct {
		name      string
		taskCount int
		batchSize int
		wantCount int
	}{
		{name: "batch smaller than buffer", taskCount: 3, batchSize: 2, wantCount: 2},
		{name: "batch equals buffer", taskCount: 3, batchSize: 3, wantCount: 3},
		{name: "batch larger than buffer", taskCount: 3, batchSize: 5, wantCount: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buffer := newChunkedTaskBuffer()
			fillTaskBuffer(t, buffer, 0, tc.taskCount)
			batch := make([]Task, tc.batchSize)

			n := buffer.PopBatch(batch)
			if n != tc.wantCount {
				t.Fatalf("PopBatch() = %d, want %d", n, tc.wantCount)
			}
			for i := 0; i < n; i++ {
				if got := bufferTaskID(t, batch[i]); got != i {
					t.Fatalf("task %d has id %d, want %d", i, got, i)
				}
			}
			if got, want := buffer.Len(), int64(tc.taskCount-tc.wantCount); got != want {
				t.Fatalf("Len() = %d, want %d", got, want)
			}
		})
	}

	t.Run("crosses a chunk boundary in FIFO order", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		// Read seven tasks from the second chunk and leave ten behind. This
		// catches index-reset errors while advancing from one chunk to the next.
		const tailCount = 17
		const remaining = 10
		taskCount := taskChunkSize + tailCount
		batchSize := taskCount - remaining
		fillTaskBuffer(t, buffer, 0, taskCount)
		batch := make([]Task, batchSize)

		n := buffer.PopBatch(batch)
		if n != batchSize {
			t.Fatalf("PopBatch() = %d, want %d", n, batchSize)
		}
		for i, task := range batch {
			if got := bufferTaskID(t, task); got != i {
				t.Fatalf("task %d has id %d, want %d", i, got, i)
			}
		}
		if got := buffer.Len(); got != remaining {
			t.Fatalf("Len() = %d, want %d", got, remaining)
		}
	})

	t.Run("buffer is reusable after being drained", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		firstCount := taskChunkSize + 1
		fillTaskBuffer(t, buffer, 0, firstCount)
		// The extra destination slot makes PopBatch attempt one more pop after
		// the last task, exercising the empty-buffer cleanup path.
		firstBatch := make([]Task, firstCount+1)
		if n := buffer.PopBatch(firstBatch); n != firstCount {
			t.Fatalf("first PopBatch() = %d, want %d", n, firstCount)
		}
		if got := buffer.Len(); got != 0 {
			t.Fatalf("Len() after drain = %d, want 0", got)
		}

		fillTaskBuffer(t, buffer, 100, 3)
		secondBatch := make([]Task, 3)
		if n := buffer.PopBatch(secondBatch); n != len(secondBatch) {
			t.Fatalf("second PopBatch() = %d, want %d", n, len(secondBatch))
		}
		assertTaskIDs(t, secondBatch, []int{100, 101, 102})
	})
}

// TestChunkedTaskBufferCloseAndFull verifies rejection precedence without
// discarding tasks that were accepted before Close.
func TestChunkedTaskBufferCloseAndFull(t *testing.T) {
	t.Run("close is idempotent and accepted tasks remain drainable", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		fillTaskBuffer(t, buffer, 10, 2)
		buffer.Close()
		buffer.Close()
		callbackCalls := 0

		result := buffer.PushAndForward(bufferTestTask{id: 12}, func(Task) bool {
			callbackCalls++
			return true
		})

		if result != taskBufferClosed {
			t.Fatalf("PushAndForward() = %v, want %v", result, taskBufferClosed)
		}
		if callbackCalls != 0 {
			t.Fatalf("forward callback called %d times, want 0", callbackCalls)
		}
		if got := buffer.Len(); got != 2 {
			t.Fatalf("Len() = %d, want 2", got)
		}
		assertTaskIDs(t, drainTaskBuffer(buffer), []int{10, 11})
	})

	t.Run("full buffer rejects before forwarding", func(t *testing.T) {
		buffer := newChunkedTaskBuffer()
		fillTaskBufferWithTask(t, buffer, bufferTestTask{id: 1}, maxChunkLen)
		callbackCalls := 0
		forward := func(Task) bool {
			callbackCalls++
			return true
		}

		if result := buffer.PushAndForward(bufferTestTask{id: 2}, forward); result != taskBufferFull {
			t.Fatalf("PushAndForward() = %v, want %v", result, taskBufferFull)
		}
		if callbackCalls != 0 {
			t.Fatalf("forward callback called %d times, want 0", callbackCalls)
		}
		if got := buffer.Len(); got != maxChunkLen {
			t.Fatalf("Len() = %d, want %d", got, maxChunkLen)
		}

		// Closed is checked before full in PushAndForward, so a buffer in both
		// states must report taskBufferClosed.
		buffer.Close()
		if result := buffer.PushAndForward(bufferTestTask{id: 3}, forward); result != taskBufferClosed {
			t.Fatalf("PushAndForward() on closed full buffer = %v, want %v", result, taskBufferClosed)
		}
		if callbackCalls != 0 {
			t.Fatalf("forward callback called %d times, want 0", callbackCalls)
		}
	})
}

// TestChunkedTaskBufferConcurrentPush is a race-detector target. It checks
// conservation and uniqueness rather than ordering because producer lock
// acquisition order is intentionally nondeterministic.
func TestChunkedTaskBufferConcurrentPush(t *testing.T) {
	const (
		producerCount    = 8
		tasksPerProducer = 1000
		totalTasks       = producerCount * tasksPerProducer
	)

	buffer := newChunkedTaskBuffer()
	var accepted atomic.Int64
	var callbackCalls atomic.Int64
	var unexpectedResults atomic.Int64
	var producers sync.WaitGroup

	for producer := 0; producer < producerCount; producer++ {
		producers.Add(1)
		go func(producer int) {
			defer producers.Done()

			start := producer * tasksPerProducer
			for i := 0; i < tasksPerProducer; i++ {
				// Reject forwarding so every accepted task remains in the buffer
				// until the single-threaded verification phase below.
				result := buffer.PushAndForward(bufferTestTask{id: start + i}, func(Task) bool {
					callbackCalls.Add(1)
					return false
				})
				if result == taskBufferAccepted {
					accepted.Add(1)
				} else {
					unexpectedResults.Add(1)
				}
			}
		}(producer)
	}
	producers.Wait()

	if got := unexpectedResults.Load(); got != 0 {
		t.Fatalf("got %d unexpected push results", got)
	}
	if got := accepted.Load(); got != totalTasks {
		t.Fatalf("accepted %d tasks, want %d", got, totalTasks)
	}
	if got := callbackCalls.Load(); got != totalTasks {
		t.Fatalf("forward callback called %d times, want %d", got, totalTasks)
	}

	tasks := drainTaskBuffer(buffer)
	if len(tasks) != totalTasks {
		t.Fatalf("drained %d tasks, want %d", len(tasks), totalTasks)
	}
	seen := make([]bool, totalTasks)
	for _, task := range tasks {
		id := bufferTaskID(t, task)
		if id < 0 || id >= totalTasks {
			t.Fatalf("task id %d is outside [0, %d)", id, totalTasks)
		}
		if seen[id] {
			t.Fatalf("task id %d was drained more than once", id)
		}
		seen[id] = true
	}
	for id, wasSeen := range seen {
		if !wasSeen {
			t.Fatalf("task id %d was not drained", id)
		}
	}
	if got := buffer.Len(); got != 0 {
		t.Fatalf("Len() = %d after drain, want 0", got)
	}
}
