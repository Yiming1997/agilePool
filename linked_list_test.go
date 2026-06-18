package agilepool

import (
	"testing"
	"time"
)

// NewWorker creates a test worker instance.
// Parameters:
//
//	lastActiveAt: the worker's last active time
//
// Returns:
//
//	*worker: a worker instance for testing
func NewWorker(lastActiveAt time.Time) *worker {
	return &worker{
		lastActiveAt: lastActiveAt,
	}
}

// TestLinkedList_Add tests the add functionality of the linked list.
// Verifies that the list length matches expectations after adding different numbers of workers.
func TestLinkedList_Add(t *testing.T) {
	// Define test case table covering normal and edge cases
	tests := []struct {
		name     string // test case name
		addCount int    // number of workers to add
		wantLen  int64  // expected list length
	}{
		{
			name:     "add one worker",
			addCount: 1,
			wantLen:  1,
		},
		{
			name:     "add multiple workers",
			addCount: 5,
			wantLen:  5,
		},
		{
			name:     "add zero workers", // edge case: adding 0 workers
			addCount: 0,
			wantLen:  0,
		},
	}

	// Run all test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new linked list instance
			ll := newLinkedList()

			// Add the specified number of workers as required by the test case
			for i := 0; i < tt.addCount; i++ {
				ll.Add(NewWorker(time.Now()))
			}

			// Verify the list length matches expectations
			if got := ll.Len(); got != tt.wantLen {
				t.Errorf("Len() = %v, want %v", got, tt.wantLen)
			}
		})
	}
}

// TestLinkedList_Pop tests the pop functionality of the linked list.
// Verifies pop behavior for empty list, single-element list, and FIFO ordering.
func TestLinkedList_Pop(t *testing.T) {
	// Scenario 1: pop from empty list
	t.Run("pop from empty list", func(t *testing.T) {
		ll := newLinkedList()
		got := ll.Pop()

		// Empty list should return nil
		if got != nil {
			t.Errorf("Pop() from empty list = %v, want nil", got)
		}

		// List length should remain 0
		if ll.Len() != 0 {
			t.Errorf("Len() after pop = %v, want 0", ll.Len())
		}
	})

	// Scenario 2: pop a single worker
	t.Run("pop single worker", func(t *testing.T) {
		ll := newLinkedList()
		w := NewWorker(time.Now())
		ll.Add(w)

		// First pop should return the added worker
		got := ll.Pop()
		if got != w {
			t.Errorf("Pop() = %v, want %v", got, w)
		}

		// List should be empty after pop
		if ll.Len() != 0 {
			t.Errorf("Len() after pop = %v, want 0", ll.Len())
		}

		// Second pop from empty list should return nil
		if got2 := ll.Pop(); got2 != nil {
			t.Errorf("Pop() again = %v, want nil", got2)
		}
	})

	// Scenario 3: verify FIFO (first-in-first-out) ordering
	t.Run("pop multiple workers FIFO order", func(t *testing.T) {
		ll := newLinkedList()
		workers := make([]*worker, 3)

		// Add 3 workers in order
		for i := 0; i < 3; i++ {
			workers[i] = NewWorker(time.Now())
			ll.Add(workers[i])
		}

		// Should pop in insertion order (FIFO): index 0, 1, 2
		for i := 0; i < 3; i++ {
			got := ll.Pop()
			if got != workers[i] {
				t.Errorf("Pop() order %d = %v, want %v", i, got, workers[i])
			}
		}

		// List should be empty after all pops
		if ll.Len() != 0 {
			t.Errorf("Len() after all pops = %v, want 0", ll.Len())
		}
	})
}

// TestLinkedList_RemoveExpired tests the expired worker removal functionality.
// Verifies correct identification and removal of expired workers based on last active time and expiry duration.
func TestLinkedList_RemoveExpired(t *testing.T) {
	now := time.Now()
	expiry := 5 * time.Second

	// Define test case table covering various expiration scenarios
	tests := []struct {
		name            string        // test case name
		workers         []time.Time   // last active time of each worker
		expiry          time.Duration // expiry duration
		now             time.Time     // current time
		expectedRemoved int           // expected number of workers removed
		expectedLen     int64         // expected remaining length
	}{
		{
			name:            "no workers", // empty list test
			workers:         []time.Time{},
			expiry:          expiry,
			now:             now,
			expectedRemoved: 0,
			expectedLen:     0,
		},
		{
			name: "no expired workers", // all workers are still active
			workers: []time.Time{
				now.Add(-2 * time.Second),
				now.Add(-3 * time.Second),
				now.Add(-1 * time.Second),
			},
			expiry:          5 * time.Second,
			now:             now,
			expectedRemoved: 0, // all workers were active within the last 5 seconds
			expectedLen:     3,
		},
		{
			name: "all expired workers", // all workers have expired
			workers: []time.Time{
				now.Add(-6 * time.Second),
				now.Add(-7 * time.Second),
				now.Add(-10 * time.Second),
			},
			expiry:          5 * time.Second,
			now:             now,
			expectedRemoved: 3, // all workers' last active time exceeds 5 seconds
			expectedLen:     0,
		},
		{
			name: "mixed expired and active", // mixed scenario: some expired, some active
			workers: []time.Time{
				now.Add(-6 * time.Second), // expired
				now.Add(-3 * time.Second), // active
				now.Add(-8 * time.Second), // expired
				now.Add(-1 * time.Second), // active
				now.Add(-4 * time.Second), // active (4s < 5s)
			},
			expiry:          5 * time.Second,
			now:             now,
			expectedRemoved: 2, // only the first and third are expired
			expectedLen:     3,
		},
		{
			name: "just beyond expiry boundary", // edge case: exactly 1 nanosecond past expiry
			workers: []time.Time{
				now.Add(-5*time.Second - 1*time.Nanosecond),
			},
			expiry:          5 * time.Second,
			now:             now,
			expectedRemoved: 1, // even 1 nanosecond past counts as expired
			expectedLen:     0,
		},
	}

	// Run all test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ll := newLinkedList()

			// Add workers as specified by the test case
			for _, lastActive := range tt.workers {
				ll.Add(NewWorker(lastActive))
			}

			// Execute expired worker removal
			removed := ll.RemoveExpired(tt.now, tt.expiry)

			// Verify removal count
			if removed != tt.expectedRemoved {
				t.Errorf("RemoveExpired() removed = %v, want %v", removed, tt.expectedRemoved)
			}

			// Verify remaining length
			if got := ll.Len(); got != tt.expectedLen {
				t.Errorf("Len() after removal = %v, want %v", got, tt.expectedLen)
			}
		})
	}
}

// TestLinkedList_RemoveExpired_PreservesOrder tests that the order of remaining workers is preserved after removing expired ones.
// Verifies that deletion operations do not break the original FIFO order of remaining elements.
func TestLinkedList_RemoveExpired_PreservesOrder(t *testing.T) {
	now := time.Now()
	expiry := 5 * time.Second

	ll := newLinkedList()
	// Create a mixed list of expired and active workers
	workers := []*worker{
		NewWorker(now.Add(-6 * time.Second)), // index 0: expired, will be removed
		NewWorker(now.Add(-2 * time.Second)), // index 1: active, should remain
		NewWorker(now.Add(-7 * time.Second)), // index 2: expired, will be removed
		NewWorker(now.Add(-3 * time.Second)), // index 3: active, should remain
	}

	// Add all workers in order
	for _, w := range workers {
		ll.Add(w)
	}

	// Execute expired worker removal
	ll.RemoveExpired(now, expiry)

	// Verify remaining count: should only have the two active workers at indices 1 and 3
	if ll.Len() != 2 {
		t.Fatalf("Len() = %v, want 2", ll.Len())
	}

	// Verify pop order: index 1 should be popped first, then index 3 (preserving original order)
	first := ll.Pop()
	if first != workers[1] {
		t.Errorf("First pop = %v, want worker at index 1", first)
	}

	second := ll.Pop()
	if second != workers[3] {
		t.Errorf("Second pop = %v, want worker at index 3", second)
	}
}

// // Planned unit tests
// // TestLinkedList_Concurrent tests concurrency safety.
// // Verifies that the linked list does not panic or cause data races when multiple goroutines perform add and pop operations concurrently.
// func TestLinkedList_Concurrent(t *testing.T) {
// 	ll := newLinkedList()
// 	var wg sync.WaitGroup
// 	iterations := 100 // number of operations per goroutine
// 	goroutines := 10  // number of concurrent goroutines

// 	// Start multiple goroutines to perform add operations concurrently
// 	for i := 0; i < goroutines; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			for j := 0; j < iterations; j++ {
// 				ll.Add(NewWorker(time.Now()))
// 			}
// 		}()
// 	}

// 	// Start multiple goroutines to perform pop operations concurrently
// 	for i := 0; i < goroutines; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			for j := 0; j < iterations; j++ {
// 				ll.Pop()
// 			}
// 		}()
// 	}

// 	// Wait for all goroutines to finish
// 	wg.Wait()

// 	// The final length should be 0 (equal number of adds and pops)
// 	// Note: due to non-deterministic concurrent ordering, actual length may not be 0; no strict assertion is made here.
// 	// The test is considered passing as long as it does not panic.
// 	_ = ll.Len()
// }

// TestLinkedList_AddAndPop_Sequence tests interleaved sequences of add and pop operations.
// Verifies that the list behaves as expected under complex operation sequences, consistent with FIFO queue semantics.
func TestLinkedList_AddAndPop_Sequence(t *testing.T) {
	ll := newLinkedList()

	// Prepare 5 test workers
	workers := make([]*worker, 5)
	for i := 0; i < 5; i++ {
		workers[i] = NewWorker(time.Now())
	}

	// Execute interleaved operation sequence: add 3, pop 1, add 2 more, then pop all
	ll.Add(workers[0])
	ll.Add(workers[1])
	ll.Add(workers[2])

	// Pop the first worker, should be workers[0]
	if got := ll.Pop(); got != workers[0] {
		t.Errorf("Pop() = %v, want %v", got, workers[0])
	}

	// Continue adding
	ll.Add(workers[3])
	ll.Add(workers[4])

	// Verify subsequent pop order: should be workers[1], workers[2], workers[3], workers[4]
	if got := ll.Pop(); got != workers[1] {
		t.Errorf("Pop() = %v, want %v", got, workers[1])
	}
	if got := ll.Pop(); got != workers[2] {
		t.Errorf("Pop() = %v, want %v", got, workers[2])
	}
	if got := ll.Pop(); got != workers[3] {
		t.Errorf("Pop() = %v, want %v", got, workers[3])
	}
	if got := ll.Pop(); got != workers[4] {
		t.Errorf("Pop() = %v, want %v", got, workers[4])
	}

	// List should be empty after all elements are popped
	if ll.Len() != 0 {
		t.Errorf("Len() = %v, want 0", ll.Len())
	}
}

// BenchmarkLinkedList_Add benchmarks the performance of the add operation.
// Measures the average time to add a worker to the linked list.
func BenchmarkLinkedList_Add(b *testing.B) {
	ll := newLinkedList()
	w := NewWorker(time.Now())

	b.ResetTimer() // reset the timer to exclude initialization overhead
	for i := 0; i < b.N; i++ {
		ll.Add(w)
	}
}

// BenchmarkLinkedList_Pop benchmarks the performance of the pop operation.
// Measures the average time to pop a worker from the linked list.
func BenchmarkLinkedList_Pop(b *testing.B) {
	ll := newLinkedList()
	w := NewWorker(time.Now())

	// Pre-fill with enough workers for testing
	for i := 0; i < b.N; i++ {
		ll.Add(w)
	}

	b.ResetTimer() // reset the timer to exclude pre-filling time
	for i := 0; i < b.N; i++ {
		ll.Pop()
	}
}

// BenchmarkLinkedList_RemoveExpired benchmarks the performance of removing expired workers.
// Measures the average time to remove expired workers from a linked list containing 1000 workers.
func BenchmarkLinkedList_RemoveExpired(b *testing.B) {
	ll := newLinkedList()
	now := time.Now()
	expiry := 5 * time.Second

	// Pre-fill with 1000 workers, each with a successively earlier active time
	// This creates a mix of expired and non-expired states
	for i := 0; i < 1000; i++ {
		ll.Add(NewWorker(now.Add(-time.Duration(i) * time.Second)))
	}

	b.ResetTimer() // reset the timer to exclude pre-filling time
	for i := 0; i < b.N; i++ {
		ll.RemoveExpired(now, expiry)
	}
}

func TestLinkedList_AddNil(t *testing.T) {
	ll := newLinkedList()
	ll.Add(nil) // Will the current code panic?
}