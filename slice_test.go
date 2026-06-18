package agilepool

import (
	"testing"
	"time"
)

// TestSlice_Add verifies Add and Len.
func TestSlice_Add(t *testing.T) {
	tests := []struct {
		name     string
		addCount int
		wantLen  int64
	}{
		{name: "add one worker", addCount: 1, wantLen: 1},
		{name: "add multiple workers", addCount: 5, wantLen: 5},
		{name: "add zero workers", addCount: 0, wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSlice()
			for i := 0; i < tt.addCount; i++ {
				s.Add(NewWorker(time.Now()))
			}
			if got := s.Len(); got != tt.wantLen {
				t.Errorf("Len() = %v, want %v", got, tt.wantLen)
			}
		})
	}
}

// TestSlice_Pop tests Pop on empty, single, and multiple workers with FIFO order.
func TestSlice_Pop(t *testing.T) {
	t.Run("pop from empty list", func(t *testing.T) {
		s := newSlice()
		if got := s.Pop(); got != nil {
			t.Errorf("Pop() from empty = %v, want nil", got)
		}
		if s.Len() != 0 {
			t.Errorf("Len() after pop = %v, want 0", s.Len())
		}
	})

	t.Run("pop single worker", func(t *testing.T) {
		s := newSlice()
		w := NewWorker(time.Now())
		s.Add(w)

		if got := s.Pop(); got != w {
			t.Errorf("Pop() = %v, want %v", got, w)
		}
		if s.Len() != 0 {
			t.Errorf("Len() after pop = %v, want 0", s.Len())
		}
		if got2 := s.Pop(); got2 != nil {
			t.Errorf("Pop() again = %v, want nil", got2)
		}
	})

	t.Run("pop multiple workers FIFO order", func(t *testing.T) {
		s := newSlice()
		workers := make([]*worker, 3)
		for i := 0; i < 3; i++ {
			workers[i] = NewWorker(time.Now())
			s.Add(workers[i])
		}

		for i := 0; i < 3; i++ {
			if got := s.Pop(); got != workers[i] {
				t.Errorf("Pop() order %d = %v, want %v", i, got, workers[i])
			}
		}
		if s.Len() != 0 {
			t.Errorf("Len() after all pops = %v, want 0", s.Len())
		}
	})
}

// TestSlice_RemoveExpired verifies the full semantic contract:
//   - All expired workers are removed.
//   - All active workers are preserved with their original FIFO order.
//   - The removed count and final length are correct.
//
// Each subtest asserts more than just count/length: it verifies the exact
// identity and FIFO order of every surviving worker via consecutive Pop calls.
func TestSlice_RemoveExpired(t *testing.T) {
	now := time.Now()
	expiry := 5 * time.Second

	t.Run("no workers", func(t *testing.T) {
		s := newSlice()
		removed := s.RemoveExpired(now, expiry)
		if removed != 0 {
			t.Errorf("removed = %v, want 0", removed)
		}
		if s.Len() != 0 {
			t.Errorf("Len() = %v, want 0", s.Len())
		}
	})

	t.Run("all active", func(t *testing.T) {
		s := newSlice()
		w1 := NewWorker(now.Add(-2 * time.Second))
		w2 := NewWorker(now.Add(-3 * time.Second))
		w3 := NewWorker(now.Add(-1 * time.Second))
		s.Add(w1)
		s.Add(w2)
		s.Add(w3)

		removed := s.RemoveExpired(now, expiry)
		if removed != 0 {
			t.Errorf("removed = %v, want 0", removed)
		}
		if s.Len() != 3 {
			t.Fatalf("Len() = %v, want 3", s.Len())
		}
		if got := s.Pop(); got != w1 {
			t.Errorf("Pop() = %v, want w1", got)
		}
		if got := s.Pop(); got != w2 {
			t.Errorf("Pop() = %v, want w2", got)
		}
		if got := s.Pop(); got != w3 {
			t.Errorf("Pop() = %v, want w3", got)
		}
	})

	t.Run("all expired", func(t *testing.T) {
		s := newSlice()
		w1 := NewWorker(now.Add(-10 * time.Second))
		w2 := NewWorker(now.Add(-6 * time.Second))
		w3 := NewWorker(now.Add(-8 * time.Second))
		s.Add(w1)
		s.Add(w2)
		s.Add(w3)

		removed := s.RemoveExpired(now, expiry)
		if removed != 3 {
			t.Errorf("removed = %v, want 3", removed)
		}
		if s.Len() != 0 {
			t.Errorf("Len() = %v, want 0", s.Len())
		}
		if got := s.Pop(); got != nil {
			t.Errorf("Pop() on empty = %v, want nil", got)
		}
	})

	t.Run("expired cluster before active", func(t *testing.T) {
		s := newSlice()
		// Two short tasks become idle first (both expired),
		// then two long tasks finish later (both active).
		wExp1 := NewWorker(now.Add(-6 * time.Second))
		wExp2 := NewWorker(now.Add(-7 * time.Second))
		wAct1 := NewWorker(now.Add(-2 * time.Second))
		wAct2 := NewWorker(now.Add(-1 * time.Second))
		s.Add(wExp1)
		s.Add(wExp2)
		s.Add(wAct1)
		s.Add(wAct2)

		removed := s.RemoveExpired(now, expiry)
		if removed != 2 {
			t.Errorf("removed = %v, want 2", removed)
		}
		if s.Len() != 2 {
			t.Fatalf("Len() = %v, want 2", s.Len())
		}
		if got := s.Pop(); got != wAct1 {
			t.Errorf("Pop() = %v, want wAct1", got)
		}
		if got := s.Pop(); got != wAct2 {
			t.Errorf("Pop() = %v, want wAct2", got)
		}
		if got := s.Pop(); got != nil {
			t.Errorf("Pop() after drain = %v, want nil", got)
		}
	})

	t.Run("interleaved expired and active", func(t *testing.T) {
		s := newSlice()
		// Production-like ordering:
		//   Worker B: started t+5, ran 1s  → idle at t+6,  lastActiveAt=t+5
		//   Worker A: started t+0, ran 10s → idle at t+10, lastActiveAt=t+0
		// Insertion: B(t+5), A(t+0). More workers produce:
		//   [expired, active, expired, active]
		wExp1 := NewWorker(now.Add(-6 * time.Second)) // expired
		wAct1 := NewWorker(now.Add(-2 * time.Second)) // active
		wExp2 := NewWorker(now.Add(-7 * time.Second)) // expired
		wAct2 := NewWorker(now.Add(-1 * time.Second)) // active
		s.Add(wExp1)
		s.Add(wAct1)
		s.Add(wExp2)
		s.Add(wAct2)

		removed := s.RemoveExpired(now, expiry)
		// Correct implementation must remove only the 2 expired workers
		// and preserve both active workers in FIFO order: [wAct1, wAct2].
		if removed != 2 {
			t.Errorf("removed = %v, want 2", removed)
		}
		if s.Len() != 2 {
			t.Fatalf("Len() = %v, want 2", s.Len())
		}
		if got := s.Pop(); got != wAct1 {
			t.Errorf("Pop() = %v, want wAct1", got)
		}
		if got := s.Pop(); got != wAct2 {
			t.Errorf("Pop() = %v, want wAct2", got)
		}
		if got := s.Pop(); got != nil {
			t.Errorf("Pop() after drain = %v, want nil", got)
		}
	})

	t.Run("boundary exactly at expiry", func(t *testing.T) {
		s := newSlice()
		// lastActiveAt == now - expiry  ⇒  should be expired.
		w := NewWorker(now.Add(-5 * time.Second))
		s.Add(w)

		removed := s.RemoveExpired(now, expiry)
		if removed != 1 {
			t.Errorf("removed = %v, want 1", removed)
		}
		if s.Len() != 0 {
			t.Errorf("Len() = %v, want 0", s.Len())
		}
		if got := s.Pop(); got != nil {
			t.Errorf("Pop() on empty = %v, want nil", got)
		}
	})

	t.Run("boundary just barely active", func(t *testing.T) {
		s := newSlice()
		// lastActiveAt == now - expiry + 1ns  ⇒  should be active.
		w := NewWorker(now.Add(-5*time.Second + 1*time.Nanosecond))
		s.Add(w)

		removed := s.RemoveExpired(now, expiry)
		if removed != 0 {
			t.Errorf("removed = %v, want 0", removed)
		}
		if s.Len() != 1 {
			t.Fatalf("Len() = %v, want 1", s.Len())
		}
		if got := s.Pop(); got != w {
			t.Errorf("Pop() = %v, want w", got)
		}
	})
}

// TestSlice_AddAndPop_Sequence tests interleaved add and pop operations,
// verifying FIFO semantics hold across mixed operations.
func TestSlice_AddAndPop_Sequence(t *testing.T) {
	s := newSlice()
	workers := make([]*worker, 5)
	for i := 0; i < 5; i++ {
		workers[i] = NewWorker(time.Now())
	}

	s.Add(workers[0])
	s.Add(workers[1])
	s.Add(workers[2])

	if got := s.Pop(); got != workers[0] {
		t.Errorf("Pop() = %v, want workers[0]", got)
	}

	s.Add(workers[3])
	s.Add(workers[4])

	if got := s.Pop(); got != workers[1] {
		t.Errorf("Pop() = %v, want workers[1]", got)
	}
	if got := s.Pop(); got != workers[2] {
		t.Errorf("Pop() = %v, want workers[2]", got)
	}
	if got := s.Pop(); got != workers[3] {
		t.Errorf("Pop() = %v, want workers[3]", got)
	}
	if got := s.Pop(); got != workers[4] {
		t.Errorf("Pop() = %v, want workers[4]", got)
	}
	if s.Len() != 0 {
		t.Errorf("Len() after all ops = %v, want 0", s.Len())
	}
}

// TestSlice_ReuseAfterDrain verifies the slice is usable after being
// fully drained by Pop or RemoveExpired.
func TestSlice_ReuseAfterDrain(t *testing.T) {
	t.Run("reuse after Pop drain", func(t *testing.T) {
		s := newSlice()
		w1 := NewWorker(time.Now())
		w2 := NewWorker(time.Now())
		s.Add(w1)
		s.Add(w2)
		s.Pop()
		s.Pop()

		if s.Len() != 0 {
			t.Fatalf("Len() after drain = %v, want 0", s.Len())
		}

		w3 := NewWorker(time.Now())
		s.Add(w3)
		if s.Len() != 1 {
			t.Errorf("Len() after re-add = %v, want 1", s.Len())
		}
		if got := s.Pop(); got != w3 {
			t.Errorf("Pop() = %v, want w3", got)
		}
	})

	t.Run("reuse after RemoveExpired cleared all", func(t *testing.T) {
		now := time.Now()
		expiry := 5 * time.Second

		s := newSlice()
		s.Add(NewWorker(now.Add(-10 * time.Second)))
		s.Add(NewWorker(now.Add(-8 * time.Second)))

		removed := s.RemoveExpired(now, expiry)
		if removed != 2 {
			t.Fatalf("removed = %v, want 2", removed)
		}
		if s.Len() != 0 {
			t.Fatalf("Len() after removal = %v, want 0", s.Len())
		}

		w := NewWorker(now)
		s.Add(w)
		if s.Len() != 1 {
			t.Errorf("Len() after re-add = %v, want 1", s.Len())
		}
		if got := s.Pop(); got != w {
			t.Errorf("Pop() = %v, want w", got)
		}
	})
}

// TestSlice_RemoveExpired_MultiRound verifies correct behavior across
// multiple rounds of add + RemoveExpired with interleaved patterns,
// ensuring survivors compacting does not corrupt the slice.
func TestSlice_RemoveExpired_MultiRound(t *testing.T) {
	now := time.Now()
	// Use a short expiry to allow quick testing of multiple rounds with time advancement.
	// 2 seconds
	expiry := 2 * time.Second

	s := newSlice()

	// Round 1: interleaved [exp, act, exp, act]
	wE1 := NewWorker(now.Add(-3 * time.Second)) // expired
	wA1 := NewWorker(now.Add(-1 * time.Second)) // active
	wE2 := NewWorker(now.Add(-4 * time.Second)) // expired
	wA2 := NewWorker(now)                       // active
	s.Add(wE1)
	s.Add(wA1)
	s.Add(wE2)
	s.Add(wA2)

	removed := s.RemoveExpired(now, expiry)
	if removed != 2 {
		t.Errorf("round 1: removed = %v, want 2", removed)
	}
	if s.Len() != 2 {
		t.Fatalf("round 1: Len() = %v, want 2", s.Len())
	}
	if got := s.Pop(); got != wA1 {
		t.Errorf("round 1: Pop() = %v, want wA1", got)
	}
	if got := s.Pop(); got != wA2 {
		t.Errorf("round 1: Pop() = %v, want wA2", got)
	}

	// Round 2: add more workers and expire them all by advancing logical time
	wE3 := NewWorker(now.Add(-1 * time.Second))
	wE4 := NewWorker(now.Add(-500 * time.Millisecond))
	s.Add(wE3)
	s.Add(wE4)

	later := now.Add(3 * time.Second)

	removed = s.RemoveExpired(later, expiry)
	if removed != 2 {
		t.Errorf("round 2: removed = %v, want 2", removed)
	}
	if s.Len() != 0 {
		t.Errorf("round 2: Len() = %v, want 0", s.Len())
	}
	if got := s.Pop(); got != nil {
		t.Errorf("round 2: Pop() on empty = %v, want nil", got)
	}

	// Round 3: reuse after all cleared
	wFinal := NewWorker(later)
	s.Add(wFinal)
	if s.Len() != 1 {
		t.Errorf("round 3: Len() = %v, want 1", s.Len())
	}
	if got := s.Pop(); got != wFinal {
		t.Errorf("round 3: Pop() = %v, want wFinal", got)
	}
}
