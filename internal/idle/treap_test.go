package idle

import (
	"testing"
	"time"
)

func TestTreap_Add(t *testing.T) {
	tests := []struct {
		name     string
		addCount int
		wantLen  int64
	}{
		{
			name:     "add zero workers",
			addCount: 0,
			wantLen:  0,
		},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewTreap[*testWorker]()
			now := time.Unix(1_700_000_000, 0)

			for i := 0; i < tt.addCount; i++ {
				tr.Add(newTestWorker(now.Add(time.Duration(i) * time.Second)))
			}

			if got := tr.Len(); got != tt.wantLen {
				t.Errorf("Len() = %v, want %v", got, tt.wantLen)
			}
		})
	}
}

func TestTreap_Pop(t *testing.T) {
	t.Run("pop from empty treap", func(t *testing.T) {
		tr := NewTreap[*testWorker]()

		if got := tr.Pop(); got != nil {
			t.Errorf("Pop() = %v, want nil", got)
		}

		if got := tr.Len(); got != 0 {
			t.Errorf("Len() = %v, want 0", got)
		}
	})

	t.Run("pop oldest worker first", func(t *testing.T) {
		tr := NewTreap[*testWorker]()
		now := time.Unix(1_700_000_000, 0)

		oldest := newTestWorker(now.Add(-30 * time.Second))
		middle := newTestWorker(now.Add(-20 * time.Second))
		newest := newTestWorker(now.Add(-10 * time.Second))

		tr.root = &treapNode[*testWorker]{
			value:    middle,
			priority: 0,
			left: &treapNode[*testWorker]{
				value:    oldest,
				priority: 1,
			},
			right: &treapNode[*testWorker]{
				value:    newest,
				priority: 2,
			},
		}
		tr.size = 3

		wants := []*testWorker{oldest, middle, newest}
		for i, want := range wants {
			if got := tr.Pop(); got != want {
				t.Errorf("Pop() order %d = %v, want %v", i, got, want)
			}
		}

		if got := tr.Len(); got != 0 {
			t.Errorf("Len() after all pops = %v, want 0", got)
		}

		if got := tr.Pop(); got != nil {
			t.Errorf("Pop() after drain = %v, want nil", got)
		}
	})

	t.Run("pop workers with the same lastActiveAt", func(t *testing.T) {
		tr := NewTreap[*testWorker]()
		now := time.Unix(1_700_000_000, 0)

		older1 := newTestWorker(now.Add(-10 * time.Second))
		older2 := newTestWorker(now.Add(-10 * time.Second))
		newer1 := newTestWorker(now.Add(-9 * time.Second))
		newer2 := newTestWorker(now.Add(-9 * time.Second))

		tr.Add(newer2)
		tr.Add(older1)
		tr.Add(newer1)
		tr.Add(older2)

		if got := tr.Len(); got != 4 {
			t.Fatalf("Len() = %v, want 4", got)
		}

		assertTreapPopGroups(t, tr,
			[]*testWorker{older1, older2},
			[]*testWorker{newer1, newer2},
		)
	})
}

func TestTreap_RemoveExpired(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	expiry := 5 * time.Second

	t.Run("no workers", func(t *testing.T) {
		tr := NewTreap[*testWorker]()

		removed := tr.RemoveExpired(now, expiry)

		if removed != 0 {
			t.Errorf("RemoveExpired() = %v, want 0", removed)
		}

		if got := tr.Len(); got != 0 {
			t.Errorf("Len() = %v, want 0", got)
		}
	})

	t.Run("remove expired and preserve active workers", func(t *testing.T) {
		tr := NewTreap[*testWorker]()

		expiredOld := newTestWorker(now.Add(-10 * time.Second))
		expiredBoundary := newTestWorker(now.Add(-5 * time.Second))
		activeBoundary := newTestWorker(now.Add(
			-5*time.Second + time.Nanosecond,
		))
		activeNew := newTestWorker(now.Add(-1 * time.Second))

		tr.Add(activeNew)
		tr.Add(expiredBoundary)
		tr.Add(activeBoundary)
		tr.Add(expiredOld)

		removed := tr.RemoveExpired(now, expiry)
		if removed != 2 {
			t.Errorf("RemoveExpired() = %v, want 2", removed)
		}

		if got := tr.Len(); got != 2 {
			t.Fatalf("Len() after removal = %v, want 2", got)
		}

		assertTreapPopsOnly(t, tr, activeBoundary, activeNew)
	})

	t.Run("all active", func(t *testing.T) {
		tr := NewTreap[*testWorker]()

		older := newTestWorker(now.Add(-4 * time.Second))
		newer := newTestWorker(now.Add(-1 * time.Second))

		tr.Add(newer)
		tr.Add(older)

		removed := tr.RemoveExpired(now, expiry)
		if removed != 0 {
			t.Errorf("RemoveExpired() = %v, want 0", removed)
		}

		if got := tr.Len(); got != 2 {
			t.Fatalf("Len() = %v, want 2", got)
		}

		assertTreapPopsOnly(t, tr, older, newer)
	})

	t.Run("all expired", func(t *testing.T) {
		tr := NewTreap[*testWorker]()

		tr.Add(newTestWorker(now.Add(-20 * time.Second)))
		tr.Add(newTestWorker(now.Add(-10 * time.Second)))
		tr.Add(newTestWorker(now.Add(-5 * time.Second)))

		removed := tr.RemoveExpired(now, expiry)
		if removed != 3 {
			t.Errorf("RemoveExpired() = %v, want 3", removed)
		}

		if got := tr.Len(); got != 0 {
			t.Errorf("Len() = %v, want 0", got)
		}

		if got := tr.Pop(); got != nil {
			t.Errorf("Pop() = %v, want nil", got)
		}
	})

	t.Run("same lastActiveAt workers are removed together when expired", func(t *testing.T) {
		tr := NewTreap[*testWorker]()

		expiredSameTime1 := newTestWorker(now.Add(-10 * time.Second))
		expiredSameTime2 := newTestWorker(now.Add(-10 * time.Second))
		activeSameTime1 := newTestWorker(now.Add(-5*time.Second + time.Nanosecond))
		activeSameTime2 := newTestWorker(now.Add(-5*time.Second + time.Nanosecond))

		tr.Add(activeSameTime1)
		tr.Add(expiredSameTime1)
		tr.Add(activeSameTime2)
		tr.Add(expiredSameTime2)

		removed := tr.RemoveExpired(now, expiry)
		if removed != 2 {
			t.Errorf("RemoveExpired() = %v, want 2", removed)
		}

		if got := tr.Len(); got != 2 {
			t.Fatalf("Len() after removal = %v, want 2", got)
		}

		assertTreapPopsOnly(t, tr, activeSameTime1, activeSameTime2)
	})
}

func TestTreap_AddAndPop_Sequence(t *testing.T) {
	tr := NewTreap[*testWorker]()
	now := time.Unix(1_700_000_000, 0)

	w1 := newTestWorker(now.Add(-10 * time.Second))
	w2 := newTestWorker(now.Add(-8 * time.Second))
	w3 := newTestWorker(now.Add(-6 * time.Second))
	w4 := newTestWorker(now.Add(-9 * time.Second))
	w5 := newTestWorker(now.Add(-7 * time.Second))

	tr.Add(w1)
	tr.Add(w2)
	tr.Add(w3)

	if got := tr.Pop(); got != w1 {
		t.Fatalf("first Pop() = %v, want %v", got, w1)
	}

	tr.Add(w4)
	tr.Add(w5)

	assertTreapPopsInOrder(t, tr, w4, w2, w5, w3)
}

// TestTreap_AddDoesNotAllocate checks that adding an element stays free of
// heap allocations.
//
// The cut-off time used to be handed to split as a *time.Time. That pointer
// does not escape - split only dereferences it - so the compiler keeps the
// time on the stack and the treap is allocation-free today. The pointer was
// still a trap: the day split stores or returns it, every Add starts
// allocating a 24 byte time.Time on the hot path of a pool that parks a
// worker on every task boundary. This test fails the moment that happens.
func TestTreap_AddDoesNotAllocate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	workers := make([]*testWorker, 64)
	for i := range workers {
		workers[i] = newTestWorker(now.Add(time.Duration(i) * time.Second))
	}

	tr := NewTreap[*testWorker]()
	// Warm the node pool up so the first Get does not allocate a node.
	for _, w := range workers {
		tr.Add(w)
	}
	for range workers {
		tr.Pop()
	}

	allocs := testing.AllocsPerRun(100, func() {
		for _, w := range workers {
			tr.Add(w)
			tr.Pop()
		}
	})
	if allocs != 0 {
		t.Fatalf("Add allocated %v objects per run, want 0", allocs)
	}
}

// BenchmarkTreapAdd measures the cost of one Add/Pop cycle, including the
// allocations it makes.
func BenchmarkTreapAdd(b *testing.B) {
	now := time.Unix(1_700_000_000, 0)
	workers := make([]*testWorker, 64)
	for i := range workers {
		workers[i] = newTestWorker(now.Add(time.Duration(i) * time.Second))
	}

	tr := NewTreap[*testWorker]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, w := range workers {
			tr.Add(w)
			tr.Pop()
		}
	}
}

func assertTreapPopsOnly(t *testing.T, tr *Treap[*testWorker], wants ...*testWorker) {
	t.Helper()

	remaining := make(map[*testWorker]bool, len(wants))
	for _, want := range wants {
		remaining[want] = true
	}

	for i := 0; i < len(wants); i++ {
		got := tr.Pop()
		if !remaining[got] {
			t.Fatalf("Pop() order %d = %v, want one of remaining workers", i, got)
		}
		delete(remaining, got)
	}

	if got := tr.Pop(); got != nil {
		t.Errorf("Pop() after drain = %v, want nil", got)
	}
}

func assertTreapPopsInOrder(t *testing.T, tr *Treap[*testWorker], wants ...*testWorker) {
	t.Helper()

	for i, want := range wants {
		if got := tr.Pop(); got != want {
			t.Fatalf("Pop() order %d = %v, want %v", i, got, want)
		}
	}

	if got := tr.Pop(); got != nil {
		t.Errorf("Pop() after drain = %v, want nil", got)
	}
}

func assertTreapPopGroups(t *testing.T, tr *Treap[*testWorker], groups ...[]*testWorker) {
	t.Helper()

	for i, group := range groups {
		remaining := make(map[*testWorker]bool, len(group))
		for _, want := range group {
			remaining[want] = true
		}

		for j := 0; j < len(group); j++ {
			got := tr.Pop()
			if !remaining[got] {
				t.Fatalf("Pop() group %d item %d = %v, want one of %v", i, j, got, group)
			}
			delete(remaining, got)
		}
	}

	if got := tr.Pop(); got != nil {
		t.Errorf("Pop() after drain = %v, want nil", got)
	}
}

func TestTreap_ReuseAfterDrain(t *testing.T) {
	t.Run("reuse after Pop drain", func(t *testing.T) {
		tr := NewTreap[*testWorker]()
		now := time.Unix(1_700_000_000, 0)

		tr.Add(newTestWorker(now.Add(-2 * time.Second)))
		tr.Add(newTestWorker(now.Add(-1 * time.Second)))
		tr.Pop()
		tr.Pop()

		if got := tr.Len(); got != 0 {
			t.Fatalf("Len() after drain = %v, want 0", got)
		}

		w := newTestWorker(now)
		tr.Add(w)

		if got := tr.Len(); got != 1 {
			t.Errorf("Len() after re-add = %v, want 1", got)
		}

		if got := tr.Pop(); got != w {
			t.Errorf("Pop() = %v, want w", got)
		}
	})

	t.Run("reuse after RemoveExpired cleared all", func(t *testing.T) {
		tr := NewTreap[*testWorker]()
		now := time.Unix(1_700_000_000, 0)
		expiry := 5 * time.Second

		tr.Add(newTestWorker(now.Add(-10 * time.Second)))
		tr.Add(newTestWorker(now.Add(-8 * time.Second)))

		removed := tr.RemoveExpired(now, expiry)
		if removed != 2 {
			t.Fatalf("RemoveExpired() = %v, want 2", removed)
		}

		if got := tr.Len(); got != 0 {
			t.Fatalf("Len() after removal = %v, want 0", got)
		}

		w := newTestWorker(now)
		tr.Add(w)

		if got := tr.Len(); got != 1 {
			t.Errorf("Len() after re-add = %v, want 1", got)
		}

		if got := tr.Pop(); got != w {
			t.Errorf("Pop() = %v, want w", got)
		}
	})
}
