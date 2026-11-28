package agilepool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Embedding the old hook implementation keeps the test observer compatible
// with all five mandatory events while exercising the optional sixth event.
type rejectionObserver struct {
	panicHooks
	enqueued   atomic.Int64
	completed  atomic.Int64
	rejected   atomic.Int64
	onRejected func(context.Context)
}

func (h *rejectionObserver) DispatchTaskEnqueued(context.Context) { h.enqueued.Add(1) }
func (h *rejectionObserver) DispatchTaskCompleted(context.Context, any) {
	h.completed.Add(1)
}
func (h *rejectionObserver) DispatchTaskRejected(ctx context.Context) {
	h.rejected.Add(1)
	if h.onRejected != nil {
		h.onRejected(ctx)
	}
}

func checkHookMetrics(t *testing.T, p *Pool, h *rejectionObserver, accepted, completed, rejected, pending, queued int64) {
	t.Helper()
	got := [5]int64{h.enqueued.Load(), h.completed.Load(), h.rejected.Load(), p.GetPendingTaskCount(), p.GetQueuedTaskCount()}
	want := [5]int64{accepted, completed, rejected, pending, queued}
	if got != want {
		t.Fatalf("accepted/completed/rejected/pending/queued = %v, want %v", got, want)
	}
}

func blockedObservedPool(t *testing.T, mode WorkMode, h Hooks) (*Pool, func()) {
	t.Helper()
	p := NewPool(NewConfig(WithWorkerNumCapacity(1), WithTaskQueueSize(1), WithTaskQueueShards(1), WithBlockMode(mode), WithOverflowBuffer()))
	if err := p.SetHook(h); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); p.Close(); p.Wait() })
	p.Submit(NewTask(func() error { close(started); <-release; return nil }))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	return p, unblock
}

func TestHookAccountingUsesExistingAcceptedAndCompletedEvents(t *testing.T) {
	h := &rejectionObserver{}
	p, unblock := blockedObservedPool(t, BLOCK, h)
	checkHookMetrics(t, p, h, 1, 0, 0, 1, 0)
	p.sampleRates()
	checkHookMetrics(t, p, h, 1, 0, 0, 1, 0)

	// The pool has one blocked worker and a one-slot queue, so a second task is
	// the most that can be in flight: BLOCK mode no longer spills into the
	// overflow buffer. Use that slot for a task whose context is cancelled
	// before it runs, which must still be accounted as accepted and completed
	// (the task body is skipped by contextTask).
	ctx, cancel := context.WithCancel(context.Background())
	p.SubmitCtx(ctx, NewTask(func() error { t.Error("cancelled task executed"); return nil }))
	cancel()
	checkHookMetrics(t, p, h, 2, 0, 0, 2, 1)
	unblock()
	p.Wait()
	checkHookMetrics(t, p, h, 2, 2, 0, 0, 0)
	p.sampleRates()
	checkHookMetrics(t, p, h, 2, 2, 0, 0, 0)
}

func TestRejectedHookIgnoresNilAndReportsEveryRefusedNonNilTask(t *testing.T) {
	h := &rejectionObserver{}
	p, unblock := blockedObservedPool(t, NONBLOCK, h)
	p.Submit(nil)
	var typedNil TaskFunc
	p.TrySubmit(typedNil)
	p.SubmitCtx(context.Background(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.SubmitCtx(ctx, NewTask(func() error { return nil }))
	p.Submit(NewTask(func() error { return nil }))
	if p.TrySubmit(NewTask(func() error { return nil })) {
		t.Fatal("saturated NONBLOCK pool accepted a task")
	}
	checkHookMetrics(t, p, h, 2, 0, 2, 2, 1)
	p.Close()
	p.Submit(NewTask(func() error { return nil }))
	checkHookMetrics(t, p, h, 2, 0, 3, 2, 1)
	unblock()
	p.Wait()
	checkHookMetrics(t, p, h, 2, 2, 3, 0, 0)
}

func TestRejectedHookBufferClosed(t *testing.T) {
	h := &rejectionObserver{}
	p, unblock := blockedObservedPool(t, BLOCK, h)
	p.Submit(NewTask(func() error { return nil }))
	// Isolate the buffer-closed rejection while the pool-level closed flag is 0.
	p.taskBuf.Close()
	if p.TrySubmit(NewTask(func() error { return nil })) {
		t.Fatal("closed overflow buffer accepted a task")
	}
	checkHookMetrics(t, p, h, 2, 0, 1, 2, 1)
	unblock()
	p.Wait()
	checkHookMetrics(t, p, h, 2, 2, 1, 0, 0)
}

// Done is evaluated by submit only when it reaches the saturated-buffer wait.
// The signal makes cancellation and shutdown tests exercise that select rather
// than accidentally taking an earlier rejection branch.
type admissionWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *admissionWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestRejectedHookSaturatedBufferWait(t *testing.T) {
	for _, outcome := range []string{"accept", "cancel", "close"} {
		t.Run(outcome, func(t *testing.T) {
			h := &rejectionObserver{}
			p, unblock := blockedObservedPool(t, BLOCK, h)
			noop := NewTask(func() error { return nil })
			for i := 0; i < maxChunkLen+1; i++ {
				p.Submit(noop)
			}
			accepted := int64(maxChunkLen + 2)
			underlying, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &admissionWaitContext{Context: underlying, waiting: make(chan struct{})}
			returned := make(chan bool, 1)
			go func() { returned <- p.submit(ctx, noop, false) }()
			select {
			case <-ctx.waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("submitter did not reach the saturated-buffer wait")
			}
			checkHookMetrics(t, p, h, accepted, 0, 0, accepted+1, accepted-1)
			switch outcome {
			case "accept":
				unblock()
			case "cancel":
				cancel()
			case "close":
				p.Close()
			}
			select {
			case ok := <-returned:
				if ok != (outcome == "accept") {
					t.Fatalf("accepted = %v for %s", ok, outcome)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("blocked submission did not return")
			}
			unblock()
			p.Wait()
			if outcome == "accept" {
				checkHookMetrics(t, p, h, accepted+1, accepted+1, 0, 0, 0)
			} else {
				checkHookMetrics(t, p, h, accepted, accepted, 1, 0, 0)
			}
		})
	}
}

func TestRejectedHookPreservesWrappedContext(t *testing.T) {
	type key struct{}
	want := context.WithValue(context.Background(), key{}, "request")
	var got context.Context
	h := &rejectionObserver{onRejected: func(ctx context.Context) { got = ctx }}
	p := NewPool(nil)
	if err := p.SetHook(h); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if p.TrySubmit(UpdateTask(want, NewTask(func() error { return nil }))) {
		t.Fatal("closed pool accepted a task")
	}
	if got != want || got.Value(key{}) != "request" {
		t.Fatalf("rejection context = %v, want original wrapped context", got)
	}
}

func TestRejectedHookPreCancelledSubmitCtxUsesExplicitContext(t *testing.T) {
	type key struct{}
	outer, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "outer"))
	cancel()
	inner := context.WithValue(context.Background(), key{}, "inner")
	var got context.Context
	h := &rejectionObserver{onRejected: func(ctx context.Context) { got = ctx }}
	p := NewPool(nil)
	defer p.Close()
	if err := p.SetHook(h); err != nil {
		t.Fatal(err)
	}
	p.SubmitCtx(outer, UpdateTask(inner, NewTask(func() error { return nil })))
	if got != outer || got.Value(key{}) != "outer" {
		t.Fatalf("rejection context = %v, want explicit SubmitCtx context", got)
	}
	checkHookMetrics(t, p, h, 0, 0, 1, 0, 0)
}

func TestRejectedHookRunsAfterPendingBookkeeping(t *testing.T) {
	p := NewPool(nil)
	h := &rejectionObserver{onRejected: func(context.Context) { p.Wait() }}
	if err := p.SetHook(h); err != nil {
		t.Fatal(err)
	}
	p.Close()
	returned := make(chan struct{})
	go func() { p.Submit(NewTask(func() error { return nil })); close(returned) }()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("rejection callback blocked on its own pending bookkeeping")
	}
	checkHookMetrics(t, p, h, 0, 0, 1, 0, 0)
}

func TestRejectedHookPanicsAreIsolated(t *testing.T) {
	p := NewPool(nil)
	h := &rejectionObserver{onRejected: func(context.Context) { panic("rejected hook panic") }}
	if err := p.SetHook(h); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if p.TrySubmit(NewTask(func() error { return nil })) {
		t.Fatal("closed pool accepted a task")
	}
	if !waitPoolDone(p, time.Second) {
		t.Fatal("rejected hook panic stranded Wait")
	}
	checkHookMetrics(t, p, h, 0, 0, 1, 0, 0)
}

func TestRejectedHookExtensionIsOptional(t *testing.T) {
	for _, h := range []Hooks{nil, &panicHooks{}} {
		p := NewPool(nil)
		if err := p.SetHook(h); err != nil {
			t.Fatal(err)
		}
		p.Close()
		if p.TrySubmit(NewTask(func() error { return nil })) {
			t.Fatal("closed pool accepted a task")
		}
		p.Wait()
		if pending := p.GetPendingTaskCount(); pending != 0 {
			t.Fatalf("pending = %d, want 0", pending)
		}
	}
}
