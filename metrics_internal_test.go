package agilepool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func checkTaskMetrics(t *testing.T, p *Pool, submitted, completed, rejected, pending, queued int64) {
	t.Helper()
	got := [5]int64{p.GetSubmittedTaskCount(), p.GetCompletedTaskCount(), p.GetRejectedTaskCount(), p.GetPendingTaskCount(), p.GetQueuedTaskCount()}
	want := [5]int64{submitted, completed, rejected, pending, queued}
	if got != want {
		t.Fatalf("submitted/completed/rejected/pending/queued = %v, want %v", got, want)
	}
}

func blockedMetricsPool(t *testing.T, mode WorkMode) (*Pool, func()) {
	t.Helper()
	p := NewPool(NewConfig(WithWorkerNumCapacity(1), WithTaskQueueSize(1), WithTaskQueueShards(1), WithBlockMode(mode)))
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

func TestMetricsCompletionAndSampling(t *testing.T) {
	p, unblock := blockedMetricsPool(t, BLOCK)
	checkTaskMetrics(t, p, 1, 0, 0, 1, 0) // Started is not completed.
	p.sampleRates()
	checkTaskMetrics(t, p, 1, 0, 0, 1, 0) // Sampling must not reset lifetime counts.
	p.Submit(NewTask(func() error { panic("test panic") }))
	ctx, cancel := context.WithCancel(context.Background())
	p.SubmitCtx(ctx, NewTask(func() error { t.Error("cancelled task executed"); return nil }))
	cancel()
	checkTaskMetrics(t, p, 3, 0, 0, 3, 2) // One shard slot plus one overflow slot.
	unblock()
	p.Wait()
	checkTaskMetrics(t, p, 3, 3, 0, 0, 0)
	p.sampleRates()
	checkTaskMetrics(t, p, 3, 3, 0, 0, 0)
}

func TestMetricsRejectedAndNilTasks(t *testing.T) {
	p, unblock := blockedMetricsPool(t, NONBLOCK)
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
	checkTaskMetrics(t, p, 2, 0, 2, 2, 1)
	p.Close()
	p.Submit(NewTask(func() error { return nil }))
	checkTaskMetrics(t, p, 2, 0, 3, 2, 1)
	unblock()
	p.Wait()
	checkTaskMetrics(t, p, 2, 2, 3, 0, 0)
}

func TestMetricsBufferClosedRejection(t *testing.T) {
	p, unblock := blockedMetricsPool(t, BLOCK)
	p.Submit(NewTask(func() error { return nil }))
	// Isolate the overflow-closed branch without changing production shutdown.
	p.taskBuf.Close()
	if p.TrySubmit(NewTask(func() error { return nil })) {
		t.Fatal("closed overflow buffer accepted a task")
	}
	checkTaskMetrics(t, p, 2, 0, 1, 2, 1)
	unblock()
	p.Wait()
	checkTaskMetrics(t, p, 2, 2, 1, 0, 0)
}

func TestMetricsSaturatedBuffer(t *testing.T) {
	for _, outcome := range []string{"accept", "cancel", "close"} {
		t.Run(outcome, func(t *testing.T) {
			p, unblock := blockedMetricsPool(t, BLOCK)
			noop := NewTask(func() error { return nil })
			for i := 0; i < maxChunkLen+1; i++ {
				p.Submit(noop)
			}
			accepted := int64(maxChunkLen + 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			returned := make(chan bool, 1)
			go func() { returned <- p.submit(ctx, noop) }()
			deadline := time.After(5 * time.Second)
			for p.GetPendingTaskCount() != accepted+1 {
				select {
				case <-deadline:
					t.Fatal("submitter did not reach admission")
				default:
					time.Sleep(time.Millisecond)
				}
			}
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
				checkTaskMetrics(t, p, accepted+1, accepted+1, 0, 0, 0)
			} else {
				checkTaskMetrics(t, p, accepted, accepted, 1, 0, 0)
			}
		})
	}
}
