package agilepool

import (
	"context"
	"sync/atomic"
	"testing"
)

type hookTestTask struct{}

func (*hookTestTask) Process() {}

func TestHooksReceiveUnwrappedContextTask(t *testing.T) {
	p := NewPool(NewConfig())
	defer p.Close()

	ctx := context.WithValue(context.Background(), "key", "value")
	task := &hookTestTask{}
	var started atomic.Int64
	p.OnTaskStarted(func(gotCtx context.Context, gotTask Task) {
		if gotCtx == ctx && gotTask == task {
			started.Add(1)
		}
	})

	p.SubmitCtx(ctx, task)
	p.Wait()
	if started.Load() != 1 {
		t.Fatalf("started hook did not receive original context and task")
	}
}

func TestHooksWaitIncludesCompletedHook(t *testing.T) {
	p := NewPool(NewConfig())
	defer p.Close()

	completed := make(chan struct{})
	p.OnTaskCompleted(func(context.Context, Task, any) {
		<-completed
	})
	p.Submit(TaskFunc(func() error { return nil }))

	waitReturned := make(chan struct{})
	go func() {
		p.Wait()
		close(waitReturned)
	}()

	select {
	case <-waitReturned:
		t.Fatal("Wait returned before completed hook finished")
	default:
	}
	close(completed)
	<-waitReturned
}
