package agilepool_test

import (
	"bytes"
	"context"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
	"github.com/Yiming1997/agilePool/v2/hook/dynamic"
)

// countingLogger is a Logger that only counts writes, keeping the concurrent
// probe free of extra synchronization.
type countingLogger struct {
	calls atomic.Int64
}

func (l *countingLogger) Printf(string, ...interface{}) { l.calls.Add(1) }
func (l *countingLogger) Println(...interface{})        { l.calls.Add(1) }

// lockedBuffer is an io.Writer safe for the goroutine a worker logs from.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestAgilePoolSetLoggerConcurrentWithTaskPanics is the regression test for the
// data race between Pool.SetLogger and the panic-reporting path: a worker
// goroutine reads the logger while the caller swaps it. Run with -race, this
// test fails whenever SetLogger writes the field non-atomically; without -race
// it also pins the visibility contract, i.e. a logger installed after the last
// SetLogger call must receive the next panic report.
func TestAgilePoolSetLoggerConcurrentWithTaskPanics(t *testing.T) {
	const iterations = 4000

	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(64),
	))
	defer pool.Close()
	pool.SetLogger(&countingLogger{}) // keep the default logger quiet

	panicTask := agilepool.TaskFunc(func() error { panic("setlogger race probe") })

	var submitWG sync.WaitGroup
	submitWG.Add(1)
	go func() {
		defer submitWG.Done()
		for i := 0; i < iterations; i++ {
			pool.Submit(panicTask)
			if i%32 == 0 {
				runtime.Gosched()
			}
		}
	}()

	// Swap the logger while workers are reporting the panics above.
	replacement := &countingLogger{}
	for i := 0; i < iterations; i++ {
		pool.SetLogger(replacement)
		runtime.Gosched()
	}
	submitWG.Wait()
	pool.Wait()

	// Visibility: the most recently installed logger must be the one the next
	// panic report goes to.
	final := &countingLogger{}
	pool.SetLogger(final)
	pool.Submit(panicTask)
	pool.Wait()
	if got := final.calls.Load(); got != 1 {
		t.Fatalf("logger installed by SetLogger saw %d panic reports, want 1", got)
	}
}

// TestAgilePoolSetLoggerNilRestoresDefault checks that SetLogger(nil) falls
// back to the standard-library logger instead of leaving a nil interface that
// would panic on the next panic report.
func TestAgilePoolSetLoggerNilRestoresDefault(t *testing.T) {
	var buf lockedBuffer
	previous := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(previous)

	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(4),
	))
	defer pool.Close()

	pool.SetLogger(&countingLogger{})
	pool.SetLogger(nil)

	pool.Submit(agilepool.TaskFunc(func() error { panic("nil logger fallback") }))
	pool.Wait()

	if got := buf.String(); !strings.Contains(got, "worker exits from panic") {
		t.Fatalf("standard logger did not receive the panic report; output = %q", got)
	}
}

func TestHookPanicUsesCurrentStandardLogger(t *testing.T) {
	previous := log.Writer()
	t.Cleanup(func() { log.SetOutput(previous) })
	log.SetOutput(io.Discard)

	staticHooks := hook.NewHooks()
	dynamicHooks := dynamic.NewHooks()
	for _, tc := range []struct {
		name     string
		add      func(agilepool.TaskHook)
		dispatch func(context.Context)
	}{
		{"static", staticHooks.AddTaskStarted, staticHooks.DispatchTaskStarted},
		{"dynamic", dynamicHooks.AddTaskStarted, dynamicHooks.DispatchTaskStarted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output lockedBuffer
			log.SetOutput(&output)
			tc.add(func(context.Context) { panic("hook panic probe") })
			tc.dispatch(context.Background())
			if got := output.String(); !strings.Contains(got, "hook panic probe") {
				t.Fatalf("standard logger did not receive the hook panic; output = %q", got)
			}
		})
	}
}
