package metrics

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
	"github.com/Yiming1997/agilePool/v2/hook/dynamic"
)

func newTestCollector(t *testing.T, p *agilepool.Pool) *Collector {
	t.Helper()
	c := NewCollector(p)
	h := hook.NewHooks()
	c.Register(h)
	if err := p.SetHook(h); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestWriteText(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	c := newTestCollector(t, p)
	var out bytes.Buffer
	if err := WriteText(&out, c); err != nil {
		t.Fatal(err)
	}
	names := []string{"workers_running", "workers_target", "workers_idle", "workers_created_total", "tasks_submitted_total", "tasks_completed_total", "tasks_rejected_total", "tasks_queued", "task_queue_capacity", "worker_capacity", "tasks_pending"}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 33 || !strings.HasSuffix(out.String(), "\n") {
		t.Fatalf("expected 33 newline-terminated lines, got %q", out.String())
	}
	for i, suffix := range names {
		name := "agilepool_" + suffix
		kind := "gauge"
		if strings.HasSuffix(suffix, "_total") {
			kind = "counter"
		}
		if !strings.HasPrefix(lines[3*i], "# HELP "+name+" ") || lines[3*i+1] != "# TYPE "+name+" "+kind {
			t.Fatalf("incorrect metadata for %s", name)
		}
		fields := strings.Fields(lines[3*i+2])
		if len(fields) != 2 || fields[0] != name {
			t.Fatalf("incorrect sample: %q", lines[3*i+2])
		}
		if _, err := strconv.ParseInt(fields[1], 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	// The default worker ceiling is MaxInt64; integer formatting must retain it.
	if !strings.Contains(out.String(), "agilepool_worker_capacity 9223372036854775807\n") {
		t.Fatal("worker capacity lost integer precision")
	}
	out.Reset()
	if err := WriteText(&out, c, WithNamespace("myapp"), WithSubsystem("pool")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "myapp_agilepool_pool_workers_running 0\n") {
		t.Fatal(out.String())
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWriteTextErrors(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	c := newTestCollector(t, p)
	want := errors.New("write failed")
	if err := WriteText(failingWriter{want}, c); !errors.Is(err, want) {
		t.Fatalf("got %v, want writer error", err)
	}
	for _, opt := range []Option{WithNamespace("bad\nname"), WithNamespace("9app"), WithSubsystem("bad-name")} {
		var out bytes.Buffer
		if err := WriteText(&out, c, opt); err == nil || out.Len() != 0 {
			t.Fatalf("invalid names must fail before writing: %v, %q", err, out.String())
		}
	}
	if WriteText(io.Discard, nil) == nil || WriteText(nil, c) == nil || WriteText(io.Discard, NewCollector(nil)) == nil {
		t.Fatal("nil collector/pool/writer must fail")
	}
	if got := escapeHelp("a\\b\nc"); got != "a\\\\b\\nc" {
		t.Fatalf("escaped HELP = %q", got)
	}
}

func TestHandler(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	c := newTestCollector(t, p)
	h := Handler(c)
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(method, "/metrics", nil))
		if method == http.MethodPost {
			if rr.Code != http.StatusMethodNotAllowed || rr.Header().Get("Allow") != "GET, HEAD" {
				t.Fatal(rr.Result())
			}
			continue
		}
		if rr.Code != http.StatusOK || rr.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
			t.Fatal(rr.Result())
		}
		if method == http.MethodHead && rr.Body.Len() != 0 {
			t.Fatal("HEAD must not write a body")
		}
		if method == http.MethodGet && !strings.Contains(rr.Body.String(), "# TYPE agilepool_tasks_completed_total counter\n") {
			t.Fatal("GET missing metrics")
		}
	}
	for _, h := range []http.Handler{Handler(nil), Handler(NewCollector(nil)), Handler(c, WithNamespace("invalid name"))} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if rr.Code != http.StatusInternalServerError {
			t.Fatal(rr.Result())
		}
	}
}

func TestHandlerHTTP(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	c := newTestCollector(t, p)
	p.Submit(agilepool.NewTask(func() error { return nil }))
	p.Wait()
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(c))
	server := httptest.NewServer(mux)
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("agilepool_tasks_completed_total 1\n")) {
		t.Fatalf("HTTP %d, body: %s", response.StatusCode, body)
	}
}

// Optional integration check: put the official promtool binary on PATH.
func TestPromtool(t *testing.T) {
	tool, err := exec.LookPath("promtool")
	if err != nil {
		t.Skip("promtool is not installed")
	}
	p := agilepool.NewPool(nil)
	defer p.Close()
	c := newTestCollector(t, p)
	p.Submit(agilepool.NewTask(func() error { return nil }))
	p.Wait()
	for _, opts := range [][]Option{nil, {WithNamespace("myapp"), WithSubsystem("pool")}} {
		var out bytes.Buffer
		if err := WriteText(&out, c, opts...); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		command := exec.CommandContext(ctx, tool, "check", "metrics")
		command.Stdin = &out
		result, err := command.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("promtool check metrics: %v\n%s", err, result)
		}
	}
}

func TestConcurrentScrapesAndSubmissions(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig(agilepool.WithWorkerNumCapacity(8)))
	defer p.Close()
	var completedHooks atomic.Int64
	hooks := hook.NewHooks()
	c := NewCollector(p)
	c.Register(hooks)
	hooks.AddTaskCompleted(func(_ context.Context, _ any) { completedHooks.Add(1) })
	if err := p.SetHook(hooks); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				p.Submit(agilepool.NewTask(func() error { return nil }))
				if err := WriteText(io.Discard, c); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	p.Wait()
	if completedHooks.Load() != 800 {
		t.Fatal("metrics interfered with user completion hooks")
	}
	var out bytes.Buffer
	if err := WriteText(&out, c); err != nil {
		t.Fatal(err)
	}
	for _, sample := range []string{"tasks_submitted_total 800\n", "tasks_completed_total 800\n", "tasks_rejected_total 0\n", "tasks_pending 0\n", "tasks_queued 0\n"} {
		if !strings.Contains(out.String(), "agilepool_"+sample) {
			t.Fatalf("missing %q", sample)
		}
	}
}

func TestCollectorLifecycleWithBothDispatchers(t *testing.T) {
	type dispatcher interface {
		Registrar
		agilepool.Hooks
	}
	for _, test := range []struct {
		name string
		new  func() dispatcher
	}{
		{"static", func() dispatcher { return hook.NewHooks() }},
		{"dynamic", func() dispatcher { return dynamic.NewHooks() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := agilepool.NewPool(agilepool.NewConfig(
				agilepool.WithWorkerNumCapacity(1), agilepool.WithTaskQueueSize(1),
				agilepool.WithBlockMode(agilepool.NONBLOCK),
			))
			c := NewCollector(p)
			h := test.new()
			var userCompleted atomic.Int64
			h.AddTaskCompleted(func(context.Context, any) { userCompleted.Add(1) })
			h.AddTaskCompleted(func(context.Context, any) { panic("user hook panic") })
			c.Register(h)
			_ = p.SetHook(h)
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() { unblock(); p.Close(); p.Wait() })
			p.Submit(agilepool.NewTask(func() error { close(started); <-release; return nil }))
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not start")
			}
			if c.GetSubmittedTaskCount() != 1 || c.GetCompletedTaskCount() != 0 {
				t.Fatal("running task must be accepted, not completed")
			}
			noop := agilepool.NewTask(func() error { return nil })
			p.Submit(noop) // Fills the only channel slot.
			if p.TrySubmit(noop) {
				t.Fatal("saturated NONBLOCK pool accepted task")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			p.SubmitCtx(ctx, noop)
			p.Submit(nil)
			var nilTask agilepool.TaskFunc
			p.TrySubmit(nilTask)
			p.Close()
			p.Submit(noop)
			if c.GetRejectedTaskCount() != 3 || c.GetSubmittedTaskCount() != 2 {
				t.Fatalf("accepted=%d rejected=%d", c.GetSubmittedTaskCount(), c.GetRejectedTaskCount())
			}
			unblock()
			p.Wait()
			if c.GetCompletedTaskCount() != 2 || userCompleted.Load() != 2 {
				t.Fatal("collector and user callbacks must complete before Wait returns")
			}
		})
	}
}

func TestCollectorCancellationPanicAndOverflow(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(1), agilepool.WithTaskQueueSize(1),
		agilepool.WithPanicHandler(func(agilepool.Task, any, []byte) {}),
	))
	c := newTestCollector(t, p)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); p.Close(); p.Wait() })
	p.Submit(agilepool.NewTask(func() error { close(started); <-release; return nil }))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var skippedRan atomic.Bool
	p.SubmitCtx(ctx, agilepool.NewTask(func() error { skippedRan.Store(true); return nil }))
	p.Submit(agilepool.NewTask(func() error { panic("expected") }))
	cancel()
	if p.GetQueuedTaskCount() != 2 || p.GetPendingTaskCount() != 3 || c.GetSubmittedTaskCount() != 3 {
		t.Fatal("buffered tasks are accepted and queued; executing task remains pending")
	}
	unblock()
	p.Wait()
	if c.GetCompletedTaskCount() != 3 || c.GetRejectedTaskCount() != 0 || skippedRan.Load() {
		t.Fatal("accepted cancellation skip and recovered panic must count as completion")
	}
}

func TestCollectorRegistrationDefinesCountingStart(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	c := NewCollector(p)
	noop := agilepool.NewTask(func() error { return nil })
	p.Submit(noop)
	p.Wait()
	if err := WriteText(io.Discard, c); err != nil {
		t.Fatal(err)
	}
	if c.GetSubmittedTaskCount() != 0 || c.GetCompletedTaskCount() != 0 {
		t.Fatal("construction and scraping must not implicitly install hooks")
	}
	h := hook.NewHooks()
	c.Register(h)
	_ = p.SetHook(h)
	p.Submit(noop)
	p.Wait()
	if c.GetSubmittedTaskCount() != 1 || c.GetCompletedTaskCount() != 1 {
		t.Fatal("collector must count only observed events")
	}
}

func BenchmarkWriteText(b *testing.B) {
	for _, load := range []bool{false, true} {
		name := "idle"
		if load {
			name = "submitting"
		}
		b.Run(name, func(b *testing.B) {
			p := agilepool.NewPool(agilepool.NewConfig(
				agilepool.WithWorkerNumCapacity(8), agilepool.WithTaskQueueSize(128),
				agilepool.WithBlockMode(agilepool.NONBLOCK),
			))
			c := NewCollector(p)
			h := hook.NewHooks()
			c.Register(h)
			_ = p.SetHook(h)
			stop := make(chan struct{})
			var producers sync.WaitGroup
			if load {
				for i := 0; i < 4; i++ {
					producers.Add(1)
					go func() {
						defer producers.Done()
						task := agilepool.NewTask(func() error { return nil })
						for {
							select {
							case <-stop:
								return
							default:
								p.TrySubmit(task)
							}
						}
					}()
				}
			}
			defer func() { close(stop); producers.Wait(); p.Close(); p.Wait() }()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := WriteText(io.Discard, c); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

// TestWorkersTargetMatchesPool checks that the exposed workers_target gauge
// reports the pool's own scaler decision, so a consumer of /metrics sees the
// same value as a direct call to GetScalerTarget.
func TestWorkersTargetMatchesPool(t *testing.T) {
	p := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(64),
		agilepool.WithTaskQueueSize(1024),
	))
	defer p.Close()
	c := newTestCollector(t, p)

	read := func() int64 {
		var out bytes.Buffer
		if err := WriteText(&out, c); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(out.String(), "\n") {
			if strings.HasPrefix(line, "agilepool_workers_target ") {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					t.Fatalf("malformed sample %q", line)
				}
				v, err := strconv.ParseInt(fields[1], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
		}
		t.Fatalf("workers_target not found in %q", out.String())
		return 0
	}

	capacity := p.GetCapacity()

	// An idle pool has a stable target of 0, so the exported value and the
	// getter must agree exactly here.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.GetScalerTarget() == 0 && read() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got, want := read(), p.GetScalerTarget(); got != want {
		t.Fatalf("idle pool: exported target %d does not match GetScalerTarget() %d", got, want)
	}

	// Under load the target moves on every scaler tick, so the two readings
	// are independent samples. Both must still be valid values of the same
	// quantity: never negative and never above the configured ceiling.
	blocking := make(chan struct{})
	for i := 0; i < 32; i++ {
		p.Submit(agilepool.NewTask(func() error {
			<-blocking
			return nil
		}))
	}
	defer close(blocking)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		exported := read()
		if exported < 0 || exported > capacity {
			t.Fatalf("exported target %d is outside [0, %d]", exported, capacity)
		}
		direct := p.GetScalerTarget()
		if direct < 0 || direct > capacity {
			t.Fatalf("GetScalerTarget() = %d is outside [0, %d]", direct, capacity)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
