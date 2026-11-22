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
)

func TestWriteText(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	var out bytes.Buffer
	if err := WriteText(&out, p); err != nil {
		t.Fatal(err)
	}
	names := []string{"workers_running", "workers_idle", "workers_created_total", "tasks_submitted_total", "tasks_completed_total", "tasks_rejected_total", "tasks_queued", "task_queue_capacity", "worker_capacity", "tasks_pending"}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 30 || !strings.HasSuffix(out.String(), "\n") {
		t.Fatalf("expected 30 newline-terminated lines, got %q", out.String())
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
	if err := WriteText(&out, p, WithNamespace("myapp"), WithSubsystem("pool")); err != nil {
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
	want := errors.New("write failed")
	if err := WriteText(failingWriter{want}, p); !errors.Is(err, want) {
		t.Fatalf("got %v, want writer error", err)
	}
	for _, opt := range []Option{WithNamespace("bad\nname"), WithNamespace("9app"), WithSubsystem("bad-name")} {
		var out bytes.Buffer
		if err := WriteText(&out, p, opt); err == nil || out.Len() != 0 {
			t.Fatalf("invalid names must fail before writing: %v, %q", err, out.String())
		}
	}
	if WriteText(io.Discard, nil) == nil || WriteText(nil, p) == nil {
		t.Fatal("nil pool/writer must fail")
	}
	if got := escapeHelp("a\\b\nc"); got != "a\\\\b\\nc" {
		t.Fatalf("escaped HELP = %q", got)
	}
}

func TestHandler(t *testing.T) {
	p := agilepool.NewPool(nil)
	defer p.Close()
	h := Handler(p)
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
	for _, h := range []http.Handler{Handler(nil), Handler(p, WithNamespace("invalid name"))} {
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
	p.Submit(agilepool.NewTask(func() error { return nil }))
	p.Wait()
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(p))
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
	p.Submit(agilepool.NewTask(func() error { return nil }))
	p.Wait()
	for _, opts := range [][]Option{nil, {WithNamespace("myapp"), WithSubsystem("pool")}} {
		var out bytes.Buffer
		if err := WriteText(&out, p, opts...); err != nil {
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
				if err := WriteText(io.Discard, p); err != nil {
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
	if err := WriteText(&out, p); err != nil {
		t.Fatal(err)
	}
	for _, sample := range []string{"tasks_submitted_total 800\n", "tasks_completed_total 800\n", "tasks_rejected_total 0\n", "tasks_pending 0\n", "tasks_queued 0\n"} {
		if !strings.Contains(out.String(), "agilepool_"+sample) {
			t.Fatalf("missing %q", sample)
		}
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
				if err := WriteText(io.Discard, p); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
