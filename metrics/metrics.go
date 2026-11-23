// Package metrics exposes pool statistics in Prometheus text format 0.0.4
// using an opt-in hook collector and no third-party dependencies.
package metrics

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type config struct {
	namespace string
	subsystem string
}

// Option configures the metric name prefix.
type Option func(*config)

// WithNamespace adds ns before the fixed agilepool prefix. For example,
// "myapp" produces myapp_agilepool_workers_running. The default is empty.
func WithNamespace(ns string) Option {
	return func(c *config) { c.namespace = ns }
}

// WithSubsystem inserts ss after agilepool. For example, "pool" produces
// agilepool_pool_workers_running. The default is empty.
func WithSubsystem(ss string) Option {
	return func(c *config) { c.subsystem = ss }
}

func configure(opts []Option) (string, error) {
	c := config{}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	parts := []string{}
	if c.namespace != "" {
		parts = append(parts, c.namespace)
	}
	parts = append(parts, "agilepool")
	if c.subsystem != "" {
		parts = append(parts, c.subsystem)
	}
	prefix := strings.Join(parts, "_") + "_"
	for i, ch := range prefix {
		if ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || i > 0 && ch >= '0' && ch <= '9' {
			continue
		}
		return "", fmt.Errorf("metrics: invalid metric name prefix %q", prefix)
	}
	return prefix, nil
}

// WriteText writes ten metrics in a fixed order, including HELP and TYPE.
// Values are independent concurrent snapshots, not a transactional view.
// Register the collector before submissions begin. Invalid name options or a
// nil collector/pool/writer return an error before writing.
func WriteText(w io.Writer, c *Collector, opts ...Option) error {
	prefix, err := configure(opts)
	if err != nil {
		return err
	}
	return writeText(w, c, prefix)
}

func escapeHelp(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "\n", "\\n")
}

func writeText(w io.Writer, c *Collector, prefix string) error {
	if c == nil || c.pool == nil || w == nil {
		return errors.New("metrics: collector, pool and writer must not be nil")
	}
	p := c.pool
	values := [...]struct {
		name, kind, help string
		value            int64
	}{
		{"workers_running", "gauge", "Workers in the run loop, excluding parked workers.", p.GetRunningWorkersNum()},
		{"workers_idle", "gauge", "Workers parked for reuse.", p.GetIdleWorkerCount()},
		{"workers_created_total", "counter", "Total worker structs allocated.", p.GetWorkerCreateCount()},
		{"tasks_submitted_total", "counter", "Accepted tasks observed since collector registration.", c.GetSubmittedTaskCount()},
		{"tasks_completed_total", "counter", "Finished tasks observed since registration, including panics and cancellation skips.", c.GetCompletedTaskCount()},
		{"tasks_rejected_total", "counter", "Non-nil tasks refused before acceptance since collector registration.", c.GetRejectedTaskCount()},
		{"tasks_queued", "gauge", "Tasks in handoff shards and overflow buffer, excluding worker-prefetched tasks.", p.GetQueuedTaskCount()},
		{"task_queue_capacity", "gauge", "Configured handoff capacity, excluding overflow and shard rounding.", int64(p.GetTaskQueueCapacity())},
		{"worker_capacity", "gauge", "Configured maximum running workers.", p.GetCapacity()},
		{"tasks_pending", "gauge", "Unfinished tasks, including worker-held tasks and blocked submissions.", p.GetPendingTaskCount()},
	}
	bw := bufio.NewWriter(w)
	for _, m := range values {
		name := prefix + m.name
		if _, err := fmt.Fprintf(bw, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, escapeHelp(m.help), name, m.kind, name, m.value); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// Handler serves GET and HEAD with text/plain; version=0.0.4; charset=utf-8.
// Invalid options or a nil collector/pool produce HTTP 500. Other methods
// receive 405. Callers register the collector and mount the handler at /metrics.
func Handler(c *Collector, opts ...Option) http.Handler {
	prefix, configErr := configure(opts)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body bytes.Buffer
		err := configErr
		if err == nil {
			err = writeText(&body, c, prefix)
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if r.Method == http.MethodGet {
			_, _ = w.Write(body.Bytes())
		}
	})
}
