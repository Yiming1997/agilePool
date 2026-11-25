// Command keda-example is the demo workload for the KEDA autoscaling example.
//
// It runs an agilePool behind an HTTP server:
//
//	/work    submits one short IO-bound task into the pool and waits for it.
//	         The task sleeps for TASK_MS, which stands in for an IO call.
//	/metrics serves the pool statistics in Prometheus text format. KEDA's
//	         metrics-api scaler reads this endpoint directly, so no Prometheus
//	         server and no metrics adapter are involved.
//	/healthz is the readiness probe.
//
// The pool has a hard worker ceiling (POOL_WORKERS). When the offered load
// exceeds what one pod can run, the backlog grows, the pool's own scaler
// raises its target to the ceiling, and the queue keeps filling. That is the
// state an outside autoscaler has to notice.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
	"github.com/Yiming1997/agilePool/v2/metrics"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	addr := env("ADDR", ":8080")
	workers := int64(envInt("POOL_WORKERS", 8))
	taskMS := envInt("TASK_MS", 5)

	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(workers),
		agilepool.WithTaskQueueSize(100000),
	))
	defer pool.Close()

	h := hook.NewHooks()
	collector := metrics.NewCollector(pool)
	collector.Register(h)
	if err := pool.SetHook(h); err != nil {
		fmt.Fprintln(os.Stderr, "set hook:", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(collector))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		done := make(chan struct{})
		// TrySubmit reports whether the task was accepted. A closed pool
		// refuses it, which keeps a pod that is being scaled in from parking
		// a request forever.
		if !pool.TrySubmit(agilepool.TaskFunc(func() error {
			time.Sleep(time.Duration(taskMS) * time.Millisecond)
			close(done)
			return nil
		})) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		select {
		case <-done:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			w.WriteHeader(http.StatusRequestTimeout)
		}
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	// Graceful shutdown. Kubernetes sends SIGTERM before it removes the pod
	// from the service endpoints, so this is the scale-in path: stop taking
	// new requests, let the in-flight ones finish, and only then close the
	// pool. Close rejects further submissions and Wait returns once every
	// accepted task has completed.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		pool.Close()
		pool.Wait()
	}()

	fmt.Printf("agilepool keda example: addr=%s pool_workers=%d task=%dms\n", addr, workers, taskMS)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, "serve:", err)
		os.Exit(1)
	}
}
