// Run with go run ./example/prometheus_example, then GET /submit and /metrics.
package main

import (
	"log"
	"net/http"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
	"github.com/Yiming1997/agilePool/v2/metrics"
)

func main() {
	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(2000),
		agilepool.WithTaskQueueSize(100000),
		agilepool.WithBlockMode(agilepool.NONBLOCK),
	))
	collector := metrics.NewCollector(pool)
	hooks := hook.NewHooks()
	collector.Register(hooks)
	_ = pool.SetHook(hooks) // Install before the first task submission.
	defer func() {
		pool.Close()
		pool.Wait()
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(collector))
	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if !pool.TrySubmit(agilepool.TaskFunc(func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})) {
			http.Error(w, "pool busy", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	server := &http.Server{Addr: "127.0.0.1:8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Print("submit: http://127.0.0.1:8080/submit; metrics: http://127.0.0.1:8080/metrics")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Print(err)
	}
}
