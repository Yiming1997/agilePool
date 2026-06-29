package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Example: registering and using the built-in Prometheus metrics.
//
//  1. Call agilepool.RegisterBuiltinMetrics(reg) to register all metrics.
//  2. Use agilepool.PrometheusCollect(pool) as an OnCollect callback to
//     periodically refresh the pool Gauges (capacity / running / idle /
//     queue / workers-created).
//  3. Per-task latencies (Histograms) are recorded automatically when
//     tasks are submitted via pool.SubmitCtx():
//     - Enqueue wait:  Submit -> Enqueue
//     - Queue wait:    Enqueue -> Start
//     - Run time:      Start  -> Complete
//     - Total:         Submit -> Complete
//  4. Tasks may also record custom durations via agilepool.Send(startTime)
//     or agilepool.SendWithRand(startTime, sampleRate).

func main() {
	// 1. Register built-in metrics with the default Prometheus registerer
	//    so the /metrics endpoint can serve them. Pass a custom
	//    prometheus.Registry instead if you use a private one.
	agilepool.RegisterBuiltinMetrics(prometheus.DefaultRegisterer)

	// 2. Start the Prometheus HTTP server.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Println("Prometheus metrics available at http://localhost:8080/metrics")
		http.ListenAndServe(":8080", nil)
	}()

	// 3. Create the pool.
	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithCleanPeriod(500*time.Millisecond),
		agilepool.WithWorkerNumCapacity(30000),
		agilepool.WithIdleContainerType(agilepool.LinkedListType),
		agilepool.WithSampleRate(0.3),
	))

	// 4. Periodic Gauge collection via Taker + PrometheusCollect (every 5s).
	opt := agilepool.Option{
		LoopTime: 5 * time.Second,
		Pool:     []*agilepool.Pool{pool},
		OnCollect: []func(p *agilepool.Pool){
			agilepool.PrometheusCollect(pool),
		},
	}
	go agilepool.Taker(opt)

	// 5. Console status printer for debugging.
	go func() {
		for {
			fmt.Printf("Capacity: %d, Running: %d, Idle: %d, Queue: %d, Created: %d\n",
				pool.GetCapacity(),
				pool.GetRunningWorkersNum(),
				pool.GetIdleWorkerCount(),
				pool.GetTaskQueueLen(),
				pool.GetWorkerCreateCount())
			time.Sleep(1 * time.Second)
		}
	}()

	// 6. Submit a burst of tasks. SubmitCtx stamps enqueue time, and the
	//    worker stamps start/complete times, so SendByContext can record
	//    per-stage latencies into the built-in Histograms automatically.
	var submitWG sync.WaitGroup
	taskCount := 200000

	for i := 0; i < taskCount; i++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			pool.SubmitCtx(context.Background(), agilepool.TaskFunc(func() error {
				time.Sleep(10 * time.Second) // simulated work
				return nil
			}))
		}()
	}

	submitWG.Wait()
	pool.Wait()
	fmt.Println("All tasks completed")

	// 7. Keep the process alive so Prometheus can scrape it. Real apps
	//    should listen for os.Signal for graceful shutdown.
	select {}
}
