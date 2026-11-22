package benchmark_test

import (
	"crypto/sha256"
	"runtime"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

// BenchmarkMetricsOverhead uses only pre-existing APIs so the identical file
// can be copied to a baseline checkout. Timing includes draining accepted work;
// it measures submission-to-completion throughput without scraping metrics.
func BenchmarkMetricsOverhead(b *testing.B) {
	var input [256]byte
	for i := range input {
		input[i] = byte(i)
	}
	for _, workload := range []struct {
		name string
		task agilepool.Task
	}{
		{"noop", agilepool.NewTask(func() error { return nil })},
		{"cpu", agilepool.NewTask(func() error {
			var digest [32]byte
			for i := 0; i < 16; i++ {
				digest = sha256.Sum256(input[:])
			}
			runtime.KeepAlive(digest)
			return nil
		})},
		{"sleep100us", agilepool.NewTask(func() error { time.Sleep(100 * time.Microsecond); return nil })},
	} {
		b.Run(workload.name, func(b *testing.B) {
			p := agilepool.NewPool(agilepool.NewConfig(
				agilepool.WithWorkerNumCapacity(128),
				agilepool.WithTaskQueueSize(4096),
			))
			defer p.Close()
			// Warm the same pool before timing to reduce cold-start influence.
			for i := 0; i < 4096; i++ {
				p.Submit(workload.task)
			}
			p.Wait()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					p.Submit(workload.task)
				}
			})
			p.Wait()
			b.StopTimer()
		})
	}
}
