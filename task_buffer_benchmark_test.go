// These microbenchmarks isolate chunkedTaskBuffer operations from worker-pool
// scheduling and task execution so changes can be compared with benchstat.
package agilepool

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// chunkedTaskBufferBenchmarkSink keeps observed tasks reachable and uses an
// atomic pointer so parallel benchmark workers can publish safely.
var chunkedTaskBufferBenchmarkSink atomic.Pointer[bufferTestTask]

// rejectBufferedTask forces PushAndForward to exercise its requeue path.
func rejectBufferedTask(Task) bool {
	return false
}

// fillTaskBufferBenchmark prepares a stable queue depth outside the timed
// section. pushTail requires its caller to hold taskMu.
func fillTaskBufferBenchmark(buffer *chunkedTaskBuffer, task Task, count int) {
	buffer.taskMu.Lock()
	defer buffer.taskMu.Unlock()

	for i := 0; i < count; i++ {
		buffer.pushTail(task)
	}
}

// BenchmarkChunkedTaskBufferPushAndForward compares the empty fast path with
// failed forwarding at empty and one-chunk queue depths.
func BenchmarkChunkedTaskBufferPushAndForward(b *testing.B) {
	task := &bufferTestTask{id: 1}

	b.Run("forwarded_empty", func(b *testing.B) {
		buffer := newChunkedTaskBuffer()
		var lastForwarded Task
		forward := func(task Task) bool {
			lastForwarded = task
			return true
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Successful forwarding returns the buffer to depth zero every time.
			if result := buffer.PushAndForward(task, forward); result != taskBufferAccepted {
				b.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
			}
		}
		b.StopTimer()

		chunkedTaskBufferBenchmarkSink.Store(lastForwarded.(*bufferTestTask))
		if got := buffer.Len(); got != 0 {
			b.Fatalf("Len() = %d, want 0", got)
		}
	})

	b.Run("requeued_then_drained", func(b *testing.B) {
		buffer := newChunkedTaskBuffer()
		batch := make([]Task, 1)
		var lastPopped Task

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Drain the rejected task so the queue cannot grow into the full path.
			if result := buffer.PushAndForward(task, rejectBufferedTask); result != taskBufferAccepted {
				b.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
			}
			if n := buffer.PopBatch(batch); n != 1 {
				b.Fatalf("PopBatch() = %d, want 1", n)
			}
			lastPopped = batch[0]
		}
		b.StopTimer()

		chunkedTaskBufferBenchmarkSink.Store(lastPopped.(*bufferTestTask))
		if got := buffer.Len(); got != 0 {
			b.Fatalf("Len() = %d, want 0", got)
		}
	})

	b.Run("requeued_depth_4096", func(b *testing.B) {
		buffer := newChunkedTaskBuffer()
		// Keep one complete chunk queued to exercise head/tail movement and
		// chunk recycling under a persistent backlog.
		fillTaskBufferBenchmark(buffer, task, taskChunkSize)
		batch := make([]Task, 1)
		var lastPopped Task

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// A failed forward temporarily raises depth to taskChunkSize+1;
			// PopBatch restores the original depth before the next iteration.
			if result := buffer.PushAndForward(task, rejectBufferedTask); result != taskBufferAccepted {
				b.Fatalf("PushAndForward() = %v, want %v", result, taskBufferAccepted)
			}
			if n := buffer.PopBatch(batch); n != 1 {
				b.Fatalf("PopBatch() = %d, want 1", n)
			}
			lastPopped = batch[0]
		}
		b.StopTimer()

		chunkedTaskBufferBenchmarkSink.Store(lastPopped.(*bufferTestTask))
		if got := buffer.Len(); got != taskChunkSize {
			b.Fatalf("Len() = %d, want %d", got, taskChunkSize)
		}
	})
}

// BenchmarkChunkedTaskBufferPopBatch measures fixed-size drains across several
// chunk boundaries while excluding queue reconstruction from the timer.
func BenchmarkChunkedTaskBufferPopBatch(b *testing.B) {
	// Four chunks provide repeated boundary transitions before each refill.
	const depth = 4 * taskChunkSize
	task := &bufferTestTask{id: 1}

	for _, batchSize := range []int{1, 8, 32, 64} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			buffer := newChunkedTaskBuffer()
			batch := make([]Task, batchSize)
			remaining := depth
			var lastPopped Task
			fillTaskBufferBenchmark(buffer, task, depth)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if remaining == 0 {
					// Refill is test setup, not part of the PopBatch operation being
					// measured. Each batch size divides depth exactly.
					b.StopTimer()
					fillTaskBufferBenchmark(buffer, task, depth)
					remaining = depth
					b.StartTimer()
				}

				n := buffer.PopBatch(batch)
				if n != batchSize {
					b.Fatalf("PopBatch() = %d, want %d", n, batchSize)
				}
				remaining -= n
				lastPopped = batch[n-1]
			}
			b.StopTimer()
			// ns/op describes one PopBatch call; tasks/op records how many tasks
			// that call consumes so different batch sizes remain interpretable.
			b.ReportMetric(float64(batchSize), "tasks/op")

			chunkedTaskBufferBenchmarkSink.Store(lastPopped.(*bufferTestTask))
			if got := buffer.Len(); got != int64(remaining) {
				b.Fatalf("Len() = %d, want %d", got, remaining)
			}
		})
	}
}

// BenchmarkChunkedTaskBufferParallelForward measures taskMu contention while
// successful forwarding keeps the shared buffer at depth zero.
func BenchmarkChunkedTaskBufferParallelForward(b *testing.B) {
	task := &bufferTestTask{id: 1}

	for _, parallelism := range []int{1, 4} {
		b.Run(fmt.Sprintf("parallelism_%d", parallelism), func(b *testing.B) {
			buffer := newChunkedTaskBuffer()
			var unexpectedResults atomic.Int64

			// SetParallelism is a multiplier of GOMAXPROCS, not an exact
			// goroutine count.
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var lastForwarded Task
				forward := func(task Task) bool {
					lastForwarded = task
					return true
				}

				for pb.Next() {
					if result := buffer.PushAndForward(task, forward); result != taskBufferAccepted {
						// Keep error reporting off the successful hot path and defer the
						// assertion until all benchmark workers have exited.
						unexpectedResults.Add(1)
					}
				}
				if lastForwarded != nil {
					chunkedTaskBufferBenchmarkSink.Store(lastForwarded.(*bufferTestTask))
				}
			})
			b.StopTimer()

			if got := unexpectedResults.Load(); got != 0 {
				b.Fatalf("got %d unexpected push results", got)
			}
			if got := buffer.Len(); got != 0 {
				b.Fatalf("Len() = %d, want 0", got)
			}
		})
	}
}
