package agilepool_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool"
)

const (
	taskCount = 10000000
)

func BenchmarkAgilePoolMinHeap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// 20k worker capacity gives the best performance

		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(10000),
			agilepool.WithWorkerNumCapacity(20000),
			agilepool.WithIdleContainerType(agilepool.MinHeapType),
		))

		for j := 0; j < taskCount; j++ {
			go func() {
				pool.Submit(agilepool.TaskFunc(func() error {
					time.Sleep(10 * time.Millisecond)
					return nil
				}))

			}()
		}
		pool.Wait()
		pool.Close()
	}
}

func BenchmarkAgilePoolLinkedList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// 20k worker capacity gives the best performance
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(10000),
			agilepool.WithWorkerNumCapacity(20000),
			agilepool.WithIdleContainerType(agilepool.LinkedListType),
		))

		for j := 0; j < taskCount; j++ {
			go func() {
				pool.Submit(agilepool.TaskFunc(func() error {
					time.Sleep(10 * time.Millisecond)
					return nil
				}))

			}()
		}
		pool.Wait()
		pool.Close()
	}
}

func BenchmarkAgilePoolSequentialMinHeap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// 20k worker capacity gives the best performance
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(10000),
			agilepool.WithWorkerNumCapacity(100000),
			agilepool.WithIdleContainerType(agilepool.MinHeapType),
		))

		for j := 0; j < taskCount; j++ {
			pool.Submit(agilepool.TaskFunc(func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			}))
		}
		pool.Wait()
		pool.Close()
	}
}

func BenchmarkAgilePoolSequentialLinkedList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// 20k worker capacity gives the best performance
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(100000),
			agilepool.WithWorkerNumCapacity(20000),
			agilepool.WithIdleContainerType(agilepool.LinkedListType),
		))
		for j := 0; j < taskCount; j++ {
			pool.Submit(agilepool.TaskFunc(func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			}))
		}
		pool.Wait()
		pool.Close()
	}
}

func BenchmarkAgilePoolSequentialSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(10000),
			agilepool.WithWorkerNumCapacity(20000),
			agilepool.WithIdleContainerType(agilepool.SliceType),
		))

		for j := 0; j < taskCount; j++ {
			pool.Submit(agilepool.TaskFunc(func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			}))
		}
		pool.Wait()
		pool.Close()
	}
}

type phase struct {
	ticks  int
	tokens int
}

// dispenseTokens 令牌发放函数：按 phases 定义的速率向 tokenCh 发放令牌
func dispenseTokens(b *testing.B, shard int, tokenCh chan struct{}, phases []phase, tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for phaseIndex, p := range phases {
		start := time.Now()
		for t := 0; t < p.ticks; t++ {
			<-ticker.C
			for j := 0; j < p.tokens; j++ {
				tokenCh <- struct{}{}
			}
		}
		b.Logf("shard %d phase %d elapsed: %v", shard, phaseIndex, time.Since(start))
	}
	close(tokenCh)
}

// submitTasks 任务提交函数：从 tokenCh 获取令牌并提交任务
func submitTasks(pool *agilepool.Pool, tokenCh chan struct{}, submitterCount int, wg *sync.WaitGroup, submittedCount *int64) {
	for g := 0; g < submitterCount; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tokenCh {
				atomic.AddInt64(submittedCount, 1)
				pool.Submit(agilepool.TaskFunc(func() error {
					time.Sleep(10 * time.Millisecond)
					return nil
				}))
			}
		}()
	}
}

// burst 20w ~ 150w tasks per sec, 10 shards
func BenchmarkAgilePoolBurstMinHeap(b *testing.B) {
	const (
		submitterCount  = 20000   // 并发提交
		baseRatePerSec  = 200000  // 基础速率/s
		burstRatePerSec = 1500000 // 突发速率/s
		tickInterval    = time.Millisecond
		ticksPerSec     = int(time.Second / tickInterval) // 1000
		basePerTick     = baseRatePerSec / ticksPerSec    // 200
		burstPerTick    = burstRatePerSec / ticksPerSec   // 1500
		numShards       = 10                              // 分片(单chan无法支持大量submitter)
	)

	phases := []phase{
		{ticks: 2 * ticksPerSec, tokens: basePerTick / numShards},  // 0-2s  base
		{ticks: 2 * ticksPerSec, tokens: burstPerTick / numShards}, // 2-4s  burst  ← spike 1
		{ticks: 2 * ticksPerSec, tokens: basePerTick / numShards},  // 4-6s  base
		{ticks: 2 * ticksPerSec, tokens: burstPerTick / numShards}, // 6-8s  burst  ← spike 2
		{ticks: 2 * ticksPerSec, tokens: basePerTick / numShards},  // 8-10s base
	}

	for i := 0; i < b.N; i++ {
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(10000),
			agilepool.WithWorkerNumCapacity(20000),
			agilepool.WithIdleContainerType(agilepool.MinHeapType),
		))

		// 10 shards, each handles 1/10 of tokens and submitters
		submittersPerShard := submitterCount / numShards

		var submittedCount int64
		var submitWG sync.WaitGroup
		for s := 0; s < numShards; s++ {
			tokenCh := make(chan struct{}, 10000)
			go dispenseTokens(b, s, tokenCh, phases, tickInterval)
			submitTasks(pool, tokenCh, submittersPerShard, &submitWG, &submittedCount)
		}

		submitWG.Wait()
		pool.Wait()
		b.Logf("Total submitted tasks: %d", atomic.LoadInt64(&submittedCount))
		pool.Close()
	}
}

func BenchmarkAgilePoolBurstLinkedList(b *testing.B) {
	const (
		submitterCount  = 20000   // 并发提交
		baseRatePerSec  = 200000  // 基础速率/s
		burstRatePerSec = 1500000 // 突发速率/s
		tickInterval    = time.Millisecond
		ticksPerSec     = int(time.Second / tickInterval) // 1000
		basePerTick     = baseRatePerSec / ticksPerSec    // 200
		burstPerTick    = burstRatePerSec / ticksPerSec   // 1500
		numShards       = 10                              // 分片(单chan无法支持大量submitter)
	)

	phases := []phase{
		{ticks: 2 * ticksPerSec, tokens: basePerTick / numShards},  // 0-2s  base
		{ticks: 2 * ticksPerSec, tokens: burstPerTick / numShards}, // 2-4s  burst  ← spike 1
		{ticks: 2 * ticksPerSec, tokens: basePerTick / numShards},  // 4-6s  base
		{ticks: 2 * ticksPerSec, tokens: burstPerTick / numShards}, // 6-8s  burst  ← spike 2
		{ticks: 2 * ticksPerSec, tokens: basePerTick / numShards},  // 8-10s base
	}

	for i := 0; i < b.N; i++ {
		pool := agilepool.NewPool(agilepool.NewConfig(
			agilepool.WithCleanPeriod(500*time.Millisecond),
			agilepool.WithTaskQueueSize(10000),
			agilepool.WithWorkerNumCapacity(20000),
			agilepool.WithIdleContainerType(agilepool.LinkedListType),
		))

		// 10 shards, each handles 1/10 of tokens and submitters
		submittersPerShard := submitterCount / numShards

		var submittedCount int64
		var submitWG sync.WaitGroup
		for s := 0; s < numShards; s++ {
			tokenCh := make(chan struct{}, 10000)
			go dispenseTokens(b, s, tokenCh, phases, tickInterval)
			submitTasks(pool, tokenCh, submittersPerShard, &submitWG, &submittedCount)
		}

		submitWG.Wait()
		pool.Wait()
		b.Logf("Total submitted tasks: %d", atomic.LoadInt64(&submittedCount))
		pool.Close()
	}
}

func BenchmarkNativeGoroutine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		sem := make(chan struct{}, 20000)

		for j := 0; j < taskCount; j++ {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				time.Sleep(10 * time.Millisecond)
			}()
		}
		wg.Wait()
	}
}

func BenchmarkNativeGoroutineNoLimit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for j := 0; j < taskCount; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(10 * time.Millisecond)
			}()
		}
		wg.Wait()
	}
}
