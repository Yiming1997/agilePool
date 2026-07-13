// 测试目标：衡量 sync.Mutex 在"长持锁 vs 短持锁"场景下的性能差距
//
// 背景：
//   Pool 里有一个 muIdle sync.Mutex，保护空闲 worker 容器的增删查。
//   每次 worker 完成手头任务后会调用 addToIdle(w) 把自己放回空闲容器（拿锁→Add→放锁）。
//   每次 scaler 需要唤醒空闲 worker 时调用 Pop()（拿锁→Pop→放锁）。
//   高并发下几千个 goroutine 同时拿这把锁，锁竞争就成了吞吐瓶颈。
//
// 如何制造"长持锁"和"短持锁"的对比：
//   - Slice 容器的 Pop() 是 O(n)：底层 s.workers = s.workers[1:] 会搬移整个数组。
//     当空闲容器里有 5 万个 worker 时，每一次 Pop 就要搬移 5 万个指针（~400KB），
//     持锁时间 = 搬移耗时，所有其他争用者都得等着。
//   - RingQueue 容器的 Pop() 是 O(1)：只改 head 下标，持锁时间极短。
//
// 测试流程（run 函数）：
//   1. 创建 Pool，指定容器类型（Slice 或 RingQueue），容量 5 万
//   2. 启动 5000 个并发 submitter，疯狂提交任务（共计 50 万个）
//   3. 每个任务耗时 20ms（time.Sleep），模拟真实业务
//   4. 池子会自动扩缩容：worker 不够就 Pop 空闲的 / 创建新的，worker 多了就回 idle
//   5. 这个"完成→回idle→被Pop→执行"的循环会高频撞击 muIdle，形成锁竞争
//   6. 对比两种容器的总耗时，差值就是长持锁导致的额外损耗
package agilepool_test

import (
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

const (
	// pool 最多允许 5 万个 worker 同时运行
	// 设大一点，确保大量 worker 同时在线 → 空闲容器很大 → O(n) 代价明显
	workerCap = 50000

	// 总共提交 50 万个任务，每个 20ms
	// 50万 × 20ms = 10000 秒的 CPU 时间，用 5 万 worker 并行 ≈ 理论 200ms+
	// 实际因为锁竞争、调度开销会到 600-800ms，足够看出差距
	totalTasks = 500000

	// 5000 个 goroutine 同时 submit，制造高并发争锁
	concurrentSubmitters = 5000

	// 单个任务耗时 —— 不能太短（否则 worker 瞬间完成，还没形成竞争就结束了）
	// 不能太长（否则测试跑太久）。20ms 是一个合理的中间值
	taskDuration = 20 * time.Millisecond
)

// run 执行一轮压测，返回从 Submit 开始到所有任务执行完毕的总耗时。
//
// 参数：
//   ct   — 空闲容器类型（Slice=O(n)Pop长持锁, RingQueue=O(1)Pop短持锁）
//   lt   — muIdle 锁类型（MutexLock=sync.Mutex, SpinLock=自旋锁）
//   label — 输出表格用的场景名
func run(tb testing.TB, ct agilepool.IdleContainerType, lt agilepool.LockType, label string) time.Duration {
	pool := agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithWorkerNumCapacity(workerCap),
		agilepool.WithIdleContainerType(ct),
		agilepool.WithLockType(lt), // ← 关键：按参数选择锁类型
		agilepool.WithCleanPeriod(500*time.Millisecond),
	))
	defer pool.Close()

	// 原子计数器：submitted=已提交数，completed=已完成数
	var submitted, completed atomic.Int64

	// 采样峰值：记录测试期间同时运行的最大 worker 数
	var peakRunning atomic.Int64
	stopSampler := make(chan struct{})
	defer close(stopSampler) // 确保 goroutine 退出，避免泄漏
	go func() {
		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				if r := pool.GetRunningWorkersNum(); r > peakRunning.Load() {
					peakRunning.Store(r)
				}
			case <-stopSampler:
				return
			}
		}
	}()

	// ---------- 启动并发 submitter ----------
	start := time.Now()
	for g := 0; g < concurrentSubmitters; g++ {
		go func() {
			for {
				// 原子自增拿到一个"票号"，超出总量就退出
				n := submitted.Add(1)
				if n > totalTasks {
					return
				}
				// 提交一个耗时 20ms 的任务
				// ↓ 这里会高频触发 Submit → scaler → muIdle 竞争
				pool.Submit(agilepool.TaskFunc(func() error {
					time.Sleep(taskDuration)
					completed.Add(1)
					return nil
				}))
			}
		}()
	}

	// ---------- 等待全部完成（带超时保护）----------
	deadline := time.After(120 * time.Second)
	for completed.Load() < totalTasks {
		select {
		case <-deadline:
			tb.Fatalf("超时：仅完成 %d/%d 个任务", completed.Load(), totalTasks)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
	elapsed := time.Since(start)
	pool.Wait() // 确保最后一个任务也完全结束

	// ---------- 输出一行结果 ----------
	tps := float64(totalTasks) / elapsed.Seconds()
	tb.Logf("%-32s | %8v | %10.0f t/s | peak=%-5d idle=%-5d created=%-7d",
		label,                             // 场景名
		elapsed.Round(time.Millisecond),   // 总耗时
		tps,                               // 吞吐量（每秒完成的任务数）
		peakRunning.Load(),                // 峰值并发 worker 数
		pool.GetIdleWorkerCount(),         // 结束时空闲 worker 数
		pool.GetWorkerCreateCount(),       // 累计创建的 worker 数
	)
	return elapsed
}

// TestMuIdleContentionReport 对比 4 种组合：
//   容器类型（Slice=长持锁 / RingQueue=短持锁）× 锁类型（Mutex / SpinLock）
func TestMuIdleContentionReport(t *testing.T) {
	t.Log("")
	t.Log("==============================================================================")
	t.Log("  muIdle 锁竞争压测：容器类型 × 锁类型，四象限对比")
	t.Logf("  规模：%d万任务 × %dms | %d并发submitter | 容量%d",
		totalTasks/10000, taskDuration/time.Millisecond, concurrentSubmitters, workerCap)
	t.Log("----------------------------------------------------------------------")
	t.Log("  Slice.Pop() = O(n) 搬移指针（长持锁）vs RingQueue.Pop() = O(1)（短持锁）")
	t.Log("  sync.Mutex = 内核futex（持锁久时省CPU）vs 自旋锁 = CAS循环（持锁短时快）")
	t.Log("==============================================================================")
	t.Log("")
	t.Logf("%-38s | %8s | %10s | %s", "场景", "耗时", "吞吐", "峰值/空闲/累计创建")
	t.Logf("%-38s-+-%-8s-+-%-10s-+-%s",
		"--------------------------------------", "--------", "----------", "---------------------")

	// ---- 四象限测试 ----

	// Q1：长持锁 + sync.Mutex   → 锁等得久 + 内核futex sleep → 预期最慢
	e1 := run(t, agilepool.SliceType, agilepool.MutexLock, "Slice+Mutex   (长持锁+内核锁)")

	// Q2：长持锁 + 自旋锁        → 锁等得久 + CPU自旋不sleep → 预期改善
	e2 := run(t, agilepool.SliceType, agilepool.SpinLock, "Slice+SpinLock(长持锁+自旋锁)")

	// Q3：短持锁 + sync.Mutex   → 锁瞬间释放 + 内核futex sleep → 预期较快
	e3 := run(t, agilepool.RingQueueType, agilepool.MutexLock, "RingQueue+Mutex  (短持锁+内核锁)")

	// Q4：短持锁 + 自旋锁        → 锁瞬间释放 + CPU自旋不sleep → 预期最快（baseline）
	e4 := run(t, agilepool.RingQueueType, agilepool.SpinLock, "RingQueue+SpinLock(短持锁+自旋锁)")

	// ---- 对比结论 ----
	t.Log("")
	t.Log("----------------------------------------------------------------------")
	t.Log("  对比一：Slice 长持锁场景，SpinLock 相比 Mutex 的提升")
	t.Logf("    耗时: %v → %v  (%.0f%% 提升)",
		e1.Round(time.Millisecond), e2.Round(time.Millisecond),
		(1-float64(e2)/float64(e1))*100)
	t.Log("")
	t.Log("  对比二：RingQueue 短持锁场景，SpinLock 相比 Mutex 的提升")
	t.Logf("    耗时: %v → %v  (%.0f%% 提升)",
		e3.Round(time.Millisecond), e4.Round(time.Millisecond),
		(1-float64(e4)/float64(e3))*100)
	t.Log("")
	t.Log("  结论：持锁时间越长 + 竞争越激烈 → 自旋锁优势越大")
	t.Log("       持锁时间极短时 spinlock 和 mutex 差距不大（Go mutex 内部也会自旋）")
	t.Log("----------------------------------------------------------------------")
}
