// spinlock 纯锁性能对比：sync.Mutex vs spinLock
//
// N 个 goroutine 同时抢同一把锁，每个做 K 次 Lock→原子加→Unlock。
// 测量总耗时 → 计算每秒完成的 Lock/Unlock 对数。
// 对比不同并发度下两种锁的吞吐差距。
package agilepool_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
)

const (
	opsPerG = 500_000 // 每个 goroutine 做 50 万次 Lock→临界区→Unlock
)

// benchLock 对一把锁做并发压测，返回总耗时。
//
// 参数：
//
//	lock       — 被测锁（sync.Mutex 或 spinLock 都实现了 sync.Locker）
//	goroutines — 同时有多少个 goroutine 在抢这把锁
//	label      — 输出表格用的名称
//
// 每个 goroutine 执行 opsPerG 次循环：抢锁 → 原子加 counter → 放锁。
// counter++ 用原子操作不用普通赋值，因为普通赋值在锁外也能做，无法证明
// 锁确实被正确持有了；原子加在锁外做会丢更新，counter 最终值 ≠ G×500000。
// 所以 counter 既充当校验码（证明没丢锁），也模拟了真实临界区的工作量。
func benchLock(tb testing.TB, lock sync.Locker, goroutines int, label string) time.Duration {
	var counter int64 // 共享的原子计数器，在锁内自增（锁外自增会丢，用来校验正确性）

	// ---------- 启动 N 个 goroutine 同时抢锁 ----------
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				lock.Lock()   // 抢锁
				counter++     // 临界区：原子加 = 模拟 idle 容器的 Add/Pop 操作
				lock.Unlock() // 放锁
			}
		}()
	}
	wg.Wait() // 等所有 goroutine 跑完
	elapsed := time.Since(start)

	// ---------- 统计吞吐 ----------
	// totalOps = goroutine数 × 每goroutine循环数 × 2（Lock+Unlock 各算一次操作）
	totalOps := int64(goroutines) * int64(opsPerG) * 2
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	// ---------- 输出一行结果 ----------
	tb.Logf("%-28s | G=%-3d | %8v | %12.0f ops/s | counter=%d",
		label,                           // 锁类型名
		goroutines,                      // 并发 goroutine 数
		elapsed.Round(time.Microsecond), // 总耗时
		opsPerSec,                       // 每秒完成的 Lock+Unlock 次数
		atomic.LoadInt64(&counter),      // 校验值，必须等于 goroutines×opsPerG
	)
	return elapsed
}

func TestSpinLockVsMutex(t *testing.T) {
	t.Log("")
	t.Log("==============================================================================")
	t.Log("  纯锁竞争压测：sync.Mutex vs spinLock")
	t.Logf("  每个 goroutine %d 次 Lock→临界区→Unlock，多个 goroutine 抢同一把锁", opsPerG)
	t.Log("==============================================================================")
	t.Log("")
	t.Logf("%-28s | %-4s | %8s | %14s | %s", "锁类型", "G数", "耗时", "吞吐(ops/s)", "counter校验")
	t.Logf("%-28s-+-%-4s-+-%-8s-+-%-14s-+-%s",
		"----------------------------", "----", "--------", "--------------", "-----------")

	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		benchLock(t, &sync.Mutex{}, n, "sync.Mutex")
		benchLock(t, agilepool.NewSpinLock(), n, "spinLock")
	}

	t.Log("")
	t.Log("==============================================================================")
	t.Log("  解读：")
	t.Log("  - G=1  无竞争，对比 Lock/Unlock 指令开销（spinLock 更轻量）")
	t.Log("  - G>=4 有竞争，对比等待策略（Mutex→futex sleep, spinLock→CAS 自旋）")
	t.Log("  - counter 必须等于 G×500000，证明没有丢操作")
	t.Log("==============================================================================")
}
