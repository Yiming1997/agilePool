package agilepool

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// 编译期检查：spinLock 实现了 sync.Locker 接口
var _ sync.Locker = (*spinLock)(nil)

// spinLock 是一个用户态自旋锁。
//
// 设计要点：
//   两阶段等待策略 ——
//     Phase 1（快路径）：纯 CAS 循环，每 16 次加一次 Gosched。
//         适合持锁时间极短（~100ns）的场景，比如 RingQueue.Pop() 只改一个下标。
//         每 16 次 yield 防止 CAS 风暴打满 cache-coherency 总线拖慢持锁者。
//     Phase 2（慢路径）：CAS + runtime.Gosched() 指数退避（1→2→4→8→16）。
//         最多重试 1024 轮后降级为纯 Gosched 等待，防止持锁者被 OS 抢占时
//         其他 goroutine 无限空转。
//
//   对比 sync.Mutex：
//     sync.Mutex 内部自旋 ~30 次 PAUSE 后进入 futex sleep。
//     对于持锁 <1μs 的场景，futex 的 sleep/wake 开销（~1-5μs）比等锁本身还久。
//     spinLock 不 sleep，用 CAS 硬等 —— 持锁越短优势越大。
//
// 注意：
//   - 不可重入
//   - 持锁超过 ~100μs 时 sync.Mutex（sleep 省 CPU）比 spinLock 更好
//   - 生产环境建议：短持锁容器（RingQueue）+ SpinLock；长持锁容器（Slice）+ Mutex
type spinLock struct {
	state uint32 // 0=空闲, 1=已持有
}

// newSpinLock 创建一个新的自旋锁。
func newSpinLock() *spinLock {
	return &spinLock{}
}

// NewSpinLock 创建一个新的自旋锁，返回 sync.Locker 接口。
// 供外部测试或需要直接使用自旋锁的场景调用。
func NewSpinLock() sync.Locker {
	return &spinLock{}
}

// Lock 获取锁，阻塞直到成功。
func (s *spinLock) Lock() {
	// —— Phase 1：快路径，CAS + 间歇 yield ——
	// 纯 CAS 约 ~1ns/次，100 次 = ~100ns。每 16 次插一个 Gosched，
	// 防止 CAS 写风暴拖慢持锁者（cache line bouncing）。
	const (
		fastSpin       = 100
		yieldInterval  = 16 // 每 16 次 CAS 让一次 P
	)
	for i := 0; i < fastSpin; i++ {
		if atomic.CompareAndSwapUint32(&s.state, 0, 1) {
			return
		}
		if i%yieldInterval == 0 {
			runtime.Gosched()
		}
	}

	// —— Phase 2：慢路径，指数退避 + Gosched ——
	backoff := 1
	const maxBackoff = 16
	const maxRounds = 1024 // 安全上限，超过后降级为纯 Gosched 等待

	for round := 0; round < maxRounds; round++ {
		if atomic.CompareAndSwapUint32(&s.state, 0, 1) {
			return
		}
		for i := 0; i < backoff; i++ {
			runtime.Gosched()
		}
		if backoff < maxBackoff {
			backoff <<= 1
		}
	}

	// —— Phase 3：兜底，持锁者可能被 OS 抢占，降级为纯 Gosched 避免 CPU 空转 ——
	for !atomic.CompareAndSwapUint32(&s.state, 0, 1) {
		runtime.Gosched()
	}
}

// Unlock 释放锁。原子写回，不需要 CAS（只有持锁者调用）。
func (s *spinLock) Unlock() {
	atomic.StoreUint32(&s.state, 0)
}
