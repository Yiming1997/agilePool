# 钩子系统稳定性测试报告

日期:2026-09-09
范围:test/ 插件式测试工具的钩子稳定性(h*)插件家族 + test 模块本地化引用
分支:`feature-test-plugin`(提交后基于其新建 `feature-hook-stability`)

## 1. 背景与目标

go-agile-pool 提供 5 类生命周期钩子(OnTaskSubmitted / OnTaskEnqueued /
OnTaskStarted / OnTaskCompleted / OnPoolClosed)。本次为 test 工具新增一批
**钩子稳定性压测插件**,目标是验证钩子系统在对抗性条件下的稳定性契约:

- 事件不丢、不重(精确记账);
- 钩子 panic 不击穿提交/执行/关闭路径;
- 单任务事件序与上下文载荷保真;
- 慢钩子、重入钩子、并发注册、Close 竞态下的行为;
- 全程无 goroutine 泄漏、无数据竞争(-race)。

测试只验证**受支持的注册契约**:钩子仅在启动期注册(可多协程并发),任务
开始后回调列表即冻结;运行中 `Add*` 属于越界用法,不在测试范围内。

## 2. 方法

- `test/` 拆分为独立 Go 模块 `github.com/Yiming1997/agilePool/v2/test`,
  `go.mod` 用 `replace => ../` 把库引用指向仓库根目录(离线、严格本地库);
  测试使用公开的 `hook` 包(与外部使用者同一入口)。
- 每个 h* 插件自建私有池(`queue == 任务数`,走直通 channel 路径,
  保证 Enqueued 每任务恰触发一次),断言 PASS/FAIL,失败退出码 1。
- 公共机制集中在 `test/hookcheck.go`(场景池、原子计数器、drain/goroutine
  泄漏等待、PASS/FAIL 报告)。
- 回归脚本:`run_hook_stress.bat` / `run_hook_stress.sh`(参数 `race` 时
  以 `-race` 构建)。

## 3. 测试插件与覆盖点

| 插件 | 施压方式 | 核心断言(均通过) |
|---|---|---|
| `hcount` | N 钩子 × M 任务满并发 | 每事件计数 == N·M;执行数 == M;无泄漏 |
| `hpanic` | 每阶段:callback 层 panic / 整层 dispatch panic | 任务照常执行、可排空、Close 正常;摘钩后补发任务正常;close 后提交静默 |
| `horder` | 2 万任务(含 2 千 panic 任务)带 ctx id | Submitted 首事件、Started 先于 Completed、无重复;ctx 载荷全事件一致;panic 值原样到 OnTaskCompleted |
| `hctx` | 存活标记波 / 预取消波 / 排队中取消波 | ctx 贯穿全事件;预取消零事件;取消后每出队任务 Started==Completed 且正常排空 |
| `hblock` | 每钩子 sleep(默认 200µs) | 不死锁、四计数精确 |
| `hchurn` | 4 goroutine 启动期注册风暴 100 钩子 | 首发钩子每事件 == num;风暴钩子每事件 == num×100;换新 Hooks 后仍精确 |
| `hreenter` | 钩子内再提交任务(budget/depth 受限) | 原任务+重入任务全周期计数精确;排空无死锁 |
| `hclose` | 串行/4 路并发 Close、panic 的 OnPoolClosed、close 后提交 | OnPoolClosed 恰好一次、池指针一致;竞态 Close 不重复;关闭后零事件 |

## 4. 执行结果

- 普通构建(`go build`):8 场景全 PASS,`run_hook_stress.bat` 端到端
  退出码 0(汇总行:`hook-stability suite: all PASS`)。
- 竞态检测(`go build -race`):8 场景全部干净(无 DATA RACE)。
- 根模块回归:`go test ./...`(38.5s + 22.9s benchmark 包)全部通过;
  钩子相关单测(`TestHook*Panic*` 系列)通过。

## 5. 关键结论

1. **注册契约**:`hook.Hooks` 的 `Add*` 仅用于启动期(多协程并发
   注册由互斥锁保证);分发读取已冻结的列表。任务运行中注册不属于契约,
   库未做任何改动。
2. **panic 双保险成立**:callback 层(库内 `invoke` 逐个 recover)与
   dispatch 层(`Pool.dispatchHook` 整体 recover)均能保证 worker 与
   提交路径的簿记(done)不被跳过。
3. **跨 goroutine 顺序边界**:Enqueued 与 Started 之间无跨 goroutine 的
   happens-before 保证(worker 可能先于提交方 enqueue 回调取走任务),
   h* 断言只依赖真实的程序序保证(Submitted 首个、Started<Completed)。
4. **取消语义**:排队中被取消的任务仍会走 Started→Completed(主体跳过),
   每出队任务 Started==Completed 恒成立;预取消的 ctx 在进入钩子前即被拒绝。
5. **关闭语义**:Close 由 CAS 保证 OnPoolClosed 恰好一次(含并发 Close);
   关闭后提交被静默丢弃、不触发任何钩子事件、wg 平衡。

## 6. 局限与后续

- `hpanic level=callback` 会按设计把每次被 recover 的 panic 打到 stderr
  (库内 Hooks logger 行为),脚本中以 `2>nul`/`2>/dev/null` 静默。
- 未覆盖 `internal/context` 追踪接线(`--hook mode=trace` 仍被显式拒绝)。
- 本批插件是正确性/稳定性验证,非性能基准;分发开销测量仍由
  `--hook mode=hook` 承担。
- 测试间互不依赖、可单独运行;给插件的 `-race` 支持已在脚本内留好入口。
