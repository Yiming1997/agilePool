
## 测试工具（test/）

项目在 `test/` 目录下提供了一个独立测试入口包（`package main`），用于在实际运行场景下对 pool 进行基准测试、指标采集和性能分析。

### 为什么编写独立的测试工具？

- 采集运行时的**内存、GC、goroutine、CPU**等系统级指标的时间变化
- 对比不同池配置（容器类型、工作模式、worker 数量）对性能的影响
- 输出 CSV/JSON，配合 `plot_csv.py` 或 `go tool pprof` 分析
- 修改 `run_test.bat` / `run_test.sh` 即可快速更改参数

### 设计思路

任务时长生成（`--task-type`/`-T`）与提交控制（`--submit-type`/`-U`）解耦：

| 任务类型 | 说明 |
|---------|------|
| `fixed` | 固定时长 `--task-base` ms |
| `uniform` | `--task-base` + rand × `--task-extra` ms |
| `normal` | N(`--task-mean`, `--task-sigma`) ms |

| 提交类型 | 说明 |
|---------|------|
| `immediate` | 一次性全部提交 |
| `linear` | `--submit-interval` + rand × `--submit-jitter` ms |
| `constant` | 固定 `--submit-interval` ms |
| `poisson` | 平均间隔 `--submit-mean-interval` ms |
| `phased` | 多阶段突发：`-P "offset,dur,rate;..."` |

### 使用方式

```bash
cd test
go build -o agilepool_test.exe .
agilepool_test.exe -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -i 1 -f csv
```

也可直接运行 `run_test.bat` 或 `run_test.sh`。

### 命令行参数

| 参数 | 短形式 | 默认值 | 说明 |
|------|--------|--------|------|
| `--workers` | `-w` | `20000` | 最大 worker 数 |
| `--tasks` | `-t` | `1000000` | 提交任务总数 |
| `--clean-period` | | `500ms` | 空闲 worker 清理周期 |
| `--queue-size` | | `10000` | 任务队列大小提示 |
| `--container` | `-c` | `linkedlist` | 空闲容器类型 |
| `--mode` | `-m` | `block` | 工作模式 |
| `--task-type` | `-T` | `fixed` | 任务时长类型 |
| `--task-base` | | `10` | 基准时长（ms） |
| `--task-extra` | | `0` | 随机范围（ms, uniform） |
| `--task-mean` | | `10` | 正态均值（ms） |
| `--task-sigma` | | `5` | 正态标准差（ms） |
| `--submit-type` | `-U` | `immediate` | 提交模式 |
| `--submit-interval` | | `10` | 提交间隔（ms） |
| `--submit-jitter` | | `0` | 随机抖动（ms, linear） |
| `--submit-mean-interval` | | `50` | 泊松平均间隔（ms） |
| `--submit-phases` | `-P` | `""` | 多阶段编排（phased） |
| `--submit-shards` | | `1` | 并行分片数 |
| `--take-time` | `-i` | `0` | 采集间隔秒数（0=禁用） |
| `--log-file` | `-o` | 自动 | 输出路径 |
| `--log-format` | `-f` | `csv` | 输出格式 |
| `--cpuprofile` | | `false` | CPU profiling |
| `--memprofile` | | `false` | Memory profiling |
| `--wait-exit` | `-e` | `0` | 退出前额外等待秒数 |

### 指标采集

当 `-i` > 0 时启动后台协程，按间隔采集以下字段：

| CSV 列 | 来源 | 说明 |
|--------|------|------|
| `run_sec` | `time.Since(start).Seconds()` | 运行秒数 |
| `goroutines` | `runtime.NumGoroutine()` | goroutine 数 |
| `heap_alloc_mb` | `memStats.Alloc` | 当前堆分配（MB） |
| `total_alloc_mb` | `memStats.TotalAlloc` | 累计分配（MB） |
| `sys_mb` | `memStats.Sys` | 系统内存（MB） |
| `gc_total` | `memStats.NumGC` | GC 总次数 |
| `gc_pause_total_ms` | `PauseTotalNs / 1e6` | GC 暂停总耗时（ms） |
| `gc_pause_avg_ms` | 总耗时 / 次数 | GC 平均暂停（ms） |
| `gc_cpu_pct` | `GCCPUFraction * 100` | GC CPU 占比（%） |
| `last_gc_sec` | `(LastGC - start) / 1e9` | 上次 GC 时间（秒） |
| `next_gc_mb` | `memStats.NextGC` | 下次 GC 阈值（MB） |
| `workers_running` | `pool.GetRunningWorkersNum()` | 运行中 worker |
| `workers_idle` | `pool.GetIdleWorkerCount()` | 空闲 worker |
| `workers_created` | `pool.GetWorkerCreateCount()` | 累计创建 worker |
| `task_queue_len` | `pool.GetTaskQueueLen()` | 队列长度 |
| `cpu_pct` | `gopsutil cpu.Percent` | CPU 使用率（%） |

### 绘图

```bash
python plot_csv.py
```

扫描 `metrics_*.csv` 生成 PNG 折线图。

### 三段式退出

1. 轮询至所有 worker 空闲
2. 等待一个采集周期完成最终写入
3. 若有 `--wait-exit`，等待额外秒数

### 性能分析

```bash
agilepool_test.exe -T fixed --task-base 500 -U immediate -t 500000 -w 20000 --cpuprofile --memprofile -i 1 -f csv
go tool pprof -http=:8080 cpu_profile.prof
go tool pprof -http=:8080 mem_profile.prof
```

### 一键运行

```bash
run_test.bat        # Windows
./run_test.sh       # Linux/macOS
```
