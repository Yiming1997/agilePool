
## Test Harness（test/）

The `test/` directory provides a standalone `package main` entry point for benchmarking, metric collection, and performance analysis of agilePool under real workloads.

### Why a separate test harness?

- Captures system-level metrics over time: **memory, GC, goroutines, CPU**
- Easily compares pool configurations (container type, work mode, worker count)
- Outputs CSV/JSON for analysis with `plot_csv.py` or `go tool pprof`
- Edit `run_test.bat` / `run_test.sh` to change parameters quickly

### Design

Task duration generation (`--task-type`/`-T`) and submission control (`--submit-type`/`-U`) are decoupled and freely composable:

| Task Type | Description |
|-----------|-------------|
| `fixed` | constant `--task-base` ms |
| `uniform` | `--task-base` + rand × `--task-extra` ms |
| `normal` | N(`--task-mean`, `--task-sigma`) ms |

| Submit Type | Description |
|-------------|-------------|
| `immediate` | submit all N at once, wait for completion |
| `linear` | `--submit-interval` + rand × `--submit-jitter` ms |
| `constant` | fixed `--submit-interval` ms |
| `poisson` | mean interval `--submit-mean-interval` ms |
| `phased` | multi-phase burst: `-P "offset,dur,rate;..."` |

### Usage

```bash
cd test
go build -o agilepool_test.exe .
agilepool_test.exe -T fixed --task-base 500 -U immediate -t 200000 -w 10000 -i 1 -f csv
```

Or run `run_test.bat` / `run_test.sh` for a full test suite.

### CLI Reference

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--workers` | `-w` | `20000` | max concurrent workers |
| `--tasks` | `-t` | `1000000` | total tasks to submit |
| `--clean-period` | | `500ms` | idle worker cleanup interval |
| `--queue-size` | | `10000` | task queue size hint |
| `--container` | `-c` | `linkedlist` | idle container: linkedlist, minheap, slice, ringqueue |
| `--mode` | `-m` | `block` | work mode: block, nonblock |
| `--task-type` | `-T` | `fixed` | task type: fixed, uniform, normal |
| `--task-base` | | `10` | base task duration (ms) |
| `--task-extra` | | `0` | random extra range (ms, uniform) |
| `--task-mean` | | `10` | normal mean (ms) |
| `--task-sigma` | | `5` | normal stddev (ms) |
| `--submit-type` | `-U` | `immediate` | submit mode: immediate, linear, constant, poisson, phased |
| `--submit-interval` | | `10` | submit interval (ms) |
| `--submit-jitter` | | `0` | random jitter (ms, linear) |
| `--submit-mean-interval` | | `50` | poisson mean interval (ms) |
| `--submit-phases` | `-P` | `""` | phases: "offset,dur,rate;..." (phased) |
| `--submit-shards` | | `1` | parallel shards (phased) |
| `--take-time` | `-i` | `0` | metric sampling interval (seconds, 0=disabled) |
| `--log-file` | `-o` | auto | output file path |
| `--log-format` | `-f` | `csv` | output format: csv, json |
| `--cpuprofile` | | `false` | enable CPU profiling |
| `--memprofile` | | `false` | enable memory profiling |
| `--wait-exit` | `-e` | `0` | extra seconds to wait before exit |

### Collected Metrics

When `--take-time` (`-i`) > 0, a background goroutine samples these fields at the given interval:

| CSV Column | Source | Description |
|------------|--------|-------------|
| `run_sec` | `time.Since(start).Seconds()` | seconds since program start |
| `goroutines` | `runtime.NumGoroutine()` | current goroutine count |
| `heap_alloc_mb` | `memStats.Alloc` | current heap allocation (MB) |
| `total_alloc_mb` | `memStats.TotalAlloc` | cumulative allocation (MB) |
| `sys_mb` | `memStats.Sys` | memory obtained from OS (MB) |
| `gc_total` | `memStats.NumGC` | total GC cycles |
| `gc_pause_total_ms` | `PauseTotalNs / 1e6` | total GC pause time (ms) |
| `gc_pause_avg_ms` | total / count | average pause per GC (ms) |
| `gc_cpu_pct` | `GCCPUFraction * 100` | GC CPU time fraction (%) |
| `last_gc_sec` | `(LastGC - start) / 1e9` | last GC's relative time (seconds) |
| `next_gc_mb` | `memStats.NextGC` | heap threshold for next GC (MB) |
| `workers_running` | `pool.GetRunningWorkersNum()` | currently executing workers |
| `workers_idle` | `pool.GetIdleWorkerCount()` | currently idle workers |
| `workers_created` | `pool.GetWorkerCreateCount()` | total workers created |
| `task_queue_len` | `pool.GetTaskQueueLen()` | pending tasks in queue |
| `cpu_pct` | `gopsutil cpu.Percent` | process CPU usage (%) |

### Plotting

```bash
python plot_csv.py
```

Scans all `metrics_*.csv` and generates PNG charts (memory / workers / GC / CPU subplots).

### Three-Stage Shutdown

After tasks complete:
1. Poll until all workers are idle (`GetRunningWorkersNum() == 0`)
2. If sampling is enabled, wait one interval for final metric flush
3. If `--wait-exit` is set, wait extra seconds for memory observation

### Profiling

```bash
agilepool_test.exe -T fixed --task-base 500 -U immediate -t 500000 -w 20000 --cpuprofile --memprofile -i 1 -f csv
go tool pprof -http=:8080 cpu_profile.prof
go tool pprof -http=:8080 mem_profile.prof
```

### One-Click Run

```bash
cd test
run_test.bat        # Windows
./run_test.sh       # Linux/macOS
```
