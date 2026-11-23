# Opt-in hook collector: three-way throughput comparison

Measured on 2026-11-23. This compares the upstream pool, the revised pool with
no collector registered, and the revised pool with collector callbacks registered.
No metrics scrapes run during these task-throughput measurements.

## Setup

- Upstream baseline: `2eaf39a8d6ebb7b69943f43931860fbafc3a726c`.
- Candidate: the hook-based Collector implementation in this change.
- Windows/amd64, AMD Ryzen 7 8845H, Go 1.26.5.
- BLOCK mode, 128-worker ceiling, 4096 configured handoff slots, automatic sharding.
- GOMAXPROCS 1, 8, and 16; one parallel producer per logical processor.
- No-op, CPU (16 SHA-256 hashes of a 256-byte input), and requested 100-microsecond
  sleep workloads. Actual sleep duration depends on Windows scheduling.
- The identical common benchmark fixture is compiled in all three binaries.
  The enabled binary additionally includes `metrics_enabled_benchmark_test.go`.
  It registers the Collector with `hook.NewHooks()` before the first submission.
- Pool setup and a 4096-task warmup are outside timing. Timing includes both
  submission and draining accepted tasks, so ns/op describes aggregate throughput,
  not per-task latency percentiles.
- Ten independent process samples per configuration and version, one-second
  benchmark target. The six run orders ABC/BCA/CAB/CBA/BAC/ACB are cycled across
  rounds (A upstream, B disabled, C enabled). Versions run sequentially; no other
  builds or pressure tests run concurrently.
- Analysis: `golang.org/x/perf/cmd/benchstat` at
  `v0.0.0-20261117185222-4febdb082b2c`, installed as a temporary validation tool.

The build tag controls only this benchmark's setup. Applications enable task
counting at runtime by registering Collector callbacks, with no build tag needed.

## Results

Median ns/op and enabled time changes relative to upstream:

| Workload | GOMAXPROCS | Upstream | Disabled | Disabled p-value | Enabled | Enabled time change | Enabled p-value |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| No-op | 1 | 38.91 | 39.02 | 0.811 | 67.33 | +73.06% | <0.001 |
| No-op | 8 | 94.30 | 93.50 | 0.101 | 109.95 | +16.59% | <0.001 |
| No-op | 16 | 84.56 | 85.59 | 0.481 | 98.71 | +16.73% | <0.001 |
| CPU | 1 | 2709 | 2725 | 0.912 | 2748 | no significant difference | 0.066 |
| CPU | 8 | 385.9 | 385.6 | 0.839 | 392.9 | +1.81% | 0.037 |
| CPU | 16 | 231.9 | 228.4 | 0.616 | 238.7 | no significant difference | 0.184 |
| Sleep 100us | 1 | 4462 | 4454 | 0.517 | 4530 | +1.54% | 0.005 |
| Sleep 100us | 8 | 4251 | 4232 | 0.591 | 4272 | no significant difference | 0.093 |
| Sleep 100us | 16 | 4267 | 4253 | 0.288 | 4267 | no significant difference | 0.617 |

No statistically significant disabled-versus-upstream difference was detected in
these nine configurations. With counting enabled, the no-op workload shows a
measurable cost: +16.73% time/op at 16 logical processors corresponds to about
14.3% lower throughput; +73.06% at one processor corresponds to about 42.2% lower
throughput. Enabled counting is therefore not free and can cost more than the
initial unconditional-counter prototype, particularly for tiny tasks. This
revision moves that cost to users who opt in. The two reports were measured in
separate runs, so they are not a controlled direct comparison between prototypes.

No increase in reported median B/op or allocs/op was observed. Per-run allocation
numbers are rounded by Go's benchmark output and do not establish exact allocation
equivalence for every execution.

Raw samples: [upstream.txt](upstream.txt), [disabled.txt](disabled.txt),
[enabled.txt](enabled.txt), [benchstat.txt](benchstat.txt). Only trailing whitespace
is normalized; timing and allocation measurements are unchanged. Benchstat's
rounded `p=0.000` values are displayed as `<0.001` above. Source fingerprints for
the compiled implementation and fixture are in [source-hashes.json](source-hashes.json).

## Interpretation and limits

The revision removes unconditional lifetime-counter increments from Pool and
restores worker execution to upstream code. With no collector registered, the
accepted-task and worker paths perform no added metric increments. Rejection
exits still check the optional `RejectedHooks` extension, even without a collector.
This is not a claim of literally zero extra operations on every possible path.

The enabled result includes the existing hook dispatcher and collector counting
cost. It does not isolate individual atomic operations. Both enabled and disabled
results cover one machine and these BLOCK-mode acceptance workloads. Rejection
throughput, other configurations, operating systems, and concurrent scraping are
not measured here. `BenchmarkWriteText` measures exporter cost separately.

Benchstat reports medians, confidence intervals and pairwise p-values relative
to upstream. A non-significant result is not proof of identical performance.
Throughput change is `upstream_ns_per_op / candidate_ns_per_op - 1`, rather than
the negation of time/op change. Small sleep differences should not be interpreted
as a general I/O speedup.

The [initial always-on prototype report](../prometheus-metrics/README.md) remains
available as historical evidence; its approximately 6% no-op throughput reduction
does not describe this opt-in implementation.

## Reproduction

Create a detached baseline worktree at the upstream commit above and copy the
current `benchmark/metrics_overhead_benchmark_test.go` into its benchmark directory.
Build the three executables using the same toolchain:

```text
# In the baseline checkout:
go test -c -o upstream.exe ./benchmark
# In the candidate checkout:
go test -c -o disabled.exe ./benchmark
go test -c -tags metrics_enabled -o enabled.exe ./benchmark
```

Run each executable once per round, following the rotating order above, with:

```text
-test.run=^$ -test.bench=^BenchmarkMetricsOverhead$ -test.benchmem
-test.benchtime=1s -test.cpu=1,8,16 -test.count=1 -test.timeout=3m
```

Append each version's stdout to its own UTF-8 file, then run:

```text
benchstat upstream.txt disabled.txt enabled.txt
```
