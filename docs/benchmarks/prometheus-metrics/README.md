# Prometheus metrics: task throughput A/B

> Historical result for the unconditional-counter prototype at `005899b`.
> Superseded by the [opt-in collector comparison](../prometheus-metrics-optin/README.md).
> The raw measurements below are retained unchanged and do not describe the current implementation.

Measured on 2026-11-22. This compares the original pool with the lifetime-counter
implementation in this change, without scraping metrics during the run.

## Setup

- Baseline: `2eaf39a8d6ebb7b69943f43931860fbafc3a726c` (upstream `main`).
- Candidate: the initial feature implementation at `005899b`; no scheduler changes.
- Windows/amd64, AMD Ryzen 7 8845H, Go 1.26.5.
- Identical `benchmark/metrics_overhead_benchmark_test.go` compiled in each checkout.
- BLOCK mode, 128-worker ceiling, 4096 configured handoff slots, automatic sharding.
- GOMAXPROCS 1, 8, and 16; one parallel producer per logical processor.
- Each sample warms the pool with 4096 tasks. The measured interval includes
  submitting and draining all benchmark tasks, not just submission time.
- Three workloads: no-op, 16 SHA-256 hashes of a 256-byte input, and a requested
  100-microsecond sleep. Actual sleep duration depends on Windows timer scheduling.
- Ten independent process samples per version/configuration, one-second target
  duration. Baseline and candidate run sequentially in alternating AB/BA order.
  No other builds or pressure tests were run concurrently.
- Analysis: `golang.org/x/perf/cmd/benchstat` at
  `v0.0.0-20261117185222-4febdb082b2c`. This is a temporary validation tool,
  not a project dependency.

## Results

The numbers below are median elapsed time divided by completed tasks. They are
aggregate throughput costs, not per-task latency percentiles. Throughput change
is `baseline_ns_per_op / candidate_ns_per_op - 1`.

| Workload | GOMAXPROCS | Baseline ns/op | Candidate ns/op | Time change | Throughput change | benchstat p-value |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| No-op | 1 | 38.61 | 39.01 | +1.02% | about -1.0% | 0.050 |
| No-op | 8 | 94.34 | 97.36 | +3.20% | about -3.1% | 0.005 |
| No-op | 16 | 85.35 | 90.76 | +6.33% | about -6.0% | <0.001 |
| CPU | 1 | 2687 | 2705 | no significant difference | about -0.7% | 0.210 |
| CPU | 8 | 377.8 | 378.1 | no significant difference | about -0.1% | 0.926 |
| CPU | 16 | 216.8 | 220.2 | +1.57% | about -1.5% | 0.012 |
| Sleep 100us | 1 | 4437 | 4429 | no significant difference | about +0.2% | 0.493 |
| Sleep 100us | 8 | 4228 | 4236 | no significant difference | about -0.2% | 0.254 |
| Sleep 100us | 16 | 4245 | 4229 | -0.38% | about +0.4% | 0.007 |

Lifetime accounting has a measurable cost even when the exporter is unused:
the candidate adds an atomic increment on acceptance and on completion. This
run shows the largest relative cost for no-op tasks at 16 logical processors.
The CPU workload has a smaller cost, and the sleep workload changes by less
than 0.5%. The small sleep result is not evidence of a general I/O speedup.
Reported B/op and allocs/op do not increase in these configurations.

These results do not establish a universal performance bound. They cover one
machine, BLOCK-mode acceptance, and the listed workloads. NONBLOCK rejection
throughput, other worker/queue sizes, other operating systems, and concurrent
scraping are outside this A/B. Exporter collection cost is measured separately
by `BenchmarkWriteText`. Statistical significance is not the same as practical
impact, and a non-significant result is not proof of zero overhead.

Raw evidence: [baseline.txt](baseline.txt), [candidate.txt](candidate.txt), and
[benchstat.txt](benchstat.txt). Only trailing whitespace in tool output is
normalized; measurements are unchanged. Benchstat prints `p=0.000` for the no-op/16 result;
the table renders that as `<0.001`, not a probability of exactly zero.

## Reproduction

Create a detached baseline worktree at the commit above and copy the benchmark
file from commit `005899b` into its `benchmark/` directory. Build each benchmark
package with `go test -c -o <output.exe> ./benchmark` using the same Go toolchain.
For each binary, collect ten samples with these arguments:

```text
-test.run=^$ -test.bench=^BenchmarkMetricsOverhead$ -test.benchmem
-test.benchtime=1s -test.cpu=1,8,16 -test.count=1 -test.timeout=3m
```

Run baseline then candidate on odd rounds, candidate then baseline on even
rounds, and append each version's stdout to its own UTF-8 text file. Keep other
CPU-heavy work stopped. Compare with:

```text
benchstat baseline.txt candidate.txt
```

See the official [benchstat documentation](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
for interpreting its median, confidence interval, and significance output.
