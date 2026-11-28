# Autoscaling with KEDA

This example scales a Deployment of agilePool pods from the pool's own
concurrency demand.

KEDA's built-in `metrics-api` scaler reads `/metrics` from every ready
endpoint of a Service and sums the values. No Prometheus server and no
metrics adapter are involved: the scaler resolves the Service through its
EndpointSlice, fetches each pod endpoint directly and parses the Prometheus
text format.

The replica count follows the pool's own decision instead of a CPU proxy:

```
replicas = ceil( sum over pods of agilepool_workers_demand / budget )
```

`budget` is the concurrency one pod should carry before another pod is
added.

## The case this is built for: short IO-bound tasks

The demo task sleeps for `TASK_MS` (5 ms by default). While it waits, the
pod burns almost no CPU. Measured on a single-node kind cluster with an
offered load of 4000 requests/s against two pods:

| autoscaler | replicas | pod CPU | peak queue |
| --- | --- | --- | --- |
| none | 2 | 131m of 2000m | 17309 |
| CPU utilization 50% | 2 | 131m of 2000m | 17309 |
| this example | 2 → 6 | — | 9319, drained to 12 |

A CPU-based HPA never leaves two replicas: the pods are waiting on IO, not
burning CPU, so the utilization target is never reached and the backlog
grows without bound. The pool knows how many concurrent workers it wants,
and that number does not depend on the task being CPU-bound.

### The harder case: the downstream gets slow

The same holds when the arrival rate does not change but the service time
does. Injecting a 500 ms downstream call into the same workload:

| | per-tick service time | per-task service time |
| --- | --- | --- |
| `workers_demand` during the storm | 0 | 237 → 442 |
| replicas | 2 → 2, never scales | 2 → 4 → 6 |
| peak queue | 3554 and still growing | 426, drained to 0 |
| pod CPU | 80m → 60m, it *drops* | 66m → 137m |

The CPU row is the interesting one. While the downstream is slow the pods
are waiting, so the CPU utilisation falls and a CPU-based HPA sees nothing
at all. It does not scale out, and with a scale-down policy it can even
scale *in* while the backlog grows.

This is also why the per-task service time measurement matters. The older
per-tick measurement only samples the service time when a task completes
inside one scaler tick, so a task slower than `10 × scalerPeriod` (100 ms
by default) is never measured at all. The demand then reads 0, the
external autoscaler sees nothing, and the pool never reports that it wants
more workers.

This is the one workload shape where the CPU proxy fails and the pool's own
signal works. For CPU-bound tasks a CPU-based HPA is the simpler choice.

## Requirements

- Kubernetes 1.23 or newer
- KEDA 2.19.0 or newer. `aggregationType` and
  `aggregateFromKubeServiceEndpoints` do not exist before 2.19.
- `agilepool_workers_demand`, which requires an agilePool release from this
  branch or later. With an older release, use
  `scaledobject-workers-target.yaml` instead.

## Quick start

Build the image from the repository root so that the module replace
directive resolves the library from the working tree:

```bash
docker build -f example/keda_example/Dockerfile -t agilepool-keda-example:1 .
```

On kind, load it into the node so the cluster does not pull it:

```bash
kind load docker-image --name <cluster> agilepool-keda-example:1
```

Deploy the workload and the autoscaler:

```bash
kubectl apply -f example/keda_example/deployment.yaml
kubectl apply -f example/keda_example/scaledobject-workers-demand.yaml
```

Watch the replica count react:

```bash
kubectl -n default get scaledobject keda-example -w
kubectl -n default get pods -l app=keda-example
```

Drive some load and watch the numbers move:

```bash
kubectl -n default run load --rm -it --image=curlimages/curl --restart=Never -- \
  sh -c 'while true; do curl -s -o /dev/null http://keda-example.default.svc.cluster.local:8080/work; done'
```

Stop the load and the deployment scales back down after the cooldown and the
scale-down stabilisation window.

## Which metric to scale on

| metric | unit | use it when |
| --- | --- | --- |
| `agilepool_workers_demand` | concurrency | recommended. Unclamped by the worker capacity, so a saturated pod still reports how far it wants to grow |
| `agilepool_workers_target` | concurrency | works with any agilePool v2 release. Clamped to the capacity, so the scale-out step is quantised to `capacity / budget` |
| `agilepool_tasks_queued` | tasks | coarse fallback. The unit is a task count, so the same budget means something different for 1 ms and for 100 ms tasks |

**Enable `WithLittleLawScaling()` in the pod whenever one of these metrics drives
the replica count.** The default scaler sizes the pool from the backlog, and the
backlog is a function of how many pods are running: a single saturated pod
reports a demand proportional to its own queue, so the autoscaler provisions far
more pods than the arrival rate needs. Measured with a 100-worker pod and a
100-worker budget, a 6000 tasks/s load of 50 ms tasks (Little's law: 3 pods) made
one pod report 11.5 pods' worth of demand with 50 producer goroutines and 16 with
500. With `WithLittleLawScaling()` the demand is `arrivalRate × serviceTime` plus
a saturating bump, and the same cluster simulation climbs 1 → 3 pods.

The difference between the two concurrency metrics is only visible when a
pod is saturated. `workers_target` is clamped to `POOL_WORKERS`, so a full
pod reports the ceiling and the summed value can only grow in steps of
`capacity / budget`. `workers_demand` is published before that clamp and
keeps reporting the real requirement.

## Verified behaviour

On a single-node kind cluster with KEDA 2.21, the `workers_demand` trigger
behaves as the formula above predicts:

- The HPA condition reads `ScalingActive: True — the HPA was able to
  successfully calculate a replica count from external metric
  s0-metric-api-agilepool_workers_demand`, so the scaler resolves the gauge
  and sums it over the ready pods.
- `workers_demand` equals `workers_target` on every pod that is not clamped
  by its capacity, and `workers_saturated` stays at 0. On a pod that is
  clamped, `workers_demand` is larger and `workers_saturated` turns to 1. A
  summed demand of 29 against a summed target of 25 means one pod wanted
  twelve workers while it was only allowed eight.
- A summed demand of 29 with a budget of 4 asks for `ceil(29/4) = 8` pods.
  With `maxReplicaCount: 6` the HPA reports `ScalingLimited: True — the
  desired replica count is more than the maximum replica count`.
- The summed demand returns to zero once the load stops, and the deployment
  scales back in.

## Choosing the budget

The budget is `targetValue` in the ScaledObject. It is the number of
concurrent workers one pod should carry before another pod is added.

**It must be lower than `POOL_WORKERS`.** Both concurrency metrics are
clamped to the worker capacity. If the budget equals the capacity, a
saturated pod reports exactly the budget, the summed value can never exceed
`pods * budget`, and the deployment never scales out. This is not a
theoretical concern: it is the behaviour of the `targetValue: "8"` variant
against a pool with `POOL_WORKERS=8`.

A budget around half the capacity leaves headroom for bursts:

| pool configuration | budget |
| --- | --- |
| `POOL_WORKERS=8`, general use | `4` |
| `POOL_WORKERS=64`, general use | `32` |
| CPU-bound tasks, worker saturates a core | `capacity * 0.8` |
| IO-bound tasks, workers idle most of the time | `capacity * 0.3` to `capacity * 0.5` |

## What the pods must do

1. **Serve `/metrics`.** `metrics.Handler(collector)` is enough.
2. **Have a readiness probe that reflects real readiness.** KEDA reads the
   ready endpoints of the EndpointSlice, so a pod that is not ready yet
   contributes nothing to the sum. This gives a cold start protection for
   free.
3. **Shut down gracefully on SIGTERM.** Kubernetes sends SIGTERM before it
   removes the pod from the endpoints. Stop accepting new work, let the
   in-flight tasks finish, then exit. `pool.Close()` rejects further
   submissions and `pool.Wait()` returns once every accepted task has
   completed.

## Traps

**Do not set `metricType: Value`.** `AverageValue` is the KEDA default and
selects the per-pod HPA formula, which computes `ceil(usage / target)`
without multiplying by the replica count. `Value` selects the other formula,
`ceil(usage / target * replicas)`, which scales out far too aggressively.

**Do not use `aggregationType: average`.** Averaging divides the summed
value by the number of endpoints, so N pods each reporting 8 become 8 in
total instead of 8N. The replica count then never grows. `sum` is the
correct aggregation for a per-pod value.

**Do not install the KEDA ScaledJob CRD with a plain `kubectl apply`.** The
CRD is larger than the 256 KB limit for the last-applied annotation, so the
apply fails. Use `kubectl apply --server-side`. Without the CRD the KEDA
operator crash-loops while waiting for its caches, the metrics API server
cannot reach it, and the HPA silently receives no metric at all: nothing
scales and nothing reports an error.

## Scope

This scheme is for services whose bottleneck is a goroutine pool inside one
process. It needs the application to expose the pool metrics, which is the
price of the signal: a CPU-based or queue-depth-based autoscaler needs no
code change.

It also assumes the pods run comparable work. The demand is measured per
pod and summed, so a pod that serves slower tasks reports a higher demand
than one that serves fast tasks.

## See also

- The metric reference in the [main README](../../readme.md#prometheus-metrics)
- `GetScalerDemand()` and `GetScalerTarget()` in the pool API
