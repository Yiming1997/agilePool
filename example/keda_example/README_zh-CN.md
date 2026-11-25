# 用 KEDA 做自动扩缩容

这个示例让 Deployment 根据 agilePool 自己的并发需求扩缩容。

KEDA 内置的 `metrics-api` scaler 会读取 Service 每个就绪 endpoint 的
`/metrics` 并把数值求和。整个过程**不需要 Prometheus，也不需要 metrics
adapter**：scaler 通过 EndpointSlice 找到 Service，逐个 Pod 直接拉取并解析
Prometheus 文本格式。

副本数跟着池自己的决策走，而不是跟着 CPU 这个代理指标走：

```
replicas = ceil( 所有 Pod 的 agilepool_workers_demand 之和 / budget )
```

`budget` 是单个 Pod 在扩容前应当承担的并发数。

## 目标场景：短时 IO 密集任务

示例任务默认 `sleep(TASK_MS)`，也就是 5ms。等待期间 Pod 几乎不消耗 CPU。
在单节点 kind 集群上，用 4000 请求/s 的负载打两个 Pod 实测：

| 扩缩容方案 | 副本数 | Pod CPU | 队列峰值 |
| --- | --- | --- | --- |
| 不扩缩 | 2 | 131m / 2000m | 17309 |
| CPU 利用率 50% | 2 | 131m / 2000m | 17309 |
| 本示例 | 2 → 6 | — | 9319，之后消化到 12 |

CPU 型 HPA 永远停在两个副本：Pod 在等 IO 而不是烧 CPU，利用率永远够不到
阈值，队列就这么一路堆到一万多。而池知道自己想要多少并发，这个数字跟任务
是不是 CPU 密集无关。

### 更难的情况：下游变慢

到达率不变、只是服务时间变长的时候，同样成立。往同一个负载里注入一个
500ms 的下游调用：

| | 按 tick 测服务时间 | 按任务测服务时间 |
| --- | --- | --- |
| 风暴期间的 `workers_demand` | 0 | 237 → 442 |
| 副本数 | 2 → 2，永不扩 | 2 → 4 → 6 |
| 队列峰值 | 3554 且还在涨 | 426，之后降到 0 |
| Pod CPU | 80m → 60m，**反而降了** | 66m → 137m |

CPU 那一行最值得看。下游变慢时 Pod 都在等，CPU 利用率**下降**，CPU 型 HPA
什么都看不见。它不但不会扩容，配上缩容策略还可能在队列堆积时**反向缩容**。

这也说明为什么服务时间必须按任务测。旧的按 tick 测量只有在任务于一个 scaler
tick 内完成时才采得到样本，于是比 `10 × scalerPeriod`（默认 100ms）更慢的
任务**永远测不到**。这时 demand 读出来是 0，外部扩缩容器什么都看不到，池也
永远不会报告"我想要更多 worker"。

**这是唯一一个"CPU 代理信号失效、而池自身信号有效"的负载形态。** 如果你的
任务是 CPU 密集的，用 CPU 型 HPA 更简单。

## 前置条件

- Kubernetes 1.23 或更新
- KEDA 2.19.0 或更新。`aggregationType` 和
  `aggregateFromKubeServiceEndpoints` 这两个字段在 2.19 之前不存在
- `agilepool_workers_demand` 指标，需要本分支之后的 agilePool 版本。如果
  用的是已发布的旧版本，改用 `scaledobject-workers-target.yaml`

## 快速开始

在仓库根目录构建镜像，这样模块的 replace 指令才能解析到工作区的库：

```bash
docker build -f example/keda_example/Dockerfile -t agilepool-keda-example:1 .
```

在 kind 上把镜像装进节点，避免集群去拉取：

```bash
kind load docker-image --name <cluster> agilepool-keda-example:1
```

部署负载和扩缩容配置：

```bash
kubectl apply -f example/keda_example/deployment.yaml
kubectl apply -f example/keda_example/scaledobject-workers-demand.yaml
```

观察副本数变化：

```bash
kubectl -n default get scaledobject keda-example -w
kubectl -n default get pods -l app=keda-example
```

打点负载，看数字变化：

```bash
kubectl -n default run load --rm -it --image=curlimages/curl --restart=Never -- \
  sh -c 'while true; do curl -s -o /dev/null http://keda-example.default.svc.cluster.local:8080/work; done'
```

停掉负载后，经过 cooldown 和缩容稳定窗口，副本数会缩回去。

## 用哪个指标

| 指标 | 单位 | 什么时候用 |
| --- | --- | --- |
| `agilepool_workers_demand` | 并发度 | **推荐**。不受 worker 上限截断，饱和的 Pod 依然能报告"我还想再要多少" |
| `agilepool_workers_target` | 并发度 | 任何 agilePool v2 版本都能用。受上限截断，扩容步长被量化成 `capacity / budget` |
| `agilepool_tasks_queued` | 任务数 | 粗粒度兜底。单位是任务数，同样的 budget 在 1ms 和 100ms 的任务下含义完全不同 |

两个并发度指标的区别只在 Pod 饱和时才显现。`workers_target` 被截断到
`POOL_WORKERS`，满载的 Pod 只能报出上限，总和就只能以 `capacity / budget`
为步长往上跳。`workers_demand` 在截断之前发布，报的是真实需求。

## 已验证的行为

在单节点 kind 集群 + KEDA 2.21 上，`workers_demand` 触发器的表现与上面的
算式一致：

- HPA 的 condition 是 `ScalingActive: True — the HPA was able to successfully
  calculate a replica count from external metric
  s0-metric-api-agilepool_workers_demand`，说明 scaler 成功解析了这个指标并
  对各就绪 Pod 求和。
- 没有被上限截断的 Pod 上，`workers_demand` 与 `workers_target` 完全相等，
  `workers_saturated` 为 0。被截断的 Pod 上，`workers_demand` 更大，
  `workers_saturated` 变成 1。总需求 29 而总 target 25，意味着有一个 Pod
  想要 12 个 worker，但它只被允许 8 个。
- 总需求 29、budget 为 4 时，需要 `ceil(29/4) = 8` 个 Pod。如果
  `maxReplicaCount` 是 6，HPA 会报 `ScalingLimited: True — the desired
  replica count is more than the maximum replica count`。
- 负载停止后总需求回落到 0，副本数缩回去。

## budget 怎么定

budget 就是 ScaledObject 里的 `targetValue`，含义是"一个 Pod 扛多少并发
之后再加一个 Pod"。

**它必须小于 `POOL_WORKERS`。** 两个并发度指标都被 worker 上限截断。如果
budget 等于上限，饱和的 Pod 报出来的正好就是 budget，总和永远不可能超过
`Pod 数 × budget`，于是**永远不会扩容**。这不是理论上的顾虑：把
`targetValue` 设成 8、而池的 `POOL_WORKERS` 也是 8，实测就是不扩容。

取容量的一半左右，能给突发留出余量：

| 池配置 | budget |
| --- | --- | 
| `POOL_WORKERS=8`，通用 | `4` |
| `POOL_WORKERS=64`，通用 | `32` |
| CPU 密集任务，worker 跑满一个核 | `capacity * 0.8` |
| IO 密集任务，worker 大部分时间在等 | `capacity * 0.3` ~ `capacity * 0.5` |

## Pod 侧要满足三件事

1. **提供 `/metrics`**。挂上 `metrics.Handler(collector)` 就行。
2. **readinessProbe 反映真实就绪状态**。KEDA 只读 EndpointSlice 里就绪的
   endpoint，所以没就绪的 Pod 不会计入总和。这相当于白送了一层冷启动保护。
3. **收到 SIGTERM 后优雅退出**。Kubernetes 会先发 SIGTERM，再把 Pod 从
   endpoint 里摘掉。正确的顺序是：停止接收新请求 → 让在途任务跑完 → 退出。
   `pool.Close()` 会拒绝新的提交，`pool.Wait()` 会等到所有已接受的任务完成。

## 三个坑

**不要写 `metricType: Value`。** `AverageValue` 是 KEDA 的默认值，对应 HPA 的
"每 Pod" 算式：`ceil(usage / target)`，**不乘副本数**。`Value` 走的是另一条
`ceil(usage / target * replicas)`，会严重过度扩容。

**不要用 `aggregationType: average`。** 求平均会把总和除以 endpoint 数量，
N 个各报 8 的 Pod 平均后还是 8，而不是 8N，副本数于是永远长不上去。逐 Pod
的数值要用 `sum`。

**KEDA 的 ScaledJob CRD 不能用普通 `kubectl apply` 安装。** CRD 超过了
last-applied 注解 256KB 的上限，apply 会失败。要用
`kubectl apply --server-side`。CRD 缺失时 KEDA operator 会卡在等待缓存同步
而 crash-loop，metrics API server 连不上它，HPA 拿不到任何指标——**不扩容，
也不报错**。

## 适用范围

这个方案适合"瓶颈在单进程内部的 goroutine 池"的服务。代价是应用要自己把池
指标暴露出来：CPU 型和队列深度型扩缩容不需要改任何代码。

它还假设各 Pod 处理的工作是可比的。demand 是每个 Pod 单独测量再求和的，
处理慢任务的 Pod 会比处理快任务的 Pod 报出更高的 demand。

## 相关链接

- 主 README 里的[指标说明](../../README_zh-CN.md#prometheus-指标)
- 池 API 的 `GetScalerDemand()` 与 `GetScalerTarget()`
