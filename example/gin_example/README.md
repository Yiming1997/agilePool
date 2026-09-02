# Gin 集成示例

这个示例展示如何使用 `UpdateTask` 将 Gin 请求的 `context.Context` 传递到 agilePool 的任务中，并使用非阻塞提交实现流量控制。

## 运行

```bash
go run .
```

服务监听 `:8080`，提交请求：

```bash
curl -X POST http://localhost:8080/jobs
```

## 关键设计

- `c.Request.Context()` 是请求级 Context，客户端断开或请求超时后会自动取消。
- `agilepool.UpdateTask(ctx, task)` 将 Context 封装到 `contextTask` 中。
- `TrySubmit` 配合 `NONBLOCK` 模式，在池已达到处理能力时立即返回 `false`。
- 被拒绝的请求返回 `429 Too Many Requests`，由客户端稍后重试。
- 任务只向带缓冲的 channel 写结果，不直接使用 `gin.Context`，避免请求结束后异步访问 Gin 对象。
- 任务开始前如果 Context 已取消，`contextTask` 会跳过任务；执行中的任务需要像示例一样主动监听 Context。
- 不要把 `*gin.Context` 直接放入异步任务。示例只复制 `request.Context()`、请求 ID 和客户端 IP，避免 Gin 请求对象被复用后发生数据竞争。

## 其他用法

### 请求超时

为请求增加 deadline，任务和 handler 共享该 Context：

```go
ctx, cancel := context.WithTimeout(c.Request.Context(), 500*time.Millisecond)
defer cancel()

task := agilepool.UpdateTask(ctx, agilepool.TaskFunc(func() error {
	select {
	case <-time.After(time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}))
if !pool.TrySubmit(task) {
	c.Status(http.StatusTooManyRequests)
	return
}
```

### 携带请求元数据

将必要数据复制到标准 Context，而不是传递 Gin 对象：

```go
ctx := context.WithValue(c.Request.Context(), traceIDKey{}, c.GetHeader("X-Trace-ID"))
pool.Submit(agilepool.UpdateTask(ctx, agilepool.TaskFunc(func() error {
	traceID, _ := ctx.Value(traceIDKey{}).(string)
	log.Printf("processing trace=%s", traceID)
	return nil
})))
```

### 等待异步结果

使用带缓冲 channel 将结果安全地交回 handler。任务只写 channel，handler 负责写 HTTP 响应：

```go
resultCh := make(chan string, 1)
ctx := c.Request.Context()
if !pool.TrySubmit(agilepool.UpdateTask(ctx, agilepool.TaskFunc(func() error {
	resultCh <- "ok"
	return nil
}))) {
	c.Status(http.StatusTooManyRequests)
	return
}
select {
case value := <-resultCh:
	c.JSON(http.StatusOK, gin.H{"result": value})
case <-ctx.Done():
	c.Status(http.StatusRequestTimeout)
}

可以用以下命令观察流量控制：

```bash
for /L %i in (1,1,100) do start /B curl -X POST http://localhost:8080/jobs
```
