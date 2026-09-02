package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/gin-gonic/gin"
)

// This example keeps request work bounded. NONBLOCK makes TrySubmit return
// immediately when both the handoff channel and overflow buffer are full.
var pool = agilepool.NewPool(agilepool.NewConfig(
	agilepool.WithBlockMode(agilepool.NONBLOCK),
	agilepool.WithTaskQueueSize(32),
	agilepool.WithWorkerNumCapacity(8),
))

// taskStartTimes associates each traceID with the time when its pool task
// started. sync.Map is used because hooks run concurrently across workers.
var taskStartTimes sync.Map

func main() {
	defer pool.Close()
	// Register lifecycle hooks: the start hook records the timestamp, and the
	// completion hook calculates the elapsed time for the same traceID.
	pool.OnTaskStarted(DebugHook)
	pool.OnTaskCompleted(DebugCompleteHook)
	r := gin.Default()
	r.Use(PoolMiddleware)
	// This example shows how to limit concurrency for incoming requests.
	r.POST("/jobs", anRouterFunc)
	_ = r.Run("127.0.0.1:8080")
}

func DebugHook(ctx context.Context, task agilepool.Task) {
	if c, ok := ctx.(*gin.Context); ok {
		// UpdateTask preserves this Gin context, so the hook can read request
		// metadata that was attached before the task was submitted.
		traceID, _ := c.Get("traceID")
		taskStartTimes.Store(traceID, time.Now())
		fmt.Println("task started", traceID, c.ClientIP())
	}
}

func DebugCompleteHook(ctx context.Context, task agilepool.Task, recovered any) {
	if c, ok := ctx.(*gin.Context); ok {
		traceID, _ := c.Get("traceID")
		// LoadAndDelete both reads the start time and releases it after the
		// task completes, preventing completed requests from accumulating.
		if started, exists := taskStartTimes.LoadAndDelete(traceID); exists {
			duration := time.Since(started.(time.Time))
			// recovered is nil for normal completion and contains the panic
			// value when the pool recovered a panic from the task.
			log.Printf("task completed traceID=%v duration=%s recovered=%v", traceID, duration, recovered)
		}
	}
}

type Content struct {
	Content string `json:"content"`
}

// PoolMiddleware runs the complete Gin handler chain in an agilePool task.
// UpdateTask keeps the original Gin context available to the task and hooks.
func PoolMiddleware(c *gin.Context) {
	// done keeps the HTTP request open until the pool worker finishes the
	// complete Gin handler chain and writes the response.
	done := make(chan struct{})
	// Set metadata before submission so the start hook can read it regardless
	// of how long the task waits in the pool queue.
	c.Set("traceID", rand.Int64())
	// UpdateTask carries the Gin context through the pool. The worker executes
	// c.Next(), so normal handlers can write responses in the usual way.
	task := agilepool.UpdateTask(c, agilepool.TaskFunc(func() error {
		defer close(done)
		c.Next()
		return nil
	}))
	if !pool.TrySubmit(task) {
		// NONBLOCK rejects the request when the pool is saturated.
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "system busy"})
		c.Abort()
		return
	}
	// For this small example, assume the request is not cancelled and wait
	// until the handler chain has completed.
	<-done
}

func anRouterFunc(c *gin.Context) {
	var content Content
	if err := c.ShouldBindBodyWithJSON(&content); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// The handler itself runs in the pool; no extra result channel is needed.
	// It can write the response normally because PoolMiddleware waits for
	// c.Next() to finish before returning to the HTTP server.
	time.Sleep(10 * time.Second)
	c.JSON(http.StatusOK, content)
}
