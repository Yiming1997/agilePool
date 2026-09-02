package agilepool

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Well-known label keys
// ---------------------------------------------------------------------------

const (
	// LabelTraceID is the standard key for trace identifiers.
	LabelTraceID = "trace_id"

	// Internal keys used by TimingHook — prefixed with "_" to avoid
	// collisions with user labels.
	labelTiming          = "_timing"           // Empty{} if enabled
	labelTimingSubmitted = "_timing_submitted" // time.Time
	labelTimingEnqueued  = "_timing_enqueued"  // time.Time
	labelTimingStarted   = "_timing_started"   // time.Time
	labelTimingCompleted = "_timing_completed" // time.Time
)

// ---------------------------------------------------------------------------
// Empty — sentinel value
// ---------------------------------------------------------------------------

// Empty is a zero-allocation sentinel type.  Store an Empty{} under a key
// to signal "enabled" without allocating a meaningful value.
//
//	ctx.EnableTiming()   // stores Empty{} under "_timing"
//	_, ok := ctx.Get("_timing")  // ok==true means timing is enabled
type Empty struct{}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

// Context wraps a standard context.Context with a thread-safe mutable
// key-value store (labels).  It implements context.Context via embedding,
// and overrides Value() to search its own labels before delegating to the
// parent chain — the same design pattern used by Gin's gin.Context.
//
// Context is designed for sync.Pool reuse: obtain one via NewContext and
// call Release() after the task completes to return it to the pool.
type Context struct {
	context.Context

	mu     sync.Mutex
	labels map[string]any
}

// ---------------------------------------------------------------------------
// sync.Pool for Context reuse
// ---------------------------------------------------------------------------

var contextPool = sync.Pool{
	New: func() any {
		return &Context{labels: make(map[string]any, 8)}
	},
}

// ---------------------------------------------------------------------------
// contextKey — unexported type to avoid collisions in context.WithValue.
// ---------------------------------------------------------------------------

type contextKey struct{ name string }

var poolContextKey = contextKey{name: "agilepool.Context"}

// ---------------------------------------------------------------------------
// Constructor & lifecycle
// ---------------------------------------------------------------------------

// NewContext obtains a Context from the internal sync.Pool and initialises
// it with parent.  If parent is nil, context.Background() is used.
//
// IMPORTANT: NewContext injects *Context itself into the parent's Value
// chain via context.WithValue.  This means PoolContextFrom can find it even
// if the caller later wraps the Context with context.WithValue,
// context.WithTimeout, or context.WithCancel — matching Gin's behaviour.
func NewContext(parent context.Context) *Context {
	if parent == nil {
		parent = context.Background()
	}
	c := contextPool.Get().(*Context)
	// Inject self so PoolContextFrom survives wrapping.
	c.Context = context.WithValue(parent, poolContextKey, c)
	return c
}

// Release resets the Context and returns it to the internal pool for reuse.
// Call this once per task to avoid allocation churn.
func (c *Context) Release() {
	c.Reset()
	c.Context = nil // drop reference to parent chain
	contextPool.Put(c)
}

// Reset clears all labels and metrics state.  It is called automatically
// by Release(); you only need to call it directly if you are reusing a
// Context outside the pool.
func (c *Context) Reset() {
	c.mu.Lock()
	// Zero each entry so GC can reclaim values, then clear the map.
	for k := range c.labels {
		delete(c.labels, k)
	}
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Store / Get — generic key-value access (labels is map[string]any)
// ---------------------------------------------------------------------------

// Store sets a value under key.  Thread-safe.
func (c *Context) Store(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.labels[key] = value
}

// Get retrieves a value by key.  The bool reports whether the key was present.
// Thread-safe.
func (c *Context) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.labels[key]
	return v, ok
}

// GetString is a convenience helper that returns the string value for key,
// or "" if the key is missing or its value is not a string.
func (c *Context) GetString(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.labels[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// GetTime is a convenience helper that returns the time.Time value for key,
// or the zero time if the key is missing or its value is not a time.Time.
func (c *Context) GetTime(key string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.labels[key]; ok {
		if t, ok := v.(time.Time); ok {
			return t
		}
	}
	return time.Time{}
}

// ---------------------------------------------------------------------------
// Value — override context.Context.Value for chain lookup (Gin-style)
// ---------------------------------------------------------------------------

// Value searches the Context's labels first (for string keys), then falls
// back to the embedded context.Context chain.  This means:
//
//   - ctx.Value("trace_id")       → found in labels
//   - ctx.Value(poolContextKey)   → found in parent via WithValue injection
//   - ctx.Value(someOtherKey)     → delegated to embedded context chain
//
// This design ensures that even after wrapping with context.WithValue /
// context.WithTimeout, standard context.Value lookups can still reach the
// Context's labels.
func (c *Context) Value(key any) any {
	// String keys: search our labels map first.
	if k, ok := key.(string); ok {
		c.mu.Lock()
		if v, exists := c.labels[k]; exists {
			c.mu.Unlock()
			return v
		}
		c.mu.Unlock()
	}
	// Fall back to the embedded context chain (which includes the
	// poolContextKey injection set up in NewContext).
	return c.Context.Value(key)
}

// ---------------------------------------------------------------------------
// PoolContextFrom — extract *Context from any context.Context
// ---------------------------------------------------------------------------

// PoolContextFrom extracts a *Context from ctx.
// Returns nil if ctx is not a *Context and does not contain one in its
// Value chain.
//
// It first tries a type assertion (fast path), then falls back to
// ctx.Value(poolContextKey) (survives context.WithValue / WithTimeout
// wrapping thanks to the injection in NewContext).
func PoolContextFrom(ctx context.Context) *Context {
	if c, ok := ctx.(*Context); ok {
		return c
	}
	if c, ok := ctx.Value(poolContextKey).(*Context); ok {
		return c
	}
	return nil
}

// ---------------------------------------------------------------------------
// TraceID — auto-generating trace identifier
// ---------------------------------------------------------------------------

// TraceID returns the trace_id label.  If no trace_id has been set,
// a random hex-encoded 16-byte id is generated and stored automatically.
func (c *Context) TraceID() (string, error) {
	// Fast path: already set.
	c.mu.Lock()
	if id, ok := c.labels[LabelTraceID].(string); ok && id != "" {
		c.mu.Unlock()
		return id, nil
	}
	c.mu.Unlock()

	// Slow path: generate outside the lock (crypto/rand may block).
	id, err := newTraceID()
	if err != nil {
		return "", err
	}

	// Double-check: another goroutine may have set it while we were generating.
	c.mu.Lock()
	if existing, ok := c.labels[LabelTraceID].(string); ok && existing != "" {
		c.mu.Unlock()
		return existing, nil
	}
	c.labels[LabelTraceID] = id
	c.mu.Unlock()
	return id, nil
}

// SetTraceID explicitly sets the trace_id label.  Use this when you have an
// incoming trace id from an upstream system (e.g. X-Trace-Id header).
func (c *Context) SetTraceID(id string) {
	c.Store(LabelTraceID, id)
}

// ---------------------------------------------------------------------------
// Timing support
// ---------------------------------------------------------------------------

// EnableTiming marks this Context for lifecycle timing.  After calling this,
// any registered TimingHook will record timestamps at each lifecycle stage
// (submitted / enqueued / started / completed).
//
// Check whether timing is enabled:
//
//	_, enabled := ctx.Get(labelTiming)
func (c *Context) EnableTiming() {
	c.Store(labelTiming, Empty{})
}

// IsTimingEnabled reports whether EnableTiming() was called on this Context.
func (c *Context) IsTimingEnabled() bool {
	_, ok := c.Get(labelTiming)
	return ok
}

// Timing returns the four lifecycle timestamps recorded by TimingHook.
// A zero time.Time means that stage was not recorded (e.g. the hook was
// not registered, or timing was not enabled).
func (c *Context) Timing() (submitted, enqueued, started, completed time.Time) {
	return c.GetTime(labelTimingSubmitted),
		c.GetTime(labelTimingEnqueued),
		c.GetTime(labelTimingStarted),
		c.GetTime(labelTimingCompleted)
}

// Duration returns the elapsed time since the task started, or 0 if no
// start timestamp has been recorded.  TimingHook must be registered and
// EnableTiming() must have been called.
func (c *Context) Duration() time.Duration {
	start := c.GetTime(labelTimingStarted)
	if start.IsZero() {
		return 0
	}
	return time.Since(start)
}

// TotalLatency returns the full end-to-end latency (submitted → completed),
// or 0 if either timestamp is missing.
func (c *Context) TotalLatency() time.Duration {
	submitted, _, _, completed := c.Timing()
	if submitted.IsZero() || completed.IsZero() {
		return 0
	}
	return completed.Sub(submitted)
}

// QueueLatency returns the time spent waiting in the queue (submitted → started),
// or 0 if either timestamp is missing.
func (c *Context) QueueLatency() time.Duration {
	submitted, _, started, _ := c.Timing()
	if submitted.IsZero() || started.IsZero() {
		return 0
	}
	return started.Sub(submitted)
}

// ExecLatency returns the time spent executing (started → completed),
// or 0 if either timestamp is missing.
func (c *Context) ExecLatency() time.Duration {
	_, _, started, completed := c.Timing()
	if started.IsZero() || completed.IsZero() {
		return 0
	}
	return completed.Sub(started)
}

// HandoffLatency returns the time between submission and enqueueing
// (submitted → enqueued), or 0 if either timestamp is missing.
// This measures how long the pool took to accept the task into its
// internal channel or buffer.
func (c *Context) HandoffLatency() time.Duration {
	submitted, enqueued, _, _ := c.Timing()
	if submitted.IsZero() || enqueued.IsZero() {
		return 0
	}
	return enqueued.Sub(submitted)
}

// QueueWaitLatency returns the time spent waiting in the queue after
// enqueueing (enqueued → started), or 0 if either timestamp is missing.
// This is the pure queueing delay, excluding the initial handoff.
func (c *Context) QueueWaitLatency() time.Duration {
	_, enqueued, started, _ := c.Timing()
	if enqueued.IsZero() || started.IsZero() {
		return 0
	}
	return started.Sub(enqueued)
}

// ---------------------------------------------------------------------------
// TimingHook — pre-built four-phase lifecycle timing hooks
// ---------------------------------------------------------------------------

// TimingHook returns a set of four hooks that record timestamps into the
// Context at each lifecycle stage.  The Context must have EnableTiming()
// called on it before the hooks will record anything.
//
// Usage:
//
//	s, e, st, co := agilepool.TimingHook()
//	pool.OnTaskSubmitted(s)
//	pool.OnTaskEnqueued(e)
//	pool.OnTaskStarted(st)
//	pool.OnTaskCompleted(co)
//
// After completion, query timestamps via:
//
//	pc.Timing()          // all four time.Time values
//	pc.Duration()        // time since started
//	pc.TotalLatency()    // submitted → completed
//	pc.QueueLatency()    // submitted → started
//	pc.ExecLatency()     // started → completed
func TimingHook() (
	submitted TaskHook,
	enqueued TaskHook,
	started TaskHook,
	completed TaskCompleteHook,
) {
	submitted = func(ctx context.Context, task Task) {
		pc := PoolContextFrom(ctx)
		if pc == nil || !pc.IsTimingEnabled() {
			return
		}
		pc.Store(labelTimingSubmitted, time.Now())
	}

	enqueued = func(ctx context.Context, task Task) {
		pc := PoolContextFrom(ctx)
		if pc == nil || !pc.IsTimingEnabled() {
			return
		}
		pc.Store(labelTimingEnqueued, time.Now())
	}

	started = func(ctx context.Context, task Task) {
		pc := PoolContextFrom(ctx)
		if pc == nil || !pc.IsTimingEnabled() {
			return
		}
		pc.Store(labelTimingStarted, time.Now())
	}

	completed = func(ctx context.Context, task Task, recovered any) {
		pc := PoolContextFrom(ctx)
		if pc == nil || !pc.IsTimingEnabled() {
			return
		}
		pc.Store(labelTimingCompleted, time.Now())
	}

	return
}

// ---------------------------------------------------------------------------
// SlowTaskLogHook — detect and log slow tasks
// ---------------------------------------------------------------------------

// SlowTaskLogHook returns a TaskCompleteHook that logs a warning when a task's
// execution time exceeds maxDuration.  TimingHook must also be registered and
// EnableTiming() must have been called on the Context, otherwise the hook is
// a no-op.
//
// The log message includes the TraceID, execution latency, threshold, and
// the caller location (file:line function) captured via runtime.Caller so
// operators can trace back to the hook dispatch site.
//
// Usage:
//
//	// Register TimingHook first to capture lifecycle timestamps.
//	s, e, st, co := agilepool.TimingHook()
//	pool.OnTaskSubmitted(s)
//	pool.OnTaskEnqueued(e)
//	pool.OnTaskStarted(st)
//	pool.OnTaskCompleted(co)
//
//	// Then register SlowTaskLogHook as an additional completion hook.
//	pool.OnTaskCompleted(agilepool.SlowTaskLogHook(5*time.Second, logger))
//
//	// In the task, enable timing on the Context:
//	pc := agilepool.PoolContextFrom(ctx)
//	pc.EnableTiming()
func SlowTaskLogHook(maxDuration time.Duration, logger Logger) TaskCompleteHook {
	return func(ctx context.Context, task Task, recovered any) {
		pc := PoolContextFrom(ctx)
		if pc == nil || !pc.IsTimingEnabled() {
			return
		}
		d := pc.ExecLatency()
		if d <= 0 || d <= maxDuration {
			return
		}
		traceID, _ := pc.TraceID()

		// Capture the caller location for diagnostics.
		// skip=3 skips: this closure → dispatch wrapper → dispatchTaskCompleted,
		// landing on the worker's runTask defer where the hook was triggered.
		loc := "unknown"
		if callerPC, file, line, ok := runtime.Caller(3); ok {
			loc = fmt.Sprintf("%s:%d", file, line)
			if fn := runtime.FuncForPC(callerPC); fn != nil {
				loc += " " + fn.Name()
			}
		}
		logger.Printf("[slow-task] trace_id=%s exec_latency=%v threshold=%v caller=%s",
			traceID, d, maxDuration, loc)
	}
}
