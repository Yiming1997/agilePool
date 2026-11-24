package agilepool

import (
	"context"
)

// TaskHook is the callback signature for task lifecycle events. The task
// itself is not passed; use SubmitCtx to carry per-task data through ctx.
type TaskHook func(ctx context.Context)

// TaskCompleteHook is the callback signature for task completion.
// recovered is nil on normal exit, otherwise the value passed to panic.
type TaskCompleteHook func(ctx context.Context, recovered any)

// PoolHook is the callback signature for pool-level events.
type PoolHook func(p *Pool)

// Hooks is the lifecycle callback dispatcher accepted by Pool.SetHook. The
// bundled implementation lives in
// github.com/Yiming1997/agilePool/v2/hook; custom implementations only need
// the five dispatch methods below.
type Hooks interface {
	// DispatchTaskSubmitted must only be called by the pool submission path.
	DispatchTaskSubmitted(context.Context)
	// DispatchTaskEnqueued must only be called by the pool enqueue path. It
	// fires exactly once per accepted task, once the task has been handed to
	// the handoff channel or to the overflow buffer. Buffer-accepted tasks
	// are dispatched after the buffer lock is released, so the callback may
	// observe Enqueued after Started (or even after Completed) for the same
	// task; ordering between these events is not guaranteed.
	DispatchTaskEnqueued(context.Context)

	// DispatchTaskStarted must only be called by the worker execution path.
	DispatchTaskStarted(context.Context)

	// DispatchTaskCompleted must only be called by the worker completion path.
	DispatchTaskCompleted(context.Context, any)
	// DispatchPoolClosed must only be called by Pool.Close.
	DispatchPoolClosed(*Pool)
}

// RejectedHooks is an optional extension to Hooks for non-nil submissions
// refused before acceptance: a closed pool, NONBLOCK saturation, or cancellation
// while submitting (including an already-cancelled SubmitCtx). Cancellation
// after acceptance completes the task lifecycle and does not fire this event.
//
// Callbacks run synchronously in the submitter after rejection bookkeeping is
// balanced and before the submission method returns. The context comes from
// SubmitCtx/SubmitBefore or a task wrapped with UpdateTask. Nil tasks are ignored.
// Wait does not wait for rejection callbacks; callers must also wait for their
// submitting goroutines before taking a final rejection-count snapshot.
// Existing Hooks implementations remain valid without this extension.
type RejectedHooks interface {
	DispatchTaskRejected(context.Context)
}
