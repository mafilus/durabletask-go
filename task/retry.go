package task

import (
	"fmt"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
)

// retryTask advances only in Await, preserving the observation point at which
// backoff timers and subsequent attempts acquire their action IDs. It caches
// the terminal attempt, independently of decoding the caller's result type.
type retryTask struct {
	ctx                 *WorkflowContext
	name                string
	initialAttempt      time.Time
	schedule            func(string, bool) Task
	policy              RetryPolicy
	current             Task
	dispatchExecutionID string
	recordedExecutionID string
	setTimerOrigin      func(*protos.CreateTimerAction, string)
	retryCount          int
	finished            bool
	terminalError       error
}

func (t *retryTask) TaskExecutionId() string {
	if id := t.current.TaskExecutionId(); id != "" {
		return id
	}
	return t.recordedExecutionID
}

func (t *retryTask) Await(v any) error {
	for !t.finished {
		// Await(nil) observes execution only. Decoding must never cause an
		// already successful activity or child workflow to run again.
		err := t.current.Await(nil)
		if err == nil {
			t.finished = true
			break
		}
		// A history-processing error is not an attempt failure. Preserve it
		// rather than asking the retry policy to schedule unrelated work.
		if current, ok := t.current.(*completableTask); !ok || !current.isCompleted {
			t.finished, t.terminalError = true, err
			break
		}
		current := t.current.(*completableTask)
		// Dispatch IDs may be generated while scheduling. Only an observed
		// historical ID is safe to expose to workflow code or reuse after failure.
		// Cancellation does not provide a new identity of its own.
		if !current.isCanceled || current.TaskExecutionId() != "" {
			t.dispatchExecutionID = current.TaskExecutionId()
			t.recordedExecutionID = current.TaskExecutionId()
		}
		if t.retryCount+1 >= t.policy.MaxAttempts {
			t.finished, t.terminalError = true, err
			break
		}
		delay := computeNextDelay(t.ctx.CurrentTimeUtc, t.policy, t.retryCount, t.initialAttempt, err)
		if delay == 0 {
			t.finished, t.terminalError = true, err
			break
		}
		timer, action := t.ctx.createTimerInternal(&t.name, delay)
		t.setTimerOrigin(action, t.dispatchExecutionID)
		if timerErr := timer.Await(nil); timerErr != nil {
			t.finished, t.terminalError = true, fmt.Errorf("%v %w", timerErr, err)
			break
		}
		t.retryCount++
		t.current = t.schedule(t.dispatchExecutionID, true)
	}
	if t.terminalError != nil {
		return t.terminalError
	}
	return t.current.Await(v)
}
