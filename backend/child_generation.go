package backend

import (
	"errors"
	"fmt"

	"github.com/mafilus/durabletask-go/api/protos"
)

// ErrChildExecutionProvenanceRequired requires draining or migrating legacy
// child results whose captured parent execution cannot be established.
var ErrChildExecutionProvenanceRequired = errors.New("child parent execution provenance required")

func IsChildResult(e *protos.HistoryEvent) bool {
	return e.GetChildWorkflowInstanceCompleted() != nil || e.GetChildWorkflowInstanceFailed() != nil
}

// ChildResultExecutionID uses the identity captured when the child was created,
// never the destination's current identity at completion time.
func ChildResultExecutionID(state *protos.WorkflowRuntimeState, target string) (string, error) {
	parent := state.GetStartEvent().GetParentInstance().GetWorkflowInstance()
	if parent.GetInstanceId() != target || parent.GetExecutionId().GetValue() == "" {
		return "", fmt.Errorf("%w: missing captured parent identity for %s", ErrChildExecutionProvenanceRequired, target)
	}
	return parent.GetExecutionId().GetValue(), nil
}
