package workflow

import (
	"fmt"
	"testing"

	"github.com/mafilus/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

func TestWorkflowMetadataValueImplementsStringer(t *testing.T) {
	metadata := WorkflowMetadata{RuntimeStatus: api.RUNTIME_STATUS_RUNNING}
	var stringer fmt.Stringer = metadata
	require.Equal(t, "RUNNING", stringer.String())
	require.False(t, WorkflowMetadataIsComplete(&metadata))
}
