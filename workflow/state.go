package workflow

import (
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	StatusRunning        = api.RUNTIME_STATUS_RUNNING
	StatusCompleted      = api.RUNTIME_STATUS_COMPLETED
	StatusContinuedAsNew = api.RUNTIME_STATUS_CONTINUED_AS_NEW
	StatusFailed         = api.RUNTIME_STATUS_FAILED
	StatusCanceled       = api.RUNTIME_STATUS_CANCELED
	StatusTerminated     = api.RUNTIME_STATUS_TERMINATED
	StatusPending        = api.RUNTIME_STATUS_PENDING
	StatusSuspended      = api.RUNTIME_STATUS_SUSPENDED
	StatusStalled        = api.RUNTIME_STATUS_STALLED
)

// WorkflowMetadata is the SDK representation of workflow metadata. It mirrors
// the application fields returned by the service without embedding protobuf
// runtime state, so values can safely implement fmt.Stringer.
type WorkflowMetadata struct {
	InstanceId       string
	Name             string
	RuntimeStatus    protos.OrchestrationStatus
	CreatedAt        *timestamppb.Timestamp
	LastUpdatedAt    *timestamppb.Timestamp
	Input            *wrapperspb.StringValue
	Output           *wrapperspb.StringValue
	CustomStatus     *wrapperspb.StringValue
	FailureDetails   *protos.TaskFailureDetails
	CompletedAt      *timestamppb.Timestamp
	ParentInstanceId string
	Version          *wrapperspb.StringValue
	ParentAppId      *wrapperspb.StringValue
	StartedAt        *timestamppb.Timestamp
}
type ListInstanceIDsResponse protos.ListInstanceIDsResponse
type GetInstanceHistoryResponse protos.GetInstanceHistoryResponse

func (w WorkflowMetadata) String() string {
	switch w.RuntimeStatus {
	case api.RUNTIME_STATUS_RUNNING:
		return "RUNNING"
	case api.RUNTIME_STATUS_COMPLETED:
		return "COMPLETED"
	case api.RUNTIME_STATUS_CONTINUED_AS_NEW:
		return "CONTINUED_AS_NEW"
	case api.RUNTIME_STATUS_FAILED:
		return "FAILED"
	case api.RUNTIME_STATUS_CANCELED:
		return "CANCELED"
	case api.RUNTIME_STATUS_TERMINATED:
		return "TERMINATED"
	case api.RUNTIME_STATUS_PENDING:
		return "PENDING"
	case api.RUNTIME_STATUS_SUSPENDED:
		return "SUSPENDED"
	case api.RUNTIME_STATUS_STALLED:
		return "STALLED"
	default:
		return ""
	}
}

func workflowMetadataFromProto(metadata *protos.WorkflowMetadata) *WorkflowMetadata {
	if metadata == nil {
		return nil
	}
	return &WorkflowMetadata{
		InstanceId:       metadata.InstanceId,
		Name:             metadata.Name,
		RuntimeStatus:    metadata.RuntimeStatus,
		CreatedAt:        metadata.CreatedAt,
		LastUpdatedAt:    metadata.LastUpdatedAt,
		Input:            metadata.Input,
		Output:           metadata.Output,
		CustomStatus:     metadata.CustomStatus,
		FailureDetails:   metadata.FailureDetails,
		CompletedAt:      metadata.CompletedAt,
		ParentInstanceId: metadata.ParentInstanceId,
		Version:          metadata.Version,
		ParentAppId:      metadata.ParentAppId,
		StartedAt:        metadata.StartedAt,
	}
}
