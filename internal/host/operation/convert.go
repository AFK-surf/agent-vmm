package operation

import (
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func FromStore(operation store.Operation) *hostv1.Operation {
	resource := &hostv1.ResourceReference{Kind: "environment", EnvironmentId: operation.EnvironmentID}
	if strings.HasPrefix(operation.Kind, "vm-") {
		resource = &hostv1.ResourceReference{Kind: "vm", ResourceId: "local"}
	} else if operation.EnvironmentID == "" || operation.EnvironmentID == "system" {
		resource = &hostv1.ResourceReference{Kind: "system", ResourceId: "local"}
	}
	result := &hostv1.Operation{
		Id: operation.ID, Kind: operation.Kind, State: hostv1.OperationState(operation.State), Stage: operation.Stage,
		RequestId: operation.RequestID, StartedAt: timestamppb.New(operation.StartedAt), UpdatedAt: timestamppb.New(operation.UpdatedAt),
		Resource:   resource,
		OutputPath: operation.OutputPath,
	}
	if operation.ErrorReason != "" {
		result.Error = &hostv1.OperationError{Reason: operation.ErrorReason, Message: operation.ErrorMessage, Resource: result.Resource}
	}
	return result
}
