package mcpserver

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (adapter *Adapter) cleanupExecute(ctx context.Context, _ *mcp.CallToolRequest, input CleanupExecuteInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 2*time.Minute, true, func(ctx context.Context) (any, error) {
		revision, err := strconv.ParseUint(input.PlanRevision, 10, 64)
		if err != nil || revision == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid cleanup revision")
		}
		plan, err := adapter.currentCleanupPlan(ctx, input.Kind)
		if err != nil {
			return nil, err
		}
		if plan.GetRevision() != revision {
			return nil, status.Error(codes.Aborted, "cleanup plan is stale")
		}
		volumes := cleanupVolumes(plan)
		switch input.Kind {
		case "reset":
			if !input.ConfirmReset || input.ConfirmIrreversible || len(input.ConfirmedVolumeIDs) != 0 {
				return nil, status.Error(codes.InvalidArgument, "reset confirmation does not match plan")
			}
		case "discard":
			confirmed := append([]string(nil), input.ConfirmedVolumeIDs...)
			slices.Sort(confirmed)
			if !input.ConfirmIrreversible || input.ConfirmReset || !slices.Equal(confirmed, volumes) {
				return nil, status.Error(codes.InvalidArgument, "discard confirmation does not match current volumes")
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid cleanup kind")
		}
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		var response *hostv1.OperationResponse
		if input.Kind == "reset" {
			response, err = adapter.runtime.RequestReset(ctx, &hostv1.EnvironmentOperationRequest{RequestId: requestID, PlanRevision: plan.GetRevision()})
		} else {
			response, err = adapter.runtime.RequestDiscard(ctx, &hostv1.EnvironmentOperationRequest{RequestId: requestID, PlanRevision: plan.GetRevision()})
		}
		if err != nil {
			return nil, err
		}
		result := operationData(response.GetOperation())
		adapter.draining.Store(true)
		time.AfterFunc(250*time.Millisecond, adapter.cancel)
		return result, nil
	})
}

func (adapter *Adapter) currentCleanupPlan(ctx context.Context, kind string) (*hostv1.CleanupPlan, error) {
	var response *hostv1.CleanupPlanResponse
	var err error
	switch kind {
	case "reset":
		response, err = adapter.runtime.PlanResetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
	case "discard":
		response, err = adapter.runtime.PlanDiscardEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid cleanup kind")
	}
	if err != nil {
		return nil, err
	}
	if strings.ToLower(strings.TrimPrefix(response.GetPlan().GetKind().String(), "CLEANUP_KIND_")) != kind {
		return nil, status.Error(codes.Aborted, "cleanup plan kind changed")
	}
	return response.GetPlan(), nil
}

func cleanupVolumes(plan *hostv1.CleanupPlan) []string {
	var result []string
	for _, impact := range plan.GetImpacts() {
		if impact.GetIrreversible() && impact.GetResource().GetKind() == "volume" {
			result = append(result, impact.GetResource().GetResourceId())
		}
	}
	slices.Sort(result)
	return result
}

func operationData(operation *hostv1.Operation) OperationData {
	result := OperationData{ID: operation.GetId(), Kind: operation.GetKind(), State: strings.ToLower(strings.TrimPrefix(operation.GetState().String(), "OPERATION_STATE_")), Stage: operation.GetStage()}
	if failure := operation.GetError(); failure != nil {
		code := failure.GetReason()
		if !stableErrorCode(code) {
			code = "INTERNAL"
		}
		result.Error = &ToolError{Code: code, Message: "cleanup operation failed", Resource: &Resource{Kind: failure.GetResource().GetKind(), ID: failure.GetResource().GetResourceId()}}
	}
	return result
}

func stableErrorCode(code string) bool {
	for _, candidate := range []string{"INVALID_ARGUMENT", "NOT_FOUND", "ALREADY_EXISTS", "FAILED_PRECONDITION", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "CONFLICT", "TIMEOUT", "CANCELED", "UNAVAILABLE", "INTERNAL", "CONTAINER_NOT_RUNNING", "CONTAINER_RUNNING", "IMAGE_NOT_FOUND", "VOLUME_MOUNTED", "PLAN_STALE", "LEASE_REVOKED", "ALLOCATION_RETAINED"} {
		if code == candidate {
			return true
		}
	}
	return false
}
