package remoteconnector

import (
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

type disposalClient struct {
	hostv1.RemoteComputeServiceClient
	failure      error
	plan         *hostv1.AllocationCleanupPlan
	planRequests *[]*hostv1.PlanCleanupAllocationRequest
}

func (client disposalClient) DisposeAllocation(context.Context, *hostv1.DisposeAllocationRequest, ...grpc.CallOption) (*hostv1.AllocationResponse, error) {
	return nil, client.failure
}

func (client disposalClient) PlanCleanupAllocation(_ context.Context, request *hostv1.PlanCleanupAllocationRequest, _ ...grpc.CallOption) (*hostv1.AllocationCleanupPlanResponse, error) {
	*client.planRequests = append(*client.planRequests, request)
	return &hostv1.AllocationCleanupPlanResponse{Plan: client.plan}, client.failure
}

func TestCleanupPlanCarriesCurrentGenerationAndOwnerScope(t *testing.T) {
	var requests []*hostv1.PlanCleanupAllocationRequest
	handler := commandHandler{registrationID: "controller", connectionInstance: "connection", client: disposalClient{planRequests: &requests, plan: &hostv1.AllocationCleanupPlan{AllocationId: "allocation", AllocationGeneration: 13, Revision: 9, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, VolumeIds: []string{"state"}}}}
	command := &remotev1.ControllerCommand{CommandId: "observe-cleanup", Sequence: 7, ConnectionEpoch: 3, Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: "allocation", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET}}}
	result := handler.handle(context.Background(), command)
	plan := result.GetCleanupPlan()
	if plan.GetAllocationGeneration() != 13 || plan.GetRevision() != 9 || plan.GetAllocationId() != "allocation" || plan.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_RESET || len(plan.GetVolumeIds()) != 1 {
		t.Fatalf("cleanup projection lost current facts: %v", result)
	}
	if len(requests) != 1 || requests[0].GetRegistrationId() != "controller" || requests[0].GetConnectionInstance() != "connection" || requests[0].GetAllocationId() != "allocation" {
		t.Fatalf("cleanup observation lost current owner: %v", requests)
	}
}

func TestDisposalRefreshCarriesOnlyExactPreAcceptanceAuthority(t *testing.T) {
	command := &remotev1.ControllerCommand{CommandId: "dispose", TargetRevision: 7, Command: &remotev1.ControllerCommand_DisposeAllocation{DisposeAllocation: &remotev1.DisposeAllocation{AllocationId: "allocation", ExpectedGeneration: 1, ExpectedRevision: 7, RequestId: "request"}}}
	makeError := func(reason, allocation, generation string) error {
		value, err := status.New(codes.Aborted, "rejected").WithDetails(&errdetails.ErrorInfo{Reason: reason, Metadata: map[string]string{"allocation_id": allocation, "generation": generation, "revision": "8"}})
		if err != nil {
			t.Fatal(err)
		}
		return value.Err()
	}
	handler := commandHandler{client: disposalClient{failure: makeError("DISPOSAL_REVISION_NOT_ACCEPTED", "allocation", "1")}}
	result := handler.handle(context.Background(), command)
	if result.GetReason() != remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED || result.GetAllocation().GetRevision() != 8 || result.GetAllocation().GetGeneration() != 1 || result.GetAllocation().GetAllocationId() != "allocation" {
		t.Fatalf("missing exact rejected scope: %v", result)
	}
	for _, failure := range []error{makeError("ALLOCATION_DISPOSAL_CONFLICT", "allocation", "1"), makeError("DISPOSAL_REVISION_NOT_ACCEPTED", "other", "1"), makeError("DISPOSAL_REVISION_NOT_ACCEPTED", "allocation", "2")} {
		handler.client = disposalClient{failure: failure}
		if result := handler.handle(context.Background(), command); result.GetAllocation() != nil {
			t.Fatalf("untrusted scope refreshed: %v", result)
		}
	}
}
