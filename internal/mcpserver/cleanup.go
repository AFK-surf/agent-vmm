package mcpserver

import (
	"context"
	"slices"
	"strconv"
	"time"

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
		if input.Kind != "reset" && input.Kind != "discard" {
			return nil, status.Error(codes.InvalidArgument, "invalid cleanup kind")
		}
		plan, err := adapter.cleanup.PlanCleanup(ctx, input.Kind)
		if err != nil {
			return nil, err
		}
		if plan.Kind != input.Kind || plan.Revision != revision {
			return nil, status.Error(codes.Aborted, "cleanup plan is stale")
		}
		volumes := cleanupVolumes(plan)
		confirmed := append([]string(nil), input.ConfirmedVolumeIDs...)
		slices.Sort(confirmed)
		switch input.Kind {
		case "reset":
			if !input.ConfirmReset || input.ConfirmIrreversible || len(confirmed) != 0 {
				return nil, status.Error(codes.InvalidArgument, "reset confirmation does not match plan")
			}
		case "discard":
			if !input.ConfirmIrreversible || input.ConfirmReset || !slices.Equal(confirmed, volumes) {
				return nil, status.Error(codes.InvalidArgument, "discard confirmation does not match current volumes")
			}
		}
		if err := adapter.cleanup.ExecuteCleanup(ctx, CleanupExecution{Kind: input.Kind, Revision: revision, ConfirmedVolumeIDs: confirmed}); err != nil {
			return nil, err
		}
		adapter.draining.Store(true)
		time.AfterFunc(250*time.Millisecond, adapter.cancel)
		return CleanupResultData{Kind: input.Kind, State: "succeeded"}, nil
	})
}

func cleanupVolumes(plan CleanupPlan) []string {
	result := append([]string(nil), plan.VolumeIDs...)
	slices.Sort(result)
	return result
}
