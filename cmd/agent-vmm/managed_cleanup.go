//go:build darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

const managedCleanupImpactLimit = 100

func (runtime managedRuntime) cleanup(ctx context.Context, action string, opts managedOptions) (managedEnvelope, error) {
	kind := hostv1.CleanupKind_CLEANUP_KIND_RESET
	if opts.cleanupKind == "discard" {
		kind = hostv1.CleanupKind_CLEANUP_KIND_DISCARD
	}
	now := runtime.now().UTC()
	envelope := baseManagedEnvelope("cleanup", now, map[string]any{"environment_id": opts.environmentID})
	envelope.Source = []string{"host_cleanup_owner"}
	envelope.Freshness = "current"
	if action == "preview" {
		plan, err := runtime.planCleanup(ctx, opts.operatorSocket, opts.environmentID, kind)
		if err != nil || plan == nil {
			return managedEnvelope{}, errors.New("current cleanup plan is not readable")
		}
		if plan.GetEnvironmentId() != opts.environmentID || plan.GetKind() != kind || plan.GetRevision() == 0 {
			return managedEnvelope{}, errors.New("Host returned an unbound cleanup plan")
		}
		if len(plan.GetImpacts()) > managedCleanupImpactLimit {
			return managedEnvelope{}, fmt.Errorf("cleanup plan has %d impacts; the managed CLI limit is %d", len(plan.GetImpacts()), managedCleanupImpactLimit)
		}
		envelope.State = "planned"
		envelope.Stage = "preview"
		envelope.Facts = map[string]any{
			"kind":                        opts.cleanupKind,
			"plan_revision":               plan.GetRevision(),
			"impacts":                     managedCleanupImpacts(plan.GetImpacts()),
			"impact_count":                len(plan.GetImpacts()),
			"reclaimable_bytes":           nil,
			"reclaimable_bytes_available": false,
		}
		return envelope, nil
	}
	plan := &hostv1.CleanupPlan{EnvironmentId: opts.environmentID, Kind: kind, Revision: opts.planRevision}
	confirmed := append([]string(nil), opts.confirmVolumes...)
	sort.Strings(confirmed)
	envelope.State = "submitted"
	envelope.Stage = "execute"
	envelope.Facts = map[string]any{
		"kind":                        opts.cleanupKind,
		"plan_revision":               opts.planRevision,
		"confirmed_volume_ids":        confirmed,
		"reclaimable_bytes":           nil,
		"reclaimable_bytes_available": false,
	}
	operation, err := runtime.executeCleanup(ctx, opts.operatorSocket, opts.requestID, plan, confirmed)
	if err != nil {
		info := cleanupErrorInfo(err)
		reason := ""
		if info != nil {
			reason = info.GetReason()
		}
		if reason == "CLEANUP_PLAN_STALE" {
			return managedEnvelope{}, errors.New("cleanup references changed during execution; request a new plan")
		}
		if (reason == "CLEANUP_ADMISSION_TIMEOUT" || reason == "CLEANUP_ADMISSION_CANCELED") && ctx.Err() == nil {
			if reason == "CLEANUP_ADMISSION_CANCELED" {
				return managedEnvelope{}, errors.New("the Host canceled cleanup admission before it accepted an operation; inspect the competing mutation and request a new plan")
			}
			return managedEnvelope{}, errors.New("cleanup admission timed out before the Host accepted an operation; wait for the competing mutation and request a new plan")
		}
		if reason == "CLEANUP_TERMINAL_PERSIST_FAILED" {
			envelope.State = "unknown_outcome"
			envelope.Stage = "terminal_result_not_saved"
			envelope.Partial = true
			envelope.Issue = managedIssue{Code: "cleanup_terminal_persist_failed", Message: "the Host accepted cleanup but could not save its terminal result; query the same request, then restart the Host service if it remains running", Layer: "host", Retryable: managedBool(true)}
			envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}, {Action: "service_restart", Permission: "administrator"}}
			envelope.Facts["request_id"] = opts.requestID
			if operationID := info.GetMetadata()["operation_id"]; operationID != "" {
				envelope.OperationRef = map[string]any{"owner": "host", "id": operationID}
			}
			return envelope, nil
		}
		if reason == "" || reason == "CLEANUP_ADMISSION_OUTCOME_UNKNOWN" || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
			envelope.State = "unknown_outcome"
			envelope.Stage = "owner_response_pending"
			envelope.Partial = true
			envelope.Issue = managedIssue{Code: "unknown_outcome", Message: "the Host cleanup result is unknown; reconcile by retrying only the same request id and plan revision", Layer: "host", Retryable: managedBool(true)}
			envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}}
			envelope.Facts["request_id"] = opts.requestID
			return envelope, nil
		}
		return managedEnvelope{}, fmt.Errorf("Host cleanup owner rejected execution: %w", err)
	}
	if operation == nil || operation.GetRequestId() != opts.requestID {
		return managedEnvelope{}, errors.New("Host cleanup owner returned an unbound operation")
	}
	envelope.State = strings.ToLower(strings.TrimPrefix(operation.GetState().String(), "OPERATION_STATE_"))
	envelope.Stage = operation.GetStage()
	envelope.OperationRef = map[string]any{"owner": "host", "id": operation.GetId()}
	if operation.GetError() != nil {
		envelope.Issue = managedIssue{Code: operation.GetError().GetReason(), Message: boundedManagedText(operation.GetError().GetMessage(), 1024), Layer: "host", Retryable: nil}
	}
	if operation.GetState() == hostv1.OperationState_OPERATION_STATE_FAILED || operation.GetState() == hostv1.OperationState_OPERATION_STATE_INTERRUPTED {
		envelope.Partial = true
		envelope.NextAction = []managedNextAction{{Action: "cleanup_preview_new_request", Permission: "service_user"}}
	}
	return envelope, nil
}

func cleanupErrorInfo(err error) *errdetails.ErrorInfo {
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok {
			return info
		}
	}
	return nil
}

func managedCleanupImpacts(impacts []*hostv1.CleanupImpact) []map[string]any {
	result := make([]map[string]any, 0, len(impacts))
	for _, impact := range impacts {
		resource := impact.GetResource()
		result = append(result, map[string]any{
			"kind": resource.GetKind(), "environment_id": resource.GetEnvironmentId(),
			"resource_id": resource.GetResourceId(), "generation": resource.GetGeneration(),
			"generation_id": resource.GetGenerationId(), "effect": impact.GetEffect(),
			"irreversible": impact.GetIrreversible(), "blocker": impact.GetBlocker(),
		})
	}
	return result
}

func planManagedCleanup(ctx context.Context, socket, environmentID string, kind hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
	connection, err := managedOperatorConnection(ctx, socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	request := &hostv1.PlanEnvironmentRequest{EnvironmentId: environmentID}
	var response *hostv1.CleanupPlanResponse
	if kind == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		response, err = client.PlanResetEnvironment(ctx, request)
	} else {
		response, err = client.PlanDiscardEnvironment(ctx, request)
	}
	return response.GetPlan(), err
}

func executeManagedCleanup(ctx context.Context, socket, requestID string, plan *hostv1.CleanupPlan, confirmedVolumes []string) (*hostv1.Operation, error) {
	connection, err := managedOperatorConnection(ctx, socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: requestID, Plan: plan, ConfirmedVolumeIds: confirmedVolumes})
	return response.GetOperation(), err
}
