//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (runtime managedRuntime) vm(ctx context.Context, action string, opts managedOptions) (managedEnvelope, error) {
	summary, err := runtime.getImpactSummary(ctx, opts.operatorSocket)
	if err != nil || summary.GetVm() == nil {
		return managedEnvelope{}, errors.New("current Host VM impact summary is not readable")
	}
	refs := managedWorkloadFacts(summary.GetKnownWorkloads())
	before := summary.GetVm()
	envelope := baseManagedEnvelope("vm", runtime.now().UTC(), map[string]any{"vm": "local"})
	envelope.Source = []string{"host_operator"}
	envelope.Freshness = "current"
	envelope.State = managedVMState(before)
	envelope.Facts = map[string]any{
		"boot_id": before.GetBootId(), "impact_scope": "whole_vm", "known_workloads": refs, "known_workload_count": len(refs),
		"impact_summary_complete": summary.GetComplete(), "impact_summary_overflow": summary.GetOverflow(),
	}
	if !summary.GetComplete() {
		message := summary.GetIssue()
		if message == "" {
			message = "the bounded VM impact summary is incomplete"
		}
		envelope.Partial = true
		envelope.Issue = managedIssue{Code: "impact_summary_partial", Message: message, Layer: "host"}
	}
	if action == "status" {
		return envelope, nil
	}
	// The owner is re-read immediately before mutation. A changed state or boot
	// identity invalidates the operator's whole-VM acknowledgement.
	current, err := runtime.getVM(ctx, opts.operatorSocket)
	if err != nil || current == nil {
		return managedEnvelope{}, errors.New("VM fence could not be re-read")
	}
	if current.GetState() != before.GetState() || current.GetBootId() != before.GetBootId() {
		return managedEnvelope{}, errors.New("VM changed after observation; inspect and retry with the same request id only after reconciling its outcome")
	}
	fence := &hostv1.VMMutationFence{State: before.GetState(), BootId: before.GetBootId(), ObservedWorkloads: summary.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}
	var operation *hostv1.Operation
	if action == "start" {
		operation, err = runtime.startVM(ctx, opts.operatorSocket, opts.requestID, fence)
	} else {
		operation, err = runtime.stopVM(ctx, opts.operatorSocket, opts.requestID, opts.force, fence)
	}
	if err != nil {
		info := cleanupErrorInfo(err)
		reason := ""
		if info != nil {
			reason = info.GetReason()
		}
		if reason == "VM_TERMINAL_PERSIST_FAILED" {
			envelope.State = "unknown_outcome"
			envelope.Stage = "terminal_result_not_saved"
			envelope.Partial = true
			envelope.Issue = managedIssue{Code: "vm_terminal_persist_failed", Message: "the Host accepted the VM mutation but could not save its terminal result; query the same request, then restart the Host service if it remains running", Layer: "host", Retryable: managedBool(true)}
			envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}, {Action: "service_restart", Permission: "administrator"}}
			envelope.Facts["request_id"] = opts.requestID
			if operationID := info.GetMetadata()["operation_id"]; operationID != "" {
				envelope.OperationRef = map[string]any{"owner": "host", "id": operationID}
			}
			return envelope, nil
		}
		if reason == "" || reason == "VM_ADMISSION_OUTCOME_UNKNOWN" {
			envelope.State = "unknown_outcome"
			envelope.Stage = "owner_response_pending"
			envelope.Partial = true
			envelope.Issue = managedIssue{Code: "unknown_outcome", Message: "the Host VM mutation result is unknown; reconcile by retrying only the same request id", Layer: "host", Retryable: managedBool(true)}
			envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}}
			envelope.Facts["request_id"] = opts.requestID
			return envelope, nil
		}
		return managedEnvelope{}, fmt.Errorf("Host owner rejected vm %s: %w", action, err)
	}
	if operation == nil || operation.GetRequestId() != opts.requestID {
		return managedEnvelope{}, errors.New("Host owner returned an unbound operation")
	}
	envelope.State = strings.ToLower(strings.TrimPrefix(operation.GetState().String(), "OPERATION_STATE_"))
	envelope.Stage = operation.GetStage()
	envelope.OperationRef = map[string]any{"owner": "host", "id": operation.GetId()}
	if operation.GetError() != nil {
		envelope.Issue = managedIssue{Code: operation.GetError().GetReason(), Message: boundedManagedText(operation.GetError().GetMessage(), 1024), Layer: "host"}
	}
	switch operation.GetState() {
	case hostv1.OperationState_OPERATION_STATE_RUNNING:
		envelope.NextAction = []managedNextAction{{Action: "operation_follow", Permission: "service_user"}}
	case hostv1.OperationState_OPERATION_STATE_FAILED, hostv1.OperationState_OPERATION_STATE_INTERRUPTED:
		envelope.Partial = true
		envelope.NextAction = []managedNextAction{{Action: "vm_status", Permission: "service_user"}}
	}
	return envelope, nil
}

func managedWorkloadFacts(references []*hostv1.ResourceReference) []map[string]any {
	result := make([]map[string]any, 0, len(references))
	for _, reference := range references {
		fact := map[string]any{"kind": reference.GetKind()}
		if reference.GetEnvironmentId() != "" {
			fact["environment_id"] = reference.GetEnvironmentId()
		}
		if reference.GetResourceId() != "" {
			fact["resource_id"] = reference.GetResourceId()
		}
		if reference.GetGeneration() != 0 {
			fact["generation"] = reference.GetGeneration()
		}
		if reference.GetGenerationId() != "" {
			fact["generation_id"] = reference.GetGenerationId()
		}
		result = append(result, fact)
	}
	return result
}

func (runtime managedRuntime) repair(ctx context.Context, opts managedOptions) (managedEnvelope, error) {
	status, err := runtime.lifecycleStatus(ctx, opts)
	if err != nil {
		return managedEnvelope{}, err
	}
	if err := validateManagedStatusTarget(status); err != nil {
		return managedEnvelope{}, err
	}
	envelope := baseManagedEnvelope("repair", runtime.now().UTC(), targetFromStatus(status, opts))
	envelope.Source = []string{"lifecycle", managedServiceSource}
	envelope.Freshness = "current"
	envelope.State = managedState(status)
	if issue, permission, refused := managedPrivilegeIssue("repair", os.Geteuid()); refused {
		envelope.State = issue.Code
		envelope.Issue = issue
		envelope.NextAction = []managedNextAction{{Action: "repair", Permission: permission}}
		return envelope, nil
	}
	if boolFact(status, "hostLoaded") && !boolFact(status, "hostRunning") && os.Geteuid() != 0 && strings.HasPrefix(stringFact(status, "hostJob"), "system/") {
		envelope.State = "needs_administrator"
		envelope.Issue = managedIssue{Code: "needs_administrator", Message: "the installed system service is loaded but not running", Layer: "service", Retryable: managedBool(true)}
		envelope.NextAction = []managedNextAction{{Action: "service_start", Permission: "administrator"}}
		return envelope, nil
	}
	args := []string{"repair", "--request-id", opts.requestID}
	if opts.registrationID != "" {
		args = append(args, "--registration-id", opts.registrationID)
	}
	args = appendServiceArguments(args, opts)
	stdout, stderr, err := runtime.runHelper(ctx, opts.lifecycle, args)
	var operation map[string]any
	decoded := jsonUnmarshalManaged(stdout, &operation) == nil && stringFact(operation, "requestId") == opts.requestID && stringFact(operation, "targetRegistrationId") == opts.registrationID
	if err != nil && !decoded {
		envelope.State = "transition_in_progress"
		envelope.Disposition = managedDispositionExclusiveTransition
		envelope.Stage = "lifecycle_response_pending"
		envelope.Partial = true
		envelope.Issue = managedIssue{Code: "unknown_outcome", Message: "repair may have been accepted; retry only the same request id and registration target", Layer: "lifecycle", Retryable: managedBool(true)}
		envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}}
		envelope.Facts = map[string]any{"request_id": opts.requestID, "transport_error": boundedManagedText(firstNonEmptyManaged(string(stderr), err.Error()), 256)}
		if active := stringFact(operation, "requestId"); active != "" && active != opts.requestID {
			envelope.Facts["active_request_id"] = active
		}
		return envelope, nil
	}
	if !decoded {
		return managedEnvelope{}, errors.New("lifecycle returned an unbound repair operation")
	}
	envelope.State = stringFact(operation, "outcome")
	envelope.Stage = stringFact(operation, "stage")
	envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
	if err != nil {
		envelope.Issue = managedIssue{Code: firstNonEmptyManaged(stringFact(operation, "failureCode"), "repair_failed"), Message: boundedManagedText(firstNonEmptyManaged(stringFact(operation, "failureMessage"), string(stderr)), 1024), Layer: "lifecycle", Retryable: managedBool(boolFact(operation, "retryable"))}
	}
	return envelope, nil
}

func jsonUnmarshalManaged(data []byte, value any) error {
	if len(data) > 1<<20 {
		return errors.New("lifecycle response exceeded 1 MiB")
	}
	if err := json.Unmarshal(data, value); err != nil {
		return errors.New("lifecycle response was not valid JSON")
	}
	return nil
}

func managedVMState(vm *hostv1.VM) string {
	return strings.ToLower(strings.TrimPrefix(vm.GetState().String(), "VM_STATE_"))
}

func getManagedImpactSummary(ctx context.Context, socket string) (*hostv1.GetVMImpactSummaryResponse, error) {
	connection, err := managedOperatorConnection(socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return hostv1.NewOperatorServiceClient(connection).GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
}
func getManagedVM(ctx context.Context, socket string) (*hostv1.VM, error) {
	connection, err := managedOperatorConnection(socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).GetVM(ctx, &hostv1.GetVMRequest{})
	return response.GetVm(), err
}
func startManagedVM(ctx context.Context, socket, requestID string, fence *hostv1.VMMutationFence) (*hostv1.Operation, error) {
	connection, err := managedOperatorConnection(socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).StartVM(ctx, &hostv1.StartVMRequest{RequestId: requestID, Fence: fence})
	return response.GetOperation(), err
}
func stopManagedVM(ctx context.Context, socket, requestID string, force bool, fence *hostv1.VMMutationFence) (*hostv1.Operation, error) {
	connection, err := managedOperatorConnection(socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).StopVM(ctx, &hostv1.StopVMRequest{RequestId: requestID, Force: force, Fence: fence})
	return response.GetOperation(), err
}
