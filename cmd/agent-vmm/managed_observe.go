//go:build darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (runtime managedRuntime) inspect(ctx context.Context, command string, opts managedOptions) (managedEnvelope, error) {
	status, err := runtime.lifecycleStatus(ctx, opts)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			now := runtime.now().UTC()
			envelope := baseManagedEnvelope(command, now, explicitManagedTarget(opts))
			envelope.Partial = true
			envelope.Issue = managedIssue{Code: "observation_timeout", Message: command + " reached its observation budget", Layer: "cli", Retryable: managedBool(true)}
			envelope.NextAction = []managedNextAction{{Action: command, Permission: "service_user"}}
			return envelope, nil
		}
		return managedEnvelope{}, err
	}
	if err := validateManagedStatusTarget(status); err != nil {
		return managedEnvelope{}, err
	}
	now := runtime.now().UTC()
	envelope := baseManagedEnvelope(command, now, targetFromStatus(status, opts))
	envelope.Source = []string{"lifecycle", "launchd"}
	envelope.Facts = status
	envelope.State = managedState(status)
	envelope.Stage, envelope.Issue = managedStageAndIssue(status)
	envelope.Freshness = managedFreshness(status, now)
	if scope, scopeErr := runtime.resolveScope(ctx, status, opts); scopeErr != nil {
		envelope.Partial = true
		envelope.Target["scope_status"] = "unavailable"
		if envelope.Issue == nil {
			envelope.Issue = managedIssue{Code: "target_unavailable", Message: boundedManagedText(scopeErr.Error(), 512), Layer: "host", Retryable: managedBool(true)}
		}
	} else {
		for key, value := range scope {
			envelope.Target[key] = value
		}
		envelope.Target["scope_status"] = "resolved"
	}
	if operation, ok := status["operation"].(map[string]any); ok {
		if id := stringFact(operation, "operationId"); id != "" {
			envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": id}
		}
	}
	if envelope.State != "healthy" {
		envelope.NextAction = []managedNextAction{{Action: "repair", Permission: "administrator"}}
	}
	if command == "diagnose" {
		if boolFact(status, "hostReadable") {
			envelope.Source = append(envelope.Source, "host_operator")
			envelope.Facts["resources"] = map[string]any{"host": status["hostResources"], "guest": status["guestResources"], "observed_at": status["snapshotObservedAt"]}
		} else {
			envelope.Partial = true
			if envelope.Issue == nil {
				envelope.Issue = managedIssue{Code: "host_unreadable", Message: "Host Operator facts are unavailable", Layer: "host", Retryable: managedBool(true)}
			}
		}
	}
	return envelope, nil
}

func (runtime managedRuntime) resolveScope(ctx context.Context, status map[string]any, opts managedOptions) (map[string]any, error) {
	if opts.registrationID == "" && opts.environmentID == "" && opts.workloadID == "" {
		return map[string]any{}, nil
	}
	if !boolFact(status, "hostReadable") {
		return nil, errors.New("Host Operator cannot resolve the requested scope")
	}
	socket := opts.operatorSocket
	if socket == "" {
		socket = managedOperatorSocket(status)
	}
	result := map[string]any{}
	if opts.environmentID != "" {
		environment, err := runtime.getEnvironment(ctx, socket, opts.environmentID)
		if err != nil || environment == nil || environment.GetId() != opts.environmentID {
			return nil, errors.New("the requested environment was not found")
		}
		if opts.registrationID != "" && environment.GetOwner().GetRegistrationId() != opts.registrationID {
			return nil, errors.New("the requested environment does not belong to the registration")
		}
		result["environment_origin"] = strings.ToLower(strings.TrimPrefix(environment.GetOrigin().String(), "ENVIRONMENT_ORIGIN_"))
		result["environment_inventory"] = strings.ToLower(strings.TrimPrefix(environment.GetInventoryState().String(), "INVENTORY_STATE_"))
	}
	if opts.registrationID != "" {
		registration, err := runtime.getRegistration(ctx, socket, opts.registrationID)
		if err != nil || registration == nil || registration.GetId() != opts.registrationID {
			return nil, errors.New("the requested registration was not found")
		}
		if len(registration.GetAllocations()) > managedMaxListLimit {
			return nil, errors.New("registration allocation inventory exceeds the 100 item observation bound")
		}
		result["registration_state"] = strings.ToLower(strings.TrimPrefix(registration.GetState().String(), "REMOTE_REGISTRATION_STATE_"))
		result["connection_state"] = strings.ToLower(strings.TrimPrefix(registration.GetConnectionState().String(), "REMOTE_CONNECTION_STATE_"))
		if opts.workloadID != "" {
			var matched *hostv1.RemoteAllocation
			for _, allocation := range registration.GetAllocations() {
				if allocation.GetLease().GetWorkloadRef() != opts.workloadID {
					continue
				}
				if matched != nil {
					return nil, errors.New("the workload maps to multiple local allocations")
				}
				matched = allocation
			}
			if matched == nil {
				return nil, errors.New("the requested workload has no local allocation")
			}
			if opts.environmentID != "" && matched.GetEnvironmentId() != opts.environmentID {
				return nil, errors.New("the requested workload does not map to the environment")
			}
			result["allocation_id"] = matched.GetAllocationId()
			result["allocation_generation"] = matched.GetGeneration()
			result["lease_state"] = strings.ToLower(strings.TrimPrefix(matched.GetLease().GetState().String(), "LEASE_STATE_"))
		}
	}
	return result, nil
}

func (runtime managedRuntime) lifecycleStatus(ctx context.Context, opts managedOptions) (map[string]any, error) {
	args := []string{"status"}
	if opts.registrationID != "" {
		args = append(args, "--registration-id", opts.registrationID)
	}
	args = appendServiceArguments(args, opts)
	stdout, stderr, err := runtime.runHelper(ctx, opts.lifecycle, args)
	if err != nil {
		return nil, fmt.Errorf("lifecycle status failed: %s", boundedManagedText(firstNonEmptyManaged(string(stderr), err.Error()), 1024))
	}
	if len(stdout) > managedDiagnoseBytes {
		return nil, errors.New("lifecycle status exceeded 2 MiB")
	}
	var status map[string]any
	if err := json.Unmarshal(stdout, &status); err != nil {
		return nil, errors.New("lifecycle status was not valid JSON")
	}
	return status, nil
}

func (runtime managedRuntime) lifecycleOperation(ctx context.Context, opts managedOptions) (map[string]any, error) {
	args := []string{"operation", "--operation-id", opts.operationID}
	args = appendServiceArguments(args, opts)
	stdout, stderr, err := runtime.runHelper(ctx, opts.lifecycle, args)
	if err != nil {
		return nil, fmt.Errorf("lifecycle operation failed: %s", boundedManagedText(firstNonEmptyManaged(string(stderr), err.Error()), 1024))
	}
	if len(stdout) > 1<<20 {
		return nil, errors.New("lifecycle operation exceeded 1 MiB")
	}
	var operation map[string]any
	if err := json.Unmarshal(stdout, &operation); err != nil || stringFact(operation, "operationId") != opts.operationID {
		return nil, errors.New("lifecycle operation was not found")
	}
	return operation, nil
}

func (runtime managedRuntime) operation(ctx context.Context, subcommand string, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
	status, err := runtime.lifecycleStatus(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := validateManagedStatusTarget(status); err != nil {
		return nil, err
	}
	target := targetFromStatus(status, opts)
	socket := opts.operatorSocket
	if socket == "" {
		socket = managedOperatorSocket(status)
	}
	observe := func() (managedEnvelope, bool, error) {
		now := runtime.now().UTC()
		if opts.owner == "lifecycle" {
			raw, err := runtime.lifecycleOperation(ctx, opts)
			if err != nil {
				return managedEnvelope{}, false, err
			}
			if registrationID := stringFact(raw, "targetRegistrationId"); registrationID != "" {
				target["registration_id"] = registrationID
			}
			envelope := baseManagedEnvelope("operation", now, target)
			envelope.Source = []string{"lifecycle_operation"}
			envelope.Facts = map[string]any{"operation": raw}
			envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": opts.operationID}
			envelope.State = firstNonEmptyManaged(stringFact(raw, "outcome"), "unknown")
			envelope.Stage = firstNonEmptyManaged(stringFact(raw, "stage"), stringFact(raw, "failureStage"), stringFact(raw, "kind"), "unknown")
			envelope.Freshness = freshnessFromMillis(numberFact(raw, "updatedAt"), now)
			if envelope.State == "failed" {
				envelope.Issue = managedIssue{Code: firstNonEmptyManaged(stringFact(raw, "failureCode"), "operation_failed"), Message: boundedManagedText(stringFact(raw, "failureMessage"), 1024), Layer: "lifecycle", Retryable: optionalBoolFact(raw, "retryable")}
			}
			terminal := envelope.State == "succeeded" || envelope.State == "failed"
			return envelope, terminal, nil
		}
		operation, err := runtime.getOperation(ctx, socket, opts.operationID)
		if err != nil {
			return managedEnvelope{}, false, err
		}
		if operation == nil || operation.GetId() != opts.operationID {
			return managedEnvelope{}, false, errors.New("host operation was not found")
		}
		envelope := baseManagedEnvelope("operation", now, target)
		envelope.Source = []string{"host_operation"}
		envelope.OperationRef = map[string]any{"owner": "host", "id": opts.operationID}
		envelope.State = strings.ToLower(strings.TrimPrefix(operation.GetState().String(), "OPERATION_STATE_"))
		envelope.Stage = firstNonEmptyManaged(operation.GetStage(), "unknown")
		envelope.Freshness = freshnessFromTime(operation.GetUpdatedAt().AsTime(), now)
		envelope.Facts = map[string]any{"kind": operation.GetKind(), "request_id": operation.GetRequestId(), "resource": operation.GetResource()}
		if operation.GetError() != nil {
			envelope.Issue = managedIssue{Code: operation.GetError().GetReason(), Message: boundedManagedText(operation.GetError().GetMessage(), 1024), Layer: "host", Retryable: nil}
		}
		terminal := operation.GetState() == hostv1.OperationState_OPERATION_STATE_SUCCEEDED || operation.GetState() == hostv1.OperationState_OPERATION_STATE_FAILED || operation.GetState() == hostv1.OperationState_OPERATION_STATE_INTERRUPTED
		return envelope, terminal, nil
	}

	first, terminal, err := observe()
	if err != nil || subcommand == "get" {
		return []managedEnvelope{first}, err
	}
	result := []managedEnvelope{first}
	if emit != nil {
		if err := emit(first); err != nil {
			return result, err
		}
	}
	if terminal {
		return result, nil
	}
	for {
		if err := runtime.sleep(ctx, managedPollInterval); err != nil {
			last := result[len(result)-1]
			last.Partial = true
			last.Issue = managedIssue{Code: "follow_timeout", Message: "follow reached its 60 second observation budget", Layer: opts.owner, Retryable: managedBool(true)}
			last.NextAction = []managedNextAction{{Action: "operation_follow", Permission: "service_user"}}
			result = append(result, last)
			if emit != nil {
				if err := emit(last); err != nil {
					return result, err
				}
			}
			return result, nil
		}
		next, done, observeErr := observe()
		if observeErr != nil {
			return result, observeErr
		}
		result = append(result, next)
		if emit != nil {
			if err := emit(next); err != nil {
				return result, err
			}
		}
		if done {
			return result, nil
		}
	}
}

func (runtime managedRuntime) logs(ctx context.Context, opts managedOptions) (managedEnvelope, error) {
	now := runtime.now().UTC()
	status, err := runtime.lifecycleStatus(ctx, opts)
	if err != nil {
		return managedEnvelope{}, err
	}
	if err := validateManagedStatusTarget(status); err != nil {
		return managedEnvelope{}, err
	}
	envelope := baseManagedEnvelope("logs", now, targetFromStatus(status, opts))
	envelope.State = "available"
	envelope.Stage = "read"
	envelope.Freshness = "current"
	if opts.component != "" {
		records, cursor, gap, readErr := readManagedLocalLogs(status, opts, now)
		if readErr != nil {
			return managedEnvelope{}, readErr
		}
		envelope.Source = []string{opts.component + "_log"}
		envelope.Logs, envelope.Cursor, envelope.Gap = records, cursor, gap
		if opts.operationID != "" {
			envelope.OperationRef = map[string]any{"owner": "host", "id": opts.operationID}
		}
		return envelope, nil
	}
	socket := opts.operatorSocket
	if socket == "" {
		socket = managedOperatorSocket(status)
	}
	environment, scopeErr := runtime.getEnvironment(ctx, socket, opts.environmentID)
	if scopeErr != nil || environment == nil || environment.GetId() != opts.environmentID {
		return managedEnvelope{}, errors.New("the requested environment was not found")
	}
	if opts.registrationID != "" && environment.GetOwner().GetRegistrationId() != opts.registrationID {
		return managedEnvelope{}, errors.New("the requested environment does not belong to the registration")
	}
	matches := 0
	for _, container := range environment.GetContainers() {
		if container.GetId() == opts.containerID {
			matches++
		}
	}
	if matches != 1 {
		return managedEnvelope{}, errors.New("the requested container did not resolve uniquely in the environment")
	}
	envelope.Target["scope_status"] = "resolved"
	records, cursor, gap, readErr := runtime.readLogs(ctx, socket, opts.environmentID, opts.containerID, opts.afterCursor, opts.limit, opts.follow)
	if readErr != nil {
		return managedEnvelope{}, readErr
	}
	envelope.Source = []string{"guest_container_log"}
	if !opts.includeContent {
		for index := range records {
			records[index].Data = "[REDACTED: pass --include-container-output to read container output]"
		}
	}
	envelope.Logs, envelope.Cursor, envelope.Gap = records, cursor, gap
	return envelope, nil
}

func managedOperatorSocket(status map[string]any) string {
	if value := stringFact(status, "operatorSocket"); value != "" {
		return value
	}
	serviceUser := stringFact(status, "serviceUser")
	if account, err := user.Lookup(serviceUser); err == nil {
		return filepath.Join(account.HomeDir, "Library", "Application Support", "AgentVMM", "operator.sock")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "Application Support", "AgentVMM", "operator.sock")
}

func appendServiceArguments(args []string, opts managedOptions) []string {
	if opts.serviceType != "" {
		args = append(args, "--service-type", opts.serviceType)
	}
	if opts.serviceUser != "" {
		args = append(args, "--service-user", opts.serviceUser)
	}
	if opts.serviceApp != "" {
		args = append(args, "--service-app", opts.serviceApp)
	}
	return args
}

func runManagedHelper(ctx context.Context, path string, arguments []string) ([]byte, []byte, error) {
	command := exec.CommandContext(ctx, path, arguments...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func managedOperatorConnection(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	if socket == "" || !filepath.IsAbs(socket) {
		return nil, errors.New("operator socket is unavailable")
	}
	return grpc.NewClient("passthrough:///agent-vmm-managed", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
}

func getManagedOperation(ctx context.Context, socket, operationID string) (*hostv1.Operation, error) {
	connection, err := managedOperatorConnection(ctx, socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).GetOperation(ctx, &hostv1.GetOperationRequest{OperationId: operationID})
	if err != nil || response == nil {
		return nil, err
	}
	return response.GetOperation(), nil
}

func getManagedEnvironment(ctx context.Context, socket, environmentID string) (*hostv1.AgentEnvironment, error) {
	connection, err := managedOperatorConnection(ctx, socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: environmentID})
	if err != nil || response == nil {
		return nil, err
	}
	return response.GetEnvironment(), nil
}

func getManagedRegistration(ctx context.Context, socket, registrationID string) (*hostv1.RemoteRegistration, error) {
	connection, err := managedOperatorConnection(ctx, socket)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
	if err != nil || response == nil {
		return nil, err
	}
	return response.GetRegistration(), nil
}
