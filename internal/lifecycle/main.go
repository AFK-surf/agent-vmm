//go:build darwin || linux

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

const (
	version                          = "0.1.0"
	productionHostLabel              = bundle.HostBundleIdentifier
	maintenanceRestoreWindowAttempts = 3
	statusObservationBudget          = 4 * time.Second
	// hostStopTimeout bounds a Host stop. The service manager kills the Host
	// after the 90 s stop timeout of its service definition.
	hostStopTimeout = 90 * time.Second
)

type lifecycleProgress struct {
	Event       string `json:"event"`
	RequestID   string `json:"requestId,omitempty"`
	ElapsedMS   int64  `json:"elapsedMs,omitempty"`
	VMState     string `json:"vmState,omitempty"`
	HealthState string `json:"healthState,omitempty"`
}

func reportLifecycleProgress(progress lifecycleProgress) {
	data, err := json.Marshal(progress)
	if err == nil {
		_, _ = fmt.Fprintln(os.Stderr, string(data))
	}
}

// releaseID is injected by the Host package build for diagnostics. The outer
// Server descriptor digest, not this display value, authorizes archive bytes.
var releaseID = version

var errHostServiceNotFound = errors.New("host service not found")

// The helper is the shared controller local operation boundary. Its durable
// request identity and observation-only recovery refine the local appliance
// lifecycle. controller admission remains server-owned and is deliberately absent
// from this local status. The install command delegates lifecycle ordering to
// LifecycleCoordinator; repair remains the narrow host-service repair action.

type lifecycleOperation struct {
	Cutover               *cutoverProgress `json:"cutover,omitempty"`
	OperationID           string           `json:"operationId"`
	RequestID             string           `json:"requestId"`
	Kind                  string           `json:"kind"`
	TargetRegistrationID  string           `json:"targetRegistrationId,omitempty"`
	TargetReleaseID       string           `json:"targetReleaseId,omitempty"`
	TargetArtifactSHA256  string           `json:"targetArtifactSha256,omitempty"`
	TargetArtifactSize    uint64           `json:"targetArtifactSize,omitempty"`
	Stage                 string           `json:"stage,omitempty"`
	Outcome               string           `json:"outcome"`
	Attempt               int              `json:"attempt,omitempty"`
	FailureStage          string           `json:"failureStage,omitempty"`
	FailureCode           string           `json:"failureCode,omitempty"`
	FailureMessage        string           `json:"failureMessage,omitempty"`
	Retryable             bool             `json:"retryable,omitempty"`
	StartedAt             int64            `json:"startedAt"`
	UpdatedAt             int64            `json:"updatedAt"`
	CorrelationRequestID  string           `json:"correlationRequestId,omitempty"`
	SupersededByRequestID string           `json:"supersededByRequestId,omitempty"`
}

func Main() {
	signal.Ignore(syscall.SIGPIPE, syscall.SIGHUP)
	if len(os.Args) < 2 {
		fatal("usage: agent-vmm-lifecycle version --json|preflight --json|status|operation|host-maintain|maintenance-status|publish-host|install|host-repair|registration-claim|registration-recover|registration-enable|registration-drain|registration-remove|host-update|uninstall [--service-type agent|daemon|auto] [--service-user USER] [...]")
	}
	if !knownLifecycleCommand(os.Args[1]) {
		fatal("upgrade_required: unknown lifecycle command")
	}
	if os.Args[1] == "version" {
		if len(os.Args) != 3 || os.Args[2] != "--json" {
			fatal("version requires --json")
		}
		if err := json.NewEncoder(os.Stdout).Encode(versionReport()); err != nil {
			fatal("version encoding failed")
		}
		return
	}
	if os.Args[1] == "preflight" {
		executePreflight(os.Args[2:])
		return
	}
	if lifecycleMutationCommand(os.Args[1]) {
		if err := checkMutationUser(os.Geteuid()); err != nil {
			fatal(err.Error())
		}
	}
	serviceRequest, commandArgs, err := extractServiceRequest(os.Args[2:])
	if err != nil {
		fatal(err.Error())
	}
	serviceRequest.action = os.Args[1]
	if executePlatformCommand(os.Args[1], serviceRequest, commandArgs) {
		return
	}
	if localOperatorCommand(os.Args[1]) {
		executeLocalOperator(os.Args[1], serviceRequest, commandArgs)
		return
	}
	if os.Args[1] == "operation" {
		resolved, err := observationPathsForRequest(serviceRequest)
		if err != nil {
			fatal(err.Error())
		}
		var report map[string]any
		if len(commandArgs) == 4 && commandArgs[0] == "--request-id" && commandArgs[2] == "--registration-id" && operationIDPattern.MatchString(commandArgs[1]) && operationIDPattern.MatchString(commandArgs[3]) {
			report, err = readCorrelatedOperation(resolved, commandArgs[1], commandArgs[3])
		} else {
			var id string
			id, err = parseLifecycleOperationArgs(commandArgs)
			if err == nil {
				report, err = readOperationReportByID(resolved, id)
			}
		}
		if err != nil {
			fatal(err.Error())
		}
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			fatal("operation encoding failed")
		}
		return
	}
	if os.Args[1] == "status" {
		resolved, err := observationPathsForRequest(serviceRequest)
		if err != nil {
			fatal(err.Error())
		}
		registrationID, err := parseStatusArgs(commandArgs)
		if err != nil {
			fatal(err.Error())
		}
		ctx, cancel := context.WithTimeout(context.Background(), statusObservationBudget)
		defer cancel()
		if err := json.NewEncoder(os.Stdout).Encode(readStatusContext(ctx, resolved, registrationID)); err != nil {
			fatal("status encoding failed")
		}
		return
	}
	resolved, err := lifecyclePaths(serviceRequest)
	if err != nil {
		fatal(err.Error())
	}
	switch os.Args[1] {
	case "install-operation-ack":
		fatal("upgrade_required: installation handoff was replaced by registration claim")
	case "registration-claim", "registration-recover":
		disablePairing, requestID, descriptorStdin, _, _ := parseInstallArgs("registration-claim", commandArgs)
		if requestID == "" {
			fatal("registration-claim requires --request-id")
		}
		adapter := &productionLifecycleAdapter{value: resolved, requestID: requestID}
		if os.Args[1] == "registration-recover" {
			if !descriptorStdin {
				fatal("registration-recover requires fresh authorization on stdin")
			}
			decoded, err := decodeRecoveryDescriptor(os.Stdin)
			if err != nil {
				fatal(err.Error())
			}
			adapter.recovery = &decoded
		} else if descriptorStdin {
			decoded, err := decodeRegistrationClaimDescriptor(os.Stdin)
			if err != nil {
				fatal(err.Error())
			}
			adapter.descriptor = &decoded
		} else {
			saved, err := productionRegistrationAdapterForResume(resolved, requestID)
			if err != nil {
				fatal(err.Error())
			}
			adapter = saved
		}
		registrationID := ""
		if adapter.recovery != nil {
			registrationID = adapter.recovery.RegistrationID
		} else if adapter.descriptor != nil {
			registrationID = adapter.descriptor.RegistrationID
		} else {
			registrationID = adapter.plan.Descriptor.RegistrationID
		}
		suppliedRequestID := requestID
		requestID, err = originalRegistrationRequestID(resolved, registrationID, requestID, os.Args[1])
		if err != nil {
			fatal(err.Error())
		}
		adapter.requestID = requestID
		if requestID != suppliedRequestID {
			resolved.correlationRequestID = suppliedRequestID
		}
		operation, operationErr := runLifecycleOperationWithFailedRetry(resolved, os.Args[1], requestID, registrationID, "", "", 0, true, func() error {
			return runRegistrationLifecycle(lifecycleAdapterFor(adapter), os.Args[1], LifecycleRequest{RequestID: requestID, DisablePersonalMeshPairing: disablePairing})
		})
		if operationErr == nil {
			reconcileBuiltinMCP(resolved)
		}
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "install", "host-repair", "repair":
		kind := os.Args[1]
		if kind == "host-repair" {
			kind = "repair"
		}
		disablePairing, requestID, oldProtocol, oldResume, registrationID := parseInstallArgs(kind, commandArgs)
		if oldProtocol || oldResume != "" {
			fatal("upgrade_required: use registration-claim")
		}
		requestID = lifecycleRequestID(requestID)
		operation, operationErr := runLifecycleOperationWithFailedRetry(resolved, kind, requestID, registrationID, "", "", 0, true, func() error {
			stage := func(name string) error {
				if kind == "repair" {
					return persistRepairStage(resolved, requestID, registrationID, name)
				}
				return nil
			}
			if err := stage("service"); err != nil {
				return err
			}
			if err := withHostStopTimeout(func(ctx context.Context) error { return reconcileService(ctx, resolved, disablePairing) }); err != nil {
				return err
			}
			if err := stage("vm"); err != nil {
				return err
			}
			if err := ensureVMStarted(resolved, requestID); err != nil {
				return err
			}
			if registrationID != "" {
				if err := stage("registration"); err != nil {
					return err
				}
				if err := waitRemoteRegistrationConnected(resolved, registrationID); err != nil {
					return err
				}
			}
			return stage("complete")
		})
		if operationErr == nil {
			reconcileBuiltinMCP(resolved)
		}
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "drain":
		requestID := requestIDArg(commandArgs)
		operation, operationErr := runLifecycleOperation(resolved, "drain", requestID, func() error {
			return withHostStopTimeout(func(ctx context.Context) error { return drainService(ctx, resolved) })
		})
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "host-update", "update":
		update := parseUpdateArgs(commandArgs)
		var preparationFailure *updatePreparationFailureDescriptor
		if update.preparationFailureStdin {
			decoded, decodeErr := decodeUpdatePreparationFailure(os.Stdin)
			if decodeErr != nil {
				fatal(decodeErr.Error())
			}
			preparationFailure = &decoded
		}
		operation, operationErr := runUpdateLifecycleOperationForArtifact(resolved, update.requestID, update.targetReleaseID, update.artifactSHA256, update.artifactSize, func() error {
			if preparationFailure != nil {
				if err := persistUpdateStage(resolved, update.requestID, update.targetReleaseID, preparationFailure.Stage); err != nil {
					return err
				}
				return updatePreparationError{descriptor: *preparationFailure}
			}
			return updateApplication(resolved, update.sourceApp, update.targetReleaseID, update.requestID)
		})
		if operationErr == nil {
			reconcileBuiltinMCP(resolved)
		}
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "registration-remove", "registration-revoke":
		registrationID, _, requestID := parseRegistrationStateArgs(append(commandArgs, "--state", "draining"))
		if requestID == "" {
			requestID = newOperationID("req")
		}
		operation, operationErr := runLifecycleOperationWithFailedRetry(resolved, "registration-remove", requestID, registrationID, "", "", 0, true, func() error {
			return revokeRemoteRegistration(context.Background(), resolved, registrationID, requestID)
		})
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "registration-enable", "registration-drain", "registration-state":
		stateArgs := commandArgs
		if os.Args[1] == "registration-enable" {
			stateArgs = append(append([]string{}, commandArgs...), "--state", "enabled")
		}
		if os.Args[1] == "registration-drain" {
			stateArgs = append(append([]string{}, commandArgs...), "--state", "draining")
		}
		registrationID, state, requestID := parseRegistrationStateArgs(stateArgs)
		if requestID == "" {
			requestID = newOperationID("req")
		}
		kind := os.Args[1]
		if kind == "registration-state" {
			kind = "registration-" + state.String()
		}
		operation, operationErr := runLifecycleOperationWithFailedRetry(resolved, kind, requestID, registrationID, "", "", 0, true, func() error {
			return setRemoteRegistrationState(context.Background(), resolved, registrationID, state, requestID)
		})
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "uninstall":
		purge, requestID := parseUninstallArgs(commandArgs)
		operation, operationErr := runLifecycleOperation(resolved, "remove", requestID, func() error {
			return withHostStopTimeout(func(ctx context.Context) error { return removeService(ctx, resolved, purge) })
		})
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	default:
		fatal("unknown lifecycle command")
	}
}

func knownLifecycleCommand(command string) bool {
	switch command {
	case "version", "preflight", "status", "operation", "maintenance-status":
		return true
	}
	return lifecycleMutationCommand(command) || localOperatorCommand(command)
}

func lifecycleMutationCommand(command string) bool {
	switch command {
	case "publish-host", "host-maintain", "maintenance", "install", "install-operation-ack", "host-repair", "repair", "registration-claim", "registration-recover", "registration-enable", "registration-drain", "registration-remove", "registration-state", "registration-revoke", "drain", "host-update", "update", "uninstall":
		return true
	default:
		return false
	}
}

func lifecycleRequestID(requestID string) string {
	if requestID != "" {
		return requestID
	}
	return newOperationID("req")
}

func withHostStopTimeout(action func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return action(ctx)
}

func regular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
func directory(path string) bool { info, err := os.Stat(path); return err == nil && info.IsDir() }

func fatal(message string) {
	slog.Error("lifecycle failed", "error", message)
	os.Exit(1)
}
