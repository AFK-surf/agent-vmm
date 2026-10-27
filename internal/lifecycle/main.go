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

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

const (
	version                     = "0.1.0"
	productionHostLabel         = bundle.HostBundleIdentifier
	boundedOperationMaxAttempts = 3
	statusObservationBudget     = 4 * time.Second
	// hostStopTimeout bounds a Host stop. The service manager kills the Host
	// after the 90 s stop timeout of its service definition.
	hostStopTimeout = 90 * time.Second
)

type managedInstallDispositionError struct {
	err         error
	disposition string
}

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

func (err managedInstallDispositionError) Error() string { return err.err.Error() }
func (err managedInstallDispositionError) Unwrap() error { return err.err }
func (err managedInstallDispositionError) ManagedSecretDisposition() string {
	return err.disposition
}

func terminalManagedInstallError(err error) error {
	if err == nil {
		return nil
	}
	return managedInstallDispositionError{err: err, disposition: secretDispositionDestroyTerminal}
}

func managedInstallSecretDisposition(err error) string {
	var dispositionError interface {
		ManagedSecretDisposition() string
	}
	if errors.As(err, &dispositionError) && dispositionError.ManagedSecretDisposition() == secretDispositionDestroyTerminal {
		return secretDispositionDestroyTerminal
	}
	return secretDispositionRetain
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
	Cutover              *cutoverProgress `json:"cutover,omitempty"`
	OperationID          string           `json:"operationId"`
	RequestID            string           `json:"requestId"`
	Kind                 string           `json:"kind"`
	TargetRegistrationID string           `json:"targetRegistrationId,omitempty"`
	TargetReleaseID      string           `json:"targetReleaseId,omitempty"`
	TargetArtifactSHA256 string           `json:"targetArtifactSha256,omitempty"`
	TargetArtifactSize   uint64           `json:"targetArtifactSize,omitempty"`
	Stage                string           `json:"stage,omitempty"`
	Outcome              string           `json:"outcome"`
	Attempt              int              `json:"attempt,omitempty"`
	FailureStage         string           `json:"failureStage,omitempty"`
	FailureCode          string           `json:"failureCode,omitempty"`
	FailureMessage       string           `json:"failureMessage,omitempty"`
	Retryable            bool             `json:"retryable,omitempty"`
	// SecretDisposition is independent from scheduling retryability. Only an
	// explicitly persisted terminal result or durable completion receipt may
	// authorize destruction of the managed install plan.
	SecretDisposition string `json:"secretDisposition,omitempty"`
	StartedAt         int64  `json:"startedAt"`
	UpdatedAt         int64  `json:"updatedAt"`
}

const (
	secretDispositionRetain           = "retain"
	secretDispositionDestroyTerminal  = "destroy_terminal"
	secretDispositionDestroyCompleted = "destroy_completed"
)

func Main() {
	signal.Ignore(syscall.SIGPIPE, syscall.SIGHUP)
	if len(os.Args) < 2 {
		fatal("usage: agent-vmm-lifecycle version --json|preflight --json|status|operation|maintenance|maintenance-status|publish-host|install|install-operation-ack|repair|registration-state|drain|update|uninstall [--service-type agent|daemon|auto] [--service-user USER] [...]")
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
		operationID, err := parseLifecycleOperationArgs(commandArgs)
		if err != nil {
			fatal(err.Error())
		}
		operation, err := readLifecycleOperationByID(resolved, operationID)
		if err != nil {
			fatal(err.Error())
		}
		if err := writeOperation(*operation); err != nil {
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
		operationID, err := parseInstallOperationAckArgs(commandArgs)
		if err != nil {
			fatal(err.Error())
		}
		if err := cleanupInstallPlan(resolved, operationID); err != nil {
			fatal("acknowledged install operation could not be cleaned up: " + err.Error())
		}
	case "install", "repair":
		disablePairing, requestID, operationStdin, resumeOperationID, registrationID := parseInstallArgs(os.Args[1], commandArgs)
		var descriptor *operationHandoffDescriptor
		if os.Args[1] == "install" && operationStdin {
			decoded, decodeErr := decodeOperationHandoffDescriptor(os.Stdin)
			err = decodeErr
			if err != nil {
				fatal(err.Error())
			}
			if requestID == "" || requestID != decoded.OperationID {
				fatal("--request-id must match the install descriptor operation_id")
			}
			descriptor = &decoded
		}
		if operationStdin && resumeOperationID != "" {
			fatal("--operation-stdin and --resume-operation are mutually exclusive")
		}
		var adapter LifecycleAdapter
		var productionAdapter *productionLifecycleAdapter
		completedResume := false
		var resumePreparationErr error
		if resumeOperationID != "" {
			if !operationIDPattern.MatchString(resumeOperationID) || requestID == "" {
				fatal("--resume-operation requires a valid operation id and --request-id")
			}
			completedResume, err = completedInstallExists(resolved, resumeOperationID)
			if err != nil {
				fatal(err.Error())
			}
			if !completedResume {
				productionAdapter, err = productionLifecycleAdapterForResume(resolved, resumeOperationID)
				if err != nil {
					resumePreparationErr = err
					if errors.Is(err, errManagedInstallPlanUnavailable) {
						resumePreparationErr = terminalManagedInstallError(err)
					}
				} else {
					adapter = lifecycleAdapterFor(productionAdapter)
				}
			}
		} else if descriptor != nil {
			productionAdapter = &productionLifecycleAdapter{value: resolved, descriptor: descriptor}
			adapter = lifecycleAdapterFor(productionAdapter)
		} else {
			adapter = newProductionLifecycleAdapter(resolved)
		}
		operationKind := os.Args[1]
		if descriptor != nil || resumeOperationID != "" {
			operationKind = "install-managed"
		}
		// The same request identity must fence both the durable lifecycle
		// operation and the VM start it performs. runLifecycleOperation can
		// synthesize an ID internally, but that happens after this closure has
		// captured requestID and would otherwise send an empty StartVM request.
		requestID = lifecycleRequestID(requestID)
		runOperation := runLifecycleOperation
		if operationKind == "install-managed" {
			runOperation = runManagedLifecycleOperation
		} else if operationKind == "repair" {
			runOperation = func(value paths, _ string, requestID string, action func() error) (lifecycleOperation, error) {
				return runRepairLifecycleOperation(value, requestID, registrationID, action)
			}
		}
		operation, operationErr := runOperation(resolved, operationKind, requestID, func() error {
			if os.Args[1] == "install" {
				if descriptor == nil && resumeOperationID == "" {
					reportLifecycleProgress(lifecycleProgress{Event: "service_reconcile_started", RequestID: requestID})
					if err := withHostStopTimeout(func(ctx context.Context) error { return reconcileService(ctx, resolved, disablePairing) }); err != nil {
						return err
					}
					reportLifecycleProgress(lifecycleProgress{Event: "service_reconcile_completed", RequestID: requestID})
					return ensureVMStarted(resolved, requestID)
				}
				if err := checkMaintenanceInstallTarget(resolved, resumeOperationID); err != nil {
					return err
				}
				if completedResume {
					return nil
				}
				if resumePreparationErr != nil {
					return resumePreparationErr
				}
				coordinator, err := NewLifecycleCoordinator(adapter, LifecycleCoordinatorConfig{})
				if err != nil {
					return err
				}
				if err := coordinator.Run(context.Background(), LifecycleInstall, LifecycleRequest{
					RequestID:                  requestID,
					DisablePersonalMeshPairing: disablePairing,
				}); err != nil {
					return err
				}
				return persistCompletedInstall(resolved, productionAdapter)
			}
			if err := persistRepairStage(resolved, requestID, registrationID, "service"); err != nil {
				return err
			}
			reportLifecycleProgress(lifecycleProgress{Event: "service_reconcile_started", RequestID: requestID})
			if err := withHostStopTimeout(func(ctx context.Context) error { return reconcileService(ctx, resolved, disablePairing) }); err != nil {
				return err
			}
			reportLifecycleProgress(lifecycleProgress{Event: "service_reconcile_completed", RequestID: requestID})
			if err := persistRepairStage(resolved, requestID, registrationID, "vm"); err != nil {
				return err
			}
			if err := ensureVMStarted(resolved, requestID); err != nil {
				return err
			}
			if registrationID != "" {
				if err := persistRepairStage(resolved, requestID, registrationID, "registration"); err != nil {
					return err
				}
				if err := setRemoteRegistrationState(context.Background(), resolved, registrationID, hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, requestID); err != nil {
					return err
				}
				if err := waitRemoteRegistrationConnected(resolved, registrationID); err != nil {
					return err
				}
			}
			return persistRepairStage(resolved, requestID, registrationID, "complete")
		})
		completedOperationID := resumeOperationID
		if descriptor != nil {
			completedOperationID = descriptor.OperationID
		}
		operation, operationErr = finalizeManagedInstallOperation(resolved, completedOperationID, operation, operationErr)
		if os.Args[1] == "install" && operationErr == nil {
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
	case "update":
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
	case "registration-revoke":
		registrationID, _, requestID := parseRegistrationStateArgs(append(commandArgs, "--state", "draining"))
		if requestID == "" {
			requestID = newOperationID("req")
		}
		operation, operationErr := runLifecycleOperationWithFailedRetry(resolved, "registration-revoke", requestID, registrationID, "", "", 0, true, func() error {
			return revokeRemoteRegistration(context.Background(), resolved, registrationID, requestID)
		})
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "registration-state":
		registrationID, state, requestID := parseRegistrationStateArgs(commandArgs)
		if requestID == "" {
			requestID = newOperationID("req")
		}
		operation, operationErr := runRegistrationStateOperation(resolved, "registration-"+state.String(), requestID, func() error {
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

func lifecycleMutationCommand(command string) bool {
	switch command {
	case "publish-host", "maintenance", "install", "install-operation-ack", "repair", "registration-state", "registration-revoke", "drain", "update", "uninstall":
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
