package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	version                     = "0.1.0"
	productionHostLabel         = "com.afk-surf.agent-vmm-host"
	hostTemplateResource        = productionHostLabel + ".plist.template"
	boundedOperationMaxAttempts = 3
	statusObservationBudget     = 4 * time.Second
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

var errLaunchctlServiceNotFound = errors.New("launchctl service not found")

type paths struct {
	home              string
	app               string
	resources         string
	hostPlist         string
	applicationData   string
	logs              string
	operationFile     string
	serviceConfigFile string
	agentPlist        string
	daemonPlist       string
	service           serviceConfig
}

type serviceStatus struct {
	AppInstalled         bool                              `json:"appInstalled"`
	ServiceApp           string                            `json:"serviceApp"`
	ServiceUser          string                            `json:"serviceUser"`
	ServiceUID           int                               `json:"serviceUid"`
	HostJob              string                            `json:"hostJob"`
	OperatorSocket       string                            `json:"operatorSocket"`
	HostInstalled        bool                              `json:"hostInstalled"`
	HostObserved         bool                              `json:"hostObserved"`
	HostObservationIssue string                            `json:"hostObservationIssue,omitempty"`
	HostLoaded           bool                              `json:"hostLoaded"`
	HostRunning          bool                              `json:"hostRunning"`
	HostPID              int                               `json:"hostPid,omitempty"`
	HostLastExitStatus   *int                              `json:"hostLastExitStatus,omitempty"`
	ConnectorInstalled   bool                              `json:"connectorInstalled"`
	ConnectorLoaded      bool                              `json:"connectorLoaded"`
	HostReadable         bool                              `json:"hostReadable"`
	HostHealthy          bool                              `json:"hostHealthy"`
	HostHealth           *hostv1.HealthStatus              `json:"hostHealth,omitempty"`
	RegistrationRead     string                            `json:"registrationRead,omitempty"`
	RegistrationState    string                            `json:"registrationState,omitempty"`
	RegistrationReason   string                            `json:"registrationReason,omitempty"`
	HostResources        *hostv1.LayerResources            `json:"hostResources,omitempty"`
	GuestResources       *hostv1.LayerResources            `json:"guestResources,omitempty"`
	SnapshotObservedAt   string                            `json:"snapshotObservedAt,omitempty"`
	Operation            *lifecycleOperation               `json:"operation,omitempty"`
	Startup              *startupEvidence                  `json:"startup,omitempty"`
	HostStartFailure     *store.StartFailure               `json:"hostStartFailure,omitempty"`
	DataSchema           int                               `json:"dataSchema"`
	DataSchemaIssue      string                            `json:"dataSchemaIssue,omitempty"`
	Snapshot             *hostv1.GetSystemSnapshotResponse `json:"-"`
}

// The helper is the shared Cue/BFT local operation boundary. Its durable
// request identity and observation-only recovery refine the local appliance
// lifecycle. Salix admission remains server-owned and is deliberately absent
// from this local status. The install command delegates lifecycle ordering to
// LifecycleCoordinator; repair remains the narrow host-service repair action.

type lifecycleOperation struct {
	OperationID          string `json:"operationId"`
	RequestID            string `json:"requestId"`
	Kind                 string `json:"kind"`
	TargetRegistrationID string `json:"targetRegistrationId,omitempty"`
	TargetReleaseID      string `json:"targetReleaseId,omitempty"`
	TargetArtifactSHA256 string `json:"targetArtifactSha256,omitempty"`
	TargetArtifactSize   uint64 `json:"targetArtifactSize,omitempty"`
	Stage                string `json:"stage,omitempty"`
	Outcome              string `json:"outcome"`
	Attempt              int    `json:"attempt,omitempty"`
	FailureStage         string `json:"failureStage,omitempty"`
	FailureCode          string `json:"failureCode,omitempty"`
	FailureMessage       string `json:"failureMessage,omitempty"`
	Retryable            bool   `json:"retryable,omitempty"`
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

func main() {
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
	if lifecycleMutationCommand(os.Args[1]) && os.Geteuid() == 0 {
		fatal("lifecycle business mutations must run as the configured non-root service user; use agent-vmm-service-executor only for fixed system jobs")
	}
	serviceRequest, commandArgs, err := extractServiceRequest(os.Args[2:])
	if err != nil {
		fatal(err.Error())
	}
	serviceRequest.action = os.Args[1]
	if os.Args[1] == "maintenance" || os.Args[1] == "maintenance-status" {
		executeMaintenanceCommand(os.Args[1], serviceRequest, commandArgs)
		return
	}
	if os.Args[1] == "publish-host" {
		executePublishHost(serviceRequest, commandArgs)
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
					if err := repairWithOptions(resolved, launchctl, disablePairing); err != nil {
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
			if err := repairWithOptions(resolved, launchctl, disablePairing); err != nil {
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
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "drain":
		requestID := requestIDArg(commandArgs)
		operation, operationErr := runLifecycleOperation(resolved, "drain", requestID, func() error { return drain(resolved) })
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
		operation, operationErr := runLifecycleOperation(resolved, "remove", requestID, func() error { return uninstall(resolved, purge) })
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

type lifecycleUpdateArgs struct {
	sourceApp               string
	targetReleaseID         string
	requestID               string
	artifactSHA256          string
	artifactSize            uint64
	preparationFailureStdin bool
}

type updatePreparationFailureDescriptor struct {
	Version   int    `json:"version"`
	Stage     string `json:"stage"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type updatePreparationError struct {
	descriptor updatePreparationFailureDescriptor
}

func (failure updatePreparationError) Error() string { return failure.descriptor.Message }

func parseUpdateArgs(args []string) lifecycleUpdateArgs {
	var result lifecycleUpdateArgs
	for len(args) > 0 {
		switch args[0] {
		case "--preparation-failure-stdin":
			result.preparationFailureStdin = true
			args = args[1:]
			continue
		case "--source-app":
			if len(args) < 2 {
				fatal("--source-app requires a value")
			}
			result.sourceApp = args[1]
		case "--target-release-id":
			if len(args) < 2 {
				fatal("--target-release-id requires a value")
			}
			result.targetReleaseID = args[1]
		case "--request-id":
			if len(args) < 2 {
				fatal("--request-id requires a value")
			}
			result.requestID = args[1]
		case "--artifact-sha256":
			if len(args) < 2 {
				fatal("--artifact-sha256 requires a value")
			}
			result.artifactSHA256 = strings.ToLower(args[1])
		case "--artifact-size":
			if len(args) < 2 {
				fatal("--artifact-size requires a value")
			}
			parsed, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil || parsed == 0 {
				fatal("update artifact size is invalid")
			}
			result.artifactSize = parsed
		default:
			fatal("unknown update argument")
		}
		args = args[2:]
	}
	if result.targetReleaseID == "" || !operationIDPattern.MatchString(result.targetReleaseID) || result.requestID == "" || !operationIDPattern.MatchString(result.requestID) {
		fatal("update requires a valid --target-release-id and --request-id")
	}
	if result.preparationFailureStdin == (result.sourceApp != "") {
		fatal("update requires exactly one of --source-app or --preparation-failure-stdin")
	}
	if result.sourceApp != "" && !filepath.IsAbs(result.sourceApp) {
		fatal("update requires an absolute --source-app")
	}
	if (result.artifactSHA256 == "") != (result.artifactSize == 0) {
		fatal("update artifact SHA-256 and size must be supplied together")
	}
	if result.artifactSHA256 != "" {
		decoded, err := hex.DecodeString(result.artifactSHA256)
		if err != nil || len(decoded) != 32 {
			fatal("update artifact SHA-256 is invalid")
		}
	}
	if result.preparationFailureStdin && result.artifactSHA256 == "" {
		fatal("update preparation failure requires artifact SHA-256 and size")
	}
	return result
}

func decodeUpdatePreparationFailure(input io.Reader) (updatePreparationFailureDescriptor, error) {
	var result updatePreparationFailureDescriptor
	decoder := json.NewDecoder(io.LimitReader(input, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, errors.New("update preparation failure is invalid")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return result, errors.New("update preparation failure has trailing data")
	}
	if result.Version != 1 || result.Stage != "target_selection" && result.Stage != "download" && result.Stage != "preparation" ||
		!operationIDPattern.MatchString(result.Code) || strings.TrimSpace(result.Message) == "" || len(result.Message) > 512 || strings.ContainsAny(result.Message, "\r\n") {
		return result, errors.New("update preparation failure fields are invalid")
	}
	return result, nil
}

func hostBundleReleaseID(app string, staged bool) (string, error) {
	return bundle.HostReleaseID(app, staged)
}

func updateApplication(value paths, sourceApp, targetReleaseID, requestID string) error {
	return updateApplicationWith(value, sourceApp, targetReleaseID, requestID, updateApplicationActions{
		releaseID: hostBundleReleaseID,
		drain:     drain,
		replace:   atomicReplaceApp,
		repair:    repair,
		startVM:   ensureVMStarted,
		remove:    os.RemoveAll,
	})
}

type updateApplicationActions struct {
	releaseID func(string, bool) (string, error)
	drain     func(paths) error
	replace   func(string, string) error
	repair    func(paths) error
	startVM   func(paths, string) error
	remove    func(string) error
}

func updateApplicationWith(value paths, sourceApp, targetReleaseID, requestID string, actions updateApplicationActions) error {
	if filepath.Dir(sourceApp) != filepath.Dir(value.app) || sourceApp == value.app {
		return errors.New("prepared update must be a hidden sibling of the installed Host app")
	}
	targetRelease, targetErr := actions.releaseID(value.app, false)
	sourceRelease, sourceErr := actions.releaseID(sourceApp, true)
	if targetErr == nil && targetRelease == targetReleaseID {
		if err := persistUpdateStage(value, requestID, targetReleaseID, "recovery"); err != nil {
			return err
		}
		if err := actions.repair(value); err != nil {
			return fmt.Errorf("restore updated Host service: %w", err)
		}
		if err := actions.startVM(value, requestID+"-restore"); err != nil {
			return fmt.Errorf("restore updated VM: %w", err)
		}
		if err := actions.remove(sourceApp); err != nil {
			return errors.New("retired Host bundle could not be removed")
		}
		return persistUpdateStage(value, requestID, targetReleaseID, "complete")
	}
	if targetErr != nil {
		return fmt.Errorf("current Host release is unreadable: %w", targetErr)
	}
	if sourceErr != nil || sourceRelease != targetReleaseID {
		return errors.New("prepared Host release does not match the selected target")
	}
	if err := persistUpdateStage(value, requestID, targetReleaseID, "stop"); err != nil {
		return err
	}
	if err := actions.drain(value); err != nil {
		return fmt.Errorf("stop current Host release: %w", err)
	}
	if err := persistUpdateStage(value, requestID, targetReleaseID, "replace"); err != nil {
		return err
	}
	if err := actions.replace(sourceApp, value.app); err != nil {
		return err
	}
	if err := persistUpdateStage(value, requestID, targetReleaseID, "recovery"); err != nil {
		return err
	}
	if err := actions.repair(value); err != nil {
		return fmt.Errorf("restore updated Host service: %w", err)
	}
	if err := actions.startVM(value, requestID+"-restore"); err != nil {
		return fmt.Errorf("restore updated VM: %w", err)
	}
	if err := actions.remove(sourceApp); err != nil {
		return errors.New("retired Host bundle could not be removed")
	}
	return persistUpdateStage(value, requestID, targetReleaseID, "complete")
}

func atomicReplaceApp(source, target string) error {
	if !directory(source) {
		return errors.New("staged application bundle is unavailable")
	}
	if directory(target) {
		if err := atomicSwapApp(source, target); err != nil {
			return fmt.Errorf("application bundle could not be atomically replaced: %w", err)
		}
		return nil
	}
	if err := os.Rename(source, target); err != nil {
		return fmt.Errorf("application bundle could not be installed: %w", err)
	}
	return nil
}

func lifecyclePaths(request serviceRequest) (paths, error) {
	executable, err := os.Executable()
	if err != nil {
		return paths{}, errors.New("helper location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return paths{}, errors.New("helper location is invalid")
	}
	return lifecyclePathsForRequest(request, executable)
}

func observationPathsForRequest(request serviceRequest) (paths, error) {
	executable, err := os.Executable()
	if err != nil {
		return paths{}, errors.New("helper location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return paths{}, errors.New("helper location is invalid")
	}
	config, err := resolveServiceConfig(request)
	if err != nil {
		return paths{}, err
	}
	account, err := lookupServiceAccount(config.User)
	if err != nil {
		return paths{}, errors.New("service user is unavailable")
	}
	if request.app != "" && os.Geteuid() != 0 {
		return paths{}, errors.New("--service-app is restricted to an administrator installer")
	}
	if request.sharedHost {
		request.app = sharedHostApp(account.HomeDir)
	}
	return observationPathsForConfig(account.HomeDir, executable, config, request.app)
}

func observationPathsForConfig(home, executable string, service serviceConfig, targetApp string) (paths, error) {
	if !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return paths{}, errors.New("user home directory is invalid")
	}
	helpers := filepath.Dir(executable)
	contents := filepath.Dir(helpers)
	if filepath.Base(helpers) != "Helpers" || filepath.Base(contents) != "Contents" {
		return paths{}, errors.New("helper is not inside an Agent VMM application bundle")
	}
	sourceApp := filepath.Dir(contents)
	if filepath.Base(sourceApp) != bundle.HostAppName {
		return paths{}, fmt.Errorf("application bundle must be %q", bundle.HostAppName)
	}
	app := sourceApp
	if targetApp != "" {
		if !filepath.IsAbs(targetApp) || filepath.Base(targetApp) != bundle.HostAppName {
			return paths{}, errors.New("service app must be an absolute Agent VMM Host.app path")
		}
		app = targetApp
	}
	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	agentPlist := filepath.Join(launchAgents, hostLabel+".plist")
	daemonPlist := filepath.Join("/Library/LaunchDaemons", hostLabel+".plist")
	hostPlist := agentPlist
	if service.Mode == serviceModeDaemon {
		hostPlist = daemonPlist
	}
	return paths{
		home: home, app: app, resources: filepath.Join(sourceApp, "Contents", "Resources"),
		hostPlist: hostPlist, agentPlist: agentPlist, daemonPlist: daemonPlist, service: service,
		applicationData:   filepath.Join(home, "Library", "Application Support", "AgentVMM"),
		logs:              filepath.Join(home, "Library", "Logs", "AgentVMM"),
		operationFile:     filepath.Join(home, "Library", "Application Support", "AgentVMM", "compute-node-operation.json"),
		serviceConfigFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	}, nil
}

func lifecyclePathsFor(home, executable string) (paths, error) {
	return lifecyclePathsForConfig(home, executable, serviceConfig{Version: 1, Mode: serviceModeAgent, User: "test", Group: "staff", UID: os.Getuid(), GID: os.Getgid(), ReleaseID: releaseID})
}

func lifecyclePathsForRequest(request serviceRequest, executable string) (paths, error) {
	config, err := resolveServiceConfig(request)
	if err != nil {
		return paths{}, err
	}
	account, err := lookupServiceAccount(config.User)
	if err != nil {
		return paths{}, errors.New("service user is unavailable")
	}
	if request.app != "" && os.Geteuid() != 0 {
		return paths{}, errors.New("--service-app is restricted to an administrator installer")
	}
	if request.sharedHost {
		request.app = sharedHostApp(account.HomeDir)
		if err := validateMaintenancePath(account.HomeDir, request.app); err != nil {
			return paths{}, err
		}
	}
	return lifecyclePathsForConfigWithApp(account.HomeDir, executable, config, request.app)
}

func lifecyclePathsForConfig(home, executable string, service serviceConfig) (paths, error) {
	return lifecyclePathsForConfigWithApp(home, executable, service, "")
}

func lifecyclePathsForConfigWithApp(home, executable string, service serviceConfig, targetApp string) (paths, error) {
	if !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return paths{}, errors.New("user home directory is invalid")
	}
	helpers := filepath.Dir(executable)
	if filepath.Base(helpers) != "Helpers" || filepath.Base(executable) != "agent-vmm-lifecycle" {
		return paths{}, errors.New("helper is not inside an Agent VMM application bundle")
	}
	contents := filepath.Dir(helpers)
	if filepath.Base(contents) != "Contents" {
		return paths{}, errors.New("application bundle layout is invalid")
	}
	sourceApp := filepath.Dir(contents)
	if filepath.Base(sourceApp) != bundle.HostAppName {
		return paths{}, fmt.Errorf("application bundle must be %q", bundle.HostAppName)
	}
	if err := bundle.ValidateHostApp(sourceApp); err != nil {
		return paths{}, fmt.Errorf("host application bundle is invalid: %w", err)
	}
	app := sourceApp
	if targetApp != "" {
		if !filepath.IsAbs(targetApp) || filepath.Base(targetApp) != bundle.HostAppName {
			return paths{}, errors.New("service app must be an absolute Agent VMM Host.app path")
		}
		if err := bundle.ValidateHostApp(targetApp); err != nil {
			return paths{}, fmt.Errorf("target host application bundle is invalid: %w", err)
		}
		app = targetApp
	}
	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	agentPlist := filepath.Join(launchAgents, hostLabel+".plist")
	daemonPlist := filepath.Join("/Library/LaunchDaemons", hostLabel+".plist")
	hostPlist := agentPlist
	if service.Mode == serviceModeDaemon {
		hostPlist = daemonPlist
	}
	return paths{
		home: home, app: app, resources: filepath.Join(sourceApp, "Contents", "Resources"),
		hostPlist:         hostPlist,
		agentPlist:        agentPlist,
		daemonPlist:       daemonPlist,
		service:           service,
		applicationData:   filepath.Join(home, "Library", "Application Support", "AgentVMM"),
		logs:              filepath.Join(home, "Library", "Logs", "AgentVMM"),
		operationFile:     filepath.Join(home, "Library", "Application Support", "AgentVMM", "compute-node-operation.json"),
		serviceConfigFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	}, nil
}

func readStatus(value paths) serviceStatus {
	return readStatusContext(context.Background(), value)
}

func readStatusContext(ctx context.Context, value paths, registrationIDs ...string) serviceStatus {
	result := serviceStatus{
		AppInstalled:     directory(value.app),
		ServiceApp:       value.app,
		ServiceUser:      value.service.User,
		ServiceUID:       value.service.UID,
		HostJob:          value.serviceDomain() + "/" + hostLabel,
		OperatorSocket:   filepath.Join(value.applicationData, "operator.sock"),
		HostInstalled:    regular(filepath.Join(value.app, filepath.FromSlash(bundle.HostExecutableRelativePath))) && regular(value.hostPlist),
		Operation:        readLifecycleOperation(value),
		HostStartFailure: readHostStartFailure(value),
	}
	if len(registrationIDs) != 0 && registrationIDs[0] != "" {
		result.RegistrationRead = "unreadable"
		result.RegistrationReason = "operator_unavailable"
	}
	if schema, err := store.ReadSchemaVersion(filepath.Join(value.applicationData, "host.sqlite")); err == nil {
		result.DataSchema = schema
	} else {
		result.DataSchemaIssue = err.Error()
	}
	result.Startup = readStartupEvidence(value, result.Operation)
	if service, err := launchctlServiceObserver(ctx, value.serviceDomain()+"/"+hostLabel); err == nil {
		result.HostObserved = true
		result.HostLoaded = service.Loaded
		result.HostRunning = service.Running
		result.HostPID = service.PID
		result.HostLastExitStatus = service.LastExitStatus
	} else if errors.Is(err, errLaunchctlServiceNotFound) {
		result.HostObserved = true
	} else {
		result.HostObservationIssue = err.Error()
	}
	// Local Host and exact connector observations never claim Salix admission or
	// product Node readiness. Those facts are owned by the server observation.
	result.ConnectorInstalled = result.HostInstalled
	if result.HostRunning {
		facts, _ := readOperatorFactsContext(ctx, filepath.Join(value.applicationData, "operator.sock"), registrationIDs...)
		result.HostReadable = facts.hostReadable
		result.HostHealthy = facts.hostHealthy
		result.RegistrationRead = facts.registrationRead
		result.RegistrationState = facts.registrationState
		result.RegistrationReason = facts.registrationReason
		result.ConnectorLoaded = facts.connectorConnected
		result.Snapshot = facts.snapshot
		if facts.snapshot != nil {
			result.HostHealth = facts.snapshot.GetHealth()
			result.HostResources = facts.snapshot.GetHostResources()
			result.GuestResources = facts.snapshot.GetGuestResources()
			if facts.snapshot.GetCollectedAt() != nil {
				result.SnapshotObservedAt = facts.snapshot.GetCollectedAt().AsTime().UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return result
}

func parseInstallArgs(action string, args []string) (bool, string, bool, string, string) {
	disablePairing := false
	requestID := ""
	operationStdin := false
	resumeOperationID := ""
	registrationID := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--disable-personal-mesh-pairing":
			disablePairing = true
		case "--operation-stdin":
			if action != "install" {
				fatal("--operation-stdin is only valid for install")
			}
			operationStdin = true
		case "--resume-operation":
			if action != "install" || index+1 >= len(args) {
				fatal("--resume-operation requires an install operation id")
			}
			index++
			resumeOperationID = args[index]
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		case "--registration-id":
			if action != "repair" || index+1 >= len(args) {
				fatal("--registration-id is only valid for repair")
			}
			index++
			registrationID = args[index]
		default:
			fatal("install and repair accept --disable-personal-mesh-pairing, --request-id, repair --registration-id, and install --operation-stdin or --resume-operation")
		}
	}
	if registrationID != "" && !operationIDPattern.MatchString(registrationID) {
		fatal("--registration-id is invalid")
	}
	return disablePairing, requestID, operationStdin, resumeOperationID, registrationID
}

func parseRepairArgs(args []string) (bool, string) {
	disablePairing, requestID, _, _, _ := parseInstallArgs("repair", args)
	return disablePairing, requestID
}

func parseInstallOperationAckArgs(args []string) (string, error) {
	if len(args) != 2 || args[0] != "--operation-id" || !operationIDPattern.MatchString(args[1]) {
		return "", errors.New("install-operation-ack requires a valid --operation-id")
	}
	return args[1], nil
}

func requestIDArg(args []string) string {
	for index := 0; index < len(args); index++ {
		if args[index] == "--request-id" && index+1 < len(args) {
			return args[index+1]
		}
	}
	if len(args) != 0 {
		fatal("only --request-id is accepted")
	}
	return ""
}

func parseUninstallArgs(args []string) (bool, string) {
	purge := false
	requestID := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--purge":
			purge = true
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		default:
			fatal("uninstall accepts --purge and --request-id")
		}
	}
	return purge, requestID
}

func parseRegistrationStateArgs(args []string) (string, hostv1.RemoteRegistrationState, string) {
	registrationID := ""
	requestID := ""
	state := hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_UNSPECIFIED
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--registration-id":
			if index+1 >= len(args) {
				fatal("--registration-id requires a value")
			}
			index++
			registrationID = args[index]
		case "--state":
			if index+1 >= len(args) {
				fatal("--state requires a value")
			}
			index++
			switch args[index] {
			case "enabled":
				state = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED
			case "draining":
				state = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING
			default:
				fatal("--state must be enabled or draining")
			}
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		default:
			fatal("registration-state accepts --registration-id, --state, and --request-id")
		}
	}
	if !operationIDPattern.MatchString(registrationID) || state == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_UNSPECIFIED {
		fatal("registration-state requires a valid --registration-id and --state")
	}
	return registrationID, state, requestID
}

func setRemoteRegistrationState(ctx context.Context, value paths, registrationID string, state hostv1.RemoteRegistrationState, requestID string) error {
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable for registration state change")
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	return setRemoteRegistrationStateWith(ctx, registrationID, state, requestID, client.GetRemoteRegistration, client.SetRemoteRegistrationState)
}

func waitRemoteRegistrationConnected(value paths, registrationID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable for registration connection observation")
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	return waitRemoteRegistrationConnectedWith(ctx, registrationID, client.GetRemoteRegistration, func(ctx context.Context) error {
		timer := time.NewTimer(250 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	})
}

func waitRemoteRegistrationConnectedWith(ctx context.Context, registrationID string, get func(context.Context, *hostv1.GetRemoteRegistrationRequest, ...grpc.CallOption) (*hostv1.RemoteRegistrationResponse, error), sleep func(context.Context) error) error {
	for {
		response, readErr := get(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
		if readErr == nil && response.GetRegistration().GetId() == registrationID && response.GetRegistration().GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("managed registration did not reconnect before the repair deadline: %w", context.DeadlineExceeded)
		}
		if sleep(ctx) != nil {
			return fmt.Errorf("managed registration did not reconnect before the repair deadline: %w", context.DeadlineExceeded)
		}
	}
}

func setRemoteRegistrationStateWith(
	ctx context.Context,
	registrationID string,
	state hostv1.RemoteRegistrationState,
	requestID string,
	get func(context.Context, *hostv1.GetRemoteRegistrationRequest, ...grpc.CallOption) (*hostv1.RemoteRegistrationResponse, error),
	set func(context.Context, *hostv1.SetRemoteRegistrationStateRequest, ...grpc.CallOption) (*hostv1.RemoteRegistrationResponse, error),
) error {
	current, err := get(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
	if err != nil {
		return fmt.Errorf("read managed registration: %w", err)
	}
	if current.GetRegistration().GetId() != registrationID {
		return errors.New("managed registration identity changed")
	}
	if current.GetRegistration().GetState() == state && !(state == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED && current.GetRegistration().GetConnectionState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil
	}
	_, err = set(ctx, &hostv1.SetRemoteRegistrationStateRequest{
		RegistrationId:   registrationID,
		ExpectedRevision: current.GetRegistration().GetRevision(),
		RequestId:        requestID,
		State:            state,
	})
	if err != nil {
		return fmt.Errorf("set managed registration state: %w", err)
	}
	return nil
}

func runLifecycleOperation(value paths, kind, requestID string, action func() error) (lifecycleOperation, error) {
	return runLifecycleOperationWithFailedRetry(value, kind, requestID, "", "", "", 0, false, action)
}

// Managed install retries must retain the server operation id as their request
// identity. The durable plan and its acknowledgement capability fence replay;
// unrelated lifecycle operations continue to require a new request id after a
// recorded failure.
func runManagedLifecycleOperation(value paths, kind, requestID string, action func() error) (lifecycleOperation, error) {
	if kind != "install-managed" || requestID == "" {
		return lifecycleOperation{RequestID: requestID, Kind: kind, Outcome: "unknown", SecretDisposition: secretDispositionRetain}, errors.New("managed install retry requires its stable request identity")
	}
	return runLifecycleOperationWithFailedRetry(value, kind, requestID, "", "", "", 0, true, action)
}

func runRegistrationStateOperation(value paths, kind, requestID string, action func() error) (lifecycleOperation, error) {
	if !strings.HasPrefix(kind, "registration-") || requestID == "" {
		return lifecycleOperation{RequestID: requestID, Kind: kind, Outcome: "unknown"}, errors.New("registration state retry requires its stable request identity")
	}
	return runLifecycleOperationWithFailedRetry(value, kind, requestID, "", "", "", 0, true, action)
}

func runRepairLifecycleOperation(value paths, requestID, registrationID string, action func() error) (lifecycleOperation, error) {
	return runLifecycleOperationWithFailedRetry(value, "repair", requestID, registrationID, "", "", 0, true, action)
}

func runUpdateLifecycleOperation(value paths, requestID, targetReleaseID string, action func() error) (lifecycleOperation, error) {
	return runUpdateLifecycleOperationForArtifact(value, requestID, targetReleaseID, "", 0, action)
}

func runUpdateLifecycleOperationForArtifact(value paths, requestID, targetReleaseID, artifactSHA256 string, artifactSize uint64, action func() error) (lifecycleOperation, error) {
	if targetReleaseID == "" {
		return lifecycleOperation{RequestID: requestID, Kind: "update", Outcome: "unknown"}, errors.New("update target release is required")
	}
	return runLifecycleOperationWithFailedRetry(value, "update", requestID, "", targetReleaseID, artifactSHA256, artifactSize, true, action)
}

func persistUpdateStage(value paths, requestID, targetReleaseID, stage string) error {
	operation := readLifecycleOperation(value)
	if operation == nil || operation.RequestID != requestID || operation.Kind != "update" || operation.TargetReleaseID != targetReleaseID {
		return errors.New("lifecycle operation is not owned by this request")
	}
	operation.Stage = stage
	operation.UpdatedAt = time.Now().UnixMilli()
	return persistOperationOwned(value, *operation, lifecycleOperationOwner(value))
}

func persistRepairStage(value paths, requestID, registrationID, stage string) error {
	operation := readLifecycleOperation(value)
	if operation == nil || operation.RequestID != requestID || operation.Kind != "repair" || operation.TargetRegistrationID != registrationID {
		return errors.New("repair operation target is not owned by this request")
	}
	operation.Stage = stage
	operation.UpdatedAt = time.Now().UnixMilli()
	return persistOperationOwned(value, *operation, lifecycleOperationOwner(value))
}

func runLifecycleOperationWithFailedRetry(value paths, kind, requestID, targetRegistrationID, targetReleaseID, targetArtifactSHA256 string, targetArtifactSize uint64, retryFailed bool, action func() error) (lifecycleOperation, error) {
	maintenanceRelease, err := acquireMaintenanceMutation(value, kind, requestID)
	if err != nil {
		return lifecycleOperation{RequestID: requestID, Kind: kind, Outcome: "failed", FailureCode: "local_maintenance_required", FailureMessage: err.Error()}, err
	}
	defer maintenanceRelease()
	owner := lifecycleOperationOwner(value)
	release, err := acquireOperationLockOwned(value, owner)
	if err != nil {
		operation := lifecycleOperation{RequestID: requestID, Kind: kind, Outcome: "unknown"}
		if kind == "install-managed" {
			operation.SecretDisposition = secretDispositionRetain
		}
		return operation, err
	}
	defer release()
	if requestID == "" {
		requestID = newOperationID("req")
	}
	var operation lifecycleOperation
	previousFailureStage := ""
	if previous := readLifecycleOperation(value); previous != nil && previous.RequestID == requestID {
		if previous.Kind != kind || previous.TargetRegistrationID != targetRegistrationID || previous.TargetReleaseID != targetReleaseID ||
			previous.TargetArtifactSHA256 != targetArtifactSHA256 || previous.TargetArtifactSize != targetArtifactSize {
			return *previous, errors.New("operation request identity was reused for a different target")
		}
		if kind == "install-managed" && (previous.Kind != kind || previous.OperationID != requestID) {
			previous.SecretDisposition = secretDispositionRetain
			return *previous, errors.New("managed install operation identity does not match its durable request")
		}
		if previous.Outcome == "succeeded" {
			return *previous, nil
		}
		if previous.Outcome == "failed" {
			previousFailureStage = previous.FailureStage
			if !retryFailed || previous.Kind != kind {
				return *previous, errors.New("operation already failed; use a new request id after reviewing status")
			}
			if !previous.Retryable && !(kind == "registration-revoke" && strings.Contains(previous.FailureMessage, "registration revision conflict")) {
				return *previous, errors.New("operation failure requires operator action")
			}
			if previous.Attempt >= boundedOperationMaxAttempts {
				previous.Retryable = false
				previous.SecretDisposition = secretDispositionDestroyTerminal
				previous.FailureCode = "retry_limit_reached"
				previous.FailureMessage = "Operation reached its retry limit. Review the failed stage before taking explicit action."
				previous.UpdatedAt = time.Now().UnixMilli()
				if err := persistOperationOwned(value, *previous, owner); err != nil {
					previous.SecretDisposition = secretDispositionRetain
					return *previous, err
				}
				return *previous, errors.New("managed install retry limit reached")
			}
			operation = *previous
			operation.Outcome = "pending"
			operation.Attempt++
			operation.FailureStage = ""
			operation.FailureCode = ""
			operation.FailureMessage = ""
			operation.Retryable = false
			operation.SecretDisposition = secretDispositionRetain
			operation.UpdatedAt = time.Now().UnixMilli()
			if err := persistOperationOwned(value, operation, owner); err != nil {
				return operation, err
			}
		} else if retryFailed && previous.Kind == kind {
			previousFailureStage = previous.FailureStage
			if previous.Attempt >= boundedOperationMaxAttempts {
				previous.Outcome = "failed"
				previous.Retryable = false
				previous.SecretDisposition = secretDispositionDestroyTerminal
				previous.FailureStage = kind
				previous.FailureCode = "retry_limit_reached"
				previous.FailureMessage = "Operation outcome remained unknown through its retry limit. Review local state before taking explicit action."
				previous.UpdatedAt = time.Now().UnixMilli()
				if err := persistOperationOwned(value, *previous, owner); err != nil {
					previous.SecretDisposition = secretDispositionRetain
					return *previous, err
				}
				return *previous, errors.New("operation retry limit reached")
			}
			operation = *previous
			operation.Outcome = "pending"
			operation.Attempt++
			operation.FailureStage = ""
			operation.FailureCode = ""
			operation.FailureMessage = ""
			operation.Retryable = false
			operation.SecretDisposition = secretDispositionRetain
			operation.UpdatedAt = time.Now().UnixMilli()
			if err := persistOperationOwned(value, operation, owner); err != nil {
				return operation, err
			}
		} else {
			observed := readStatus(value)
			if operationConfirmed(previous.Kind, observed) {
				previous.Outcome = "succeeded"
				previous.UpdatedAt = time.Now().UnixMilli()
				if err := persistOperationOwned(value, *previous, owner); err != nil {
					return *previous, err
				}
				return *previous, nil
			}
			previous.Outcome = "unknown"
			previous.UpdatedAt = time.Now().UnixMilli()
			_ = persistOperationOwned(value, *previous, owner)
			return *previous, errors.New("operation outcome is unknown; read status and use a new request id only after review")
		}
	} else {
		now := time.Now().UnixMilli()
		operationID := newOperationID("op")
		if kind == "install-managed" {
			operationID = requestID
		}
		operation = lifecycleOperation{
			OperationID: operationID, RequestID: requestID, Kind: kind,
			TargetRegistrationID: targetRegistrationID,
			TargetReleaseID:      targetReleaseID,
			TargetArtifactSHA256: targetArtifactSHA256,
			TargetArtifactSize:   targetArtifactSize,
			Outcome:              "pending",
			Attempt:              1, StartedAt: now, UpdatedAt: now,
		}
		if kind == "install-managed" {
			operation.SecretDisposition = secretDispositionRetain
		}
		if err := persistOperationOwned(value, operation, owner); err != nil {
			return operation, err
		}
	}
	if err := action(); err != nil {
		if current := readLifecycleOperation(value); current != nil && current.RequestID == operation.RequestID && current.Kind == operation.Kind && current.TargetRegistrationID == operation.TargetRegistrationID && current.TargetReleaseID == operation.TargetReleaseID && current.TargetArtifactSHA256 == operation.TargetArtifactSHA256 && current.TargetArtifactSize == operation.TargetArtifactSize {
			operation = *current
		}
		operation.Outcome = "failed"
		operation.FailureStage, operation.FailureCode, operation.FailureMessage, operation.Retryable = lifecycleFailure(kind, err)
		if (kind == "repair" || kind == "update") && operation.Stage != "" {
			operation.FailureStage = operation.Stage
		}
		var preparationFailure updatePreparationError
		if errors.As(err, &preparationFailure) {
			operation.FailureStage = preparationFailure.descriptor.Stage
			operation.FailureCode = preparationFailure.descriptor.Code
			operation.FailureMessage = preparationFailure.descriptor.Message
			operation.Retryable = preparationFailure.descriptor.Retryable
		} else if kind == "update" && (operation.Stage == "stop" || operation.Stage == "replace" || operation.Stage == "recovery" || operation.Stage == "complete") {
			// Once an update has entered the stopped/replacement path, the exact
			// request and selected release are the only safe forward-repair key.
			// Expected administrator handoffs advance through distinct stages.
			// Each stage receives its own bounded retry budget.
			if operation.Stage != previousFailureStage {
				operation.Attempt = 1
			}
			operation.FailureCode = "update_forward_repair_required"
			operation.Retryable = true
		}
		if kind == "update" && operation.Retryable && operation.Attempt >= boundedOperationMaxAttempts {
			operation.Retryable = false
			operation.FailureCode = "update_retry_limit_reached"
			operation.FailureMessage = "The update reached its retry limit. Review the failed stage before taking explicit action."
		}
		if kind == "install-managed" {
			operation.SecretDisposition = managedInstallSecretDisposition(err)
			// A durable non-retryable result is the local half of the
			// action-required transition. It must be reported once and then
			// acknowledged by the Server before the retained plan is removed.
			// Persist failure below revokes this disposition and keeps the plan.
			if !operation.Retryable {
				operation.SecretDisposition = secretDispositionDestroyTerminal
			}
		}
		operation.UpdatedAt = time.Now().UnixMilli()
		if persistErr := persistOperationOwned(value, operation, owner); persistErr != nil {
			operation.SecretDisposition = secretDispositionRetain
			return operation, errors.Join(err, persistErr)
		}
		return operation, err
	}
	if current := readLifecycleOperation(value); current != nil && current.RequestID == operation.RequestID && current.Kind == operation.Kind && current.TargetRegistrationID == operation.TargetRegistrationID && current.TargetReleaseID == operation.TargetReleaseID && current.TargetArtifactSHA256 == operation.TargetArtifactSHA256 && current.TargetArtifactSize == operation.TargetArtifactSize {
		operation.Stage = current.Stage
	}
	operation.Outcome = "succeeded"
	if kind == "install-managed" {
		operation.SecretDisposition = secretDispositionDestroyCompleted
	}
	operation.UpdatedAt = time.Now().UnixMilli()
	if err := persistOperationOwned(value, operation, owner); err != nil {
		return operation, err
	}
	return operation, nil
}

func lifecycleFailure(kind string, err error) (string, string, string, bool) {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "LOCAL_DISPOSED" {
			return kind, "local_disposed", "The local operator deleted this registration. Start a new authorized setup to connect again.", false
		}
	}
	message := strings.TrimSpace(strings.ReplaceAll(err.Error(), "\n", " "))
	if len(message) > 512 {
		message = message[:512]
	}
	stage := kind
	if strings.HasPrefix(message, "lifecycle ") {
		if end := strings.IndexByte(strings.TrimPrefix(message, "lifecycle "), ':'); end > 0 {
			stage = strings.TrimSpace(strings.TrimPrefix(message, "lifecycle ")[:end])
		}
	}
	retryClass := kind == "install-managed" || kind == "repair" || kind == "update" || strings.HasPrefix(kind, "registration-")
	if !retryClass {
		return stage, "operation_failed", message, false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrCoordinatorNotReady) {
		return stage, "transient_timeout", message, true
	}
	if kind == "registration-revoke" && registrationRevisionConflict(err) {
		return stage, "registration_revision_changed", "The registration changed. Continue removal of the same registration.", true
	}
	switch status.Code(err) {
	case codes.Unavailable:
		return stage, "transient_unavailable", message, true
	case codes.DeadlineExceeded:
		return stage, "transient_timeout", message, true
	}
	for code := 500; code <= 599; code++ {
		if strings.Contains(message, fmt.Sprintf("HTTP %d", code)) {
			return stage, "upstream_unavailable", message, true
		}
	}
	if strings.Contains(message, "operation handoff exchange failed") ||
		strings.Contains(message, "operation handoff acknowledgement failed") {
		return stage, "upstream_unavailable", message, true
	}
	if kind == "install-managed" {
		return stage, "install_failed", message, false
	}
	return stage, "operation_failed", message, false
}

func acquireOperationLock(value paths) (func(), error) {
	return acquireOperationLockOwned(value, nil)
}

func acquireOperationLockOwned(value paths, owner *operationFileOwner) (func(), error) {
	directory, err := openApplicationDataDirectory(value, owner)
	if err != nil {
		return func() {}, fmt.Errorf("assign lifecycle state directory ownership: %w", err)
	}
	lockName := filepath.Base(value.operationFile) + ".lock"
	file, err := openStateFileAt(directory, lockName, unix.O_RDWR|unix.O_CREAT|unix.O_NONBLOCK, 0o600, owner)
	if err != nil {
		directory.Close()
		return func() {}, fmt.Errorf("open lifecycle operation lock: %w", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			if handoffErr := handoffExistingStateFileAt(directory, filepath.Base(value.operationFile), owner); handoffErr != nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
				_ = directory.Close()
				return func() {}, fmt.Errorf("assign lifecycle operation ownership: %w", handoffErr)
			}
			_ = directory.Close()
			if truncateErr := file.Truncate(0); truncateErr != nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
				return func() {}, truncateErr
			}
			if _, seekErr := file.Seek(0, 0); seekErr != nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
				return func() {}, seekErr
			}
			if _, writeErr := file.WriteString(fmt.Sprintf("%d\n", os.Getpid())); writeErr != nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
				return func() {}, writeErr
			}
			return func() {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			_ = directory.Close()
			return func() {}, err
		}
		if time.Now().After(deadline) {
			_ = file.Close()
			_ = directory.Close()
			return func() {}, errors.New("another Agent VMM lifecycle operation is still running")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func operationConfirmed(kind string, status serviceStatus) bool {
	switch kind {
	case "install", "repair":
		return status.HostInstalled && status.HostLoaded && status.HostHealthy
	case "install-managed":
		// The generic operation record does not persist the registration
		// identity needed to prove a managed attach after a process crash.
		return false
	case "drain":
		return !status.HostLoaded
	case "remove":
		return !status.AppInstalled && !status.HostInstalled
	default:
		return false
	}
}

func newOperationID(prefix string) string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(value[:])
}

func readOperation(path string) *lifecycleOperation {
	data, err := readStateFileNoFollow(path, 1<<20)
	if err != nil {
		return nil
	}
	var operation lifecycleOperation
	if json.Unmarshal(data, &operation) != nil || operation.OperationID == "" || operation.RequestID == "" {
		return nil
	}
	return &operation
}

func persistOperation(value paths, operation lifecycleOperation) error {
	return persistOperationOwned(value, operation, nil)
}

func persistOperationOwned(value paths, operation lifecycleOperation, owner *operationFileOwner) error {
	data, err := json.Marshal(operation)
	if err != nil {
		return err
	}
	directory, err := openApplicationDataDirectory(value, owner)
	if err != nil {
		return fmt.Errorf("open lifecycle state directory: %w", err)
	}
	defer directory.Close()
	if err := atomicWriteStateFileAt(directory, filepath.Base(value.operationFile), data, 0o600, owner); err != nil {
		return fmt.Errorf("persist lifecycle operation: %w", err)
	}
	return nil
}

func readLifecycleOperation(value paths) *lifecycleOperation {
	data, err := readApplicationDataState(value, filepath.Base(value.operationFile), 1<<20)
	if err != nil {
		return nil
	}
	var operation lifecycleOperation
	if json.Unmarshal(data, &operation) != nil || operation.OperationID == "" || operation.RequestID == "" {
		return nil
	}
	return &operation
}

// versionReport is the `version --json` output. supportedSchema lists the
// existing Host database schemas that this build opens.
func versionReport() map[string]any {
	return map[string]any{"component": "agent-vmm-host", "version": releaseID, "release_id": releaseID, "supportedSchema": supportedSchema()}
}

func readHostStartFailure(value paths) *store.StartFailure {
	data, err := readApplicationDataState(value, store.StartFailureFile, 4<<10)
	if err != nil {
		return nil
	}
	var failure store.StartFailure
	if json.Unmarshal(data, &failure) != nil || failure.Code == "" {
		return nil
	}
	return &failure
}

func readLifecycleOperationByID(value paths, operationID string) (*lifecycleOperation, error) {
	operation := readLifecycleOperation(value)
	if operation == nil || operation.OperationID != operationID {
		return nil, errors.New("lifecycle operation was not found")
	}
	return operation, nil
}

func writeOperation(operation lifecycleOperation) error {
	return json.NewEncoder(os.Stdout).Encode(operation)
}

type operatorFacts struct {
	hostReadable       bool
	hostHealthy        bool
	connectorConnected bool
	registrationRead   string
	registrationState  string
	registrationReason string
	snapshot           *hostv1.GetSystemSnapshotResponse
}

func parseStatusArgs(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	if len(args) == 2 && args[0] == "--registration-id" && operationIDPattern.MatchString(args[1]) {
		return args[1], nil
	}
	return "", errors.New("status accepts --registration-id ID")
}

func parseLifecycleOperationArgs(args []string) (string, error) {
	if len(args) == 2 && args[0] == "--operation-id" && operationIDPattern.MatchString(args[1]) {
		return args[1], nil
	}
	return "", errors.New("operation requires --operation-id ID")
}

func readOperatorFacts(socket string) (operatorFacts, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return readOperatorFactsContext(ctx, socket)
}

func readOperatorFactsContext(ctx context.Context, socket string, registrationIDs ...string) (operatorFacts, error) {
	connection, err := operatorConnection(ctx, socket)
	if err != nil {
		return operatorFacts{registrationRead: "unreadable", registrationReason: "operator_unavailable"}, err
	}
	defer connection.Close()

	snapshot, err := hostv1.NewOperatorServiceClient(connection).GetSystemHealthSummary(ctx, &hostv1.GetSystemHealthSummaryRequest{})
	if err != nil {
		return operatorFacts{registrationRead: "unreadable", registrationReason: "operator_unavailable"}, err
	}
	facts := operatorFacts{
		hostReadable: true,
		snapshot:     snapshot,
		hostHealthy: snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING &&
			snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY,
	}
	if len(registrationIDs) != 0 && registrationIDs[0] != "" {
		remote, err := hostv1.NewOperatorServiceClient(connection).GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationIDs[0]})
		facts.registrationRead, facts.registrationReason = classifyRegistrationRead(err)
		if err != nil {
			return facts, err
		}
		switch remote.GetRegistration().GetState() {
		case hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED:
			facts.registrationState = "enabled"
		case hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING:
			facts.registrationState = "draining"
		case hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED:
			facts.registrationState = "revoked"
		}
		facts.connectorConnected = remote.GetRegistration().GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED
	}
	return facts, nil
}

func ensureVMStarted(value paths, requestID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCoordinatorInitTimeout)
	defer cancel()
	return ensureVMStartedContext(ctx, value, requestID)
}

func ensureVMStartedContext(ctx context.Context, value paths, requestID string) error {
	startedAt := time.Now()
	reportLifecycleProgress(lifecycleProgress{Event: "operator_connect_started", RequestID: requestID})
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable")
	}
	defer connection.Close()
	operator := hostv1.NewOperatorServiceClient(connection)
	connected := false
	for {
		vm, err := operator.GetVM(ctx, &hostv1.GetVMRequest{})
		if err != nil {
			if ctx.Err() != nil {
				return errors.New("Agent VMM VM status could not be read")
			}
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if !connected {
			reportLifecycleProgress(lifecycleProgress{Event: "operator_connected", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()})
			connected = true
		}
		if vm.GetVm().GetState() != hostv1.VMState_VM_STATE_RUNNING {
			reportLifecycleProgress(lifecycleProgress{Event: "vm_start_requested", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds(), VMState: vm.GetVm().GetState().String()})
			if err := startVMWithProgress(ctx, operator, requestID, startedAt); err != nil {
				return fmt.Errorf("Agent VMM VM could not be started: %w", err)
			}
			reportLifecycleProgress(lifecycleProgress{Event: "vm_start_accepted", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()})
		}
		lastProgress := time.Time{}
		for {
			snapshot, snapshotErr := operator.GetSystemHealthSummary(ctx, &hostv1.GetSystemHealthSummaryRequest{})
			if snapshotErr == nil && snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING &&
				snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY {
				reportLifecycleProgress(lifecycleProgress{Event: "vm_healthy", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds(), VMState: snapshot.GetVm().GetState().String(), HealthState: snapshot.GetHealth().GetState().String()})
				return nil
			}
			if lastProgress.IsZero() || time.Since(lastProgress) >= 15*time.Second {
				progress := lifecycleProgress{Event: "health_wait", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()}
				if snapshotErr == nil {
					progress.VMState = snapshot.GetVm().GetState().String()
					progress.HealthState = snapshot.GetHealth().GetState().String()
				}
				reportLifecycleProgress(progress)
				lastProgress = time.Now()
			}
			if ctx.Err() != nil {
				return errors.New("Agent VMM VM did not become healthy")
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func startVMWithProgress(ctx context.Context, operator hostv1.OperatorServiceClient, requestID string, startedAt time.Time) error {
	summary, err := operator.GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
	if err != nil || summary.GetVm() == nil {
		return errors.New("Agent VMM whole-VM impact could not be acknowledged before VM start")
	}
	fence := vmMutationFence(summary.GetVm())
	result := make(chan error, 1)
	go func() {
		_, err := operator.StartVM(ctx, &hostv1.StartVMRequest{RequestId: requestID, Fence: fence})
		result <- err
	}()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ticker.C:
			reportLifecycleProgress(lifecycleProgress{Event: "vm_start_wait", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()})
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func operatorConnection(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
}

func repair(value paths) error { return repairWith(value, launchctl) }

func repairWithPolicy(value paths, disablePairing bool) error {
	return repairWithOptions(value, launchctl, disablePairing)
}

func repairWith(value paths, run func(...string) error) error {
	return repairWithOptions(value, run, false)
}

func repairWithOptions(value paths, run func(...string) error, disablePairing bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return repairWithOptionsContext(ctx, value, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	}, disablePairing)
}

func repairWithOptionsContext(ctx context.Context, value paths, run func(context.Context, ...string) error, disablePairing bool) error {
	return repairWithOptionsContextObserved(ctx, value, run, disablePairing, os.Geteuid(), launchctlServiceObservation)
}

func repairWithOptionsContextObserved(ctx context.Context, value paths, run func(context.Context, ...string) error, disablePairing bool, euid int,
	observe func(context.Context, string) (launchctlServiceFacts, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Omission preserves the administrator-owned restrictive policy across a
	// bundle replacement. An explicit disable request can only tighten it.
	disablePairing = disablePairing || value.service.DisablePairing || value.service.Mode == serviceModeDaemon
	if !directory(value.app) {
		return errors.New("Agent VMM application bundle is missing")
	}
	if err := validateAlternateDefinitionPermission(value, euid); err != nil {
		return err
	}
	if value.service.Mode == serviceModeDaemon && euid != 0 {
		desired, renderErr := renderServiceDefinition(value, disablePairing)
		if renderErr != nil || !serviceDefinitionCurrent(ctx, value, desired, disablePairing, run, observe) {
			return errors.New("installing or updating the Agent VMM daemon requires administrator access")
		}
		if err := prepareServiceStateDirectories(value, euid); err != nil {
			return err
		}
		if err := persistServiceConfig(value, disablePairing); err != nil {
			return errors.New("service configuration could not be persisted")
		}
		return nil
	}
	definitionMode := os.FileMode(0o700)
	if value.service.Mode == serviceModeDaemon {
		definitionMode = 0o755
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), definitionMode); err != nil {
		return errors.New("launchd service directory could not be created")
	}
	if err := prepareServiceStateDirectories(value, euid); err != nil {
		return err
	}
	desired, err := renderServiceDefinition(value, disablePairing)
	if err != nil {
		return err
	}
	if serviceDefinitionCurrent(ctx, value, desired, disablePairing, run, observe) {
		return nil
	}
	if err := stopHostServiceAt(ctx, run, value.serviceDomain()); err != nil {
		return fmt.Errorf("host service could not be stopped before reload: %w", err)
	}
	if err := stopHostServiceAt(ctx, run, value.alternateServiceDomain()); err != nil {
		return fmt.Errorf("alternate host service could not be stopped before reload: %w", err)
	}
	if err := atomicWrite(value.hostPlist, desired, map[serviceMode]os.FileMode{serviceModeAgent: 0o600, serviceModeDaemon: 0o644}[value.service.Mode]); err != nil {
		return errors.New("service definition could not be installed")
	}
	if value.service.Mode == serviceModeDaemon {
		_ = os.Remove(value.agentPlist)
	} else if os.Geteuid() == 0 {
		_ = os.Remove(value.daemonPlist)
	}
	if err := run(ctx, "bootstrap", value.serviceDomain(), value.hostPlist); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("host service load was interrupted: %w", contextErr)
		}
		return fmt.Errorf("host service could not be loaded: %w", err)
	}
	if err := run(ctx, "enable", value.serviceDomain()+"/"+hostLabel); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("host service enable was interrupted: %w", contextErr)
		}
		return fmt.Errorf("host service could not be enabled: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := persistServiceConfig(value, disablePairing); err != nil {
		return errors.New("service configuration could not be persisted")
	}
	return nil
}

func prepareServiceStateDirectories(value paths, euid int) error {
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		return errors.New("application data directory could not be created")
	}
	if err := os.MkdirAll(value.logs, 0o700); err != nil {
		return errors.New("log directory could not be created")
	}
	if euid == 0 {
		for _, directory := range []string{value.applicationData, value.logs} {
			if err := os.Chown(directory, value.service.UID, value.service.GID); err != nil {
				return errors.New("service user directory ownership could not be applied")
			}
		}
	}
	return nil
}

func addProgramArgument(plist, argument string) string {
	marker := "</array>"
	index := strings.Index(plist, marker)
	if index < 0 || strings.Contains(plist, "<string>"+argument+"</string>") {
		return plist
	}
	return plist[:index] + "\t\t<string>" + xmlEscape(argument) + "</string>\n\t" + plist[index:]
}

func uninstall(value paths, purge bool) error { return uninstallWith(value, purge, launchctl) }

func drain(value paths) error { return drainWith(value, launchctl) }

// launchctl bootout waits for the host and its supervised workers to terminate.
func drainWith(value paths, run func(...string) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return drainWithContext(ctx, value, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	})
}

func drainWithContext(ctx context.Context, value paths, run func(context.Context, ...string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.service.Mode == serviceModeDaemon && os.Geteuid() != 0 {
		err := run(ctx, "print", value.serviceDomain()+"/"+hostLabel)
		if errors.Is(err, errLaunchctlServiceNotFound) {
			return waitForStateDiskRelease(ctx, filepath.Join(value.applicationData, "instance", "state.raw"))
		}
		if err != nil {
			return fmt.Errorf("host service status could not be read before drain: %w", err)
		}
		if err := drainVMForUpdate(ctx, value, newOperationID("service-drain")); err != nil {
			return err
		}
		return errors.New("VM is stopped; administrator must run agent-vmm-service-executor stop --job host")
	}
	domains := []string{value.serviceDomain(), value.alternateServiceDomain()}
	loadedDomains := make(map[string]bool, len(domains))
	for _, domain := range domains {
		err := run(ctx, "print", domain+"/"+hostLabel)
		if err == nil {
			loadedDomains[domain] = true
			continue
		}
		if !errors.Is(err, errLaunchctlServiceNotFound) {
			return fmt.Errorf("host service status could not be read before drain: %w", err)
		}
	}
	pids := make([]int, 0, len(domains))
	for _, domain := range domains {
		if !loadedDomains[domain] {
			continue
		}
		pid, err := launchctlPID(ctx, domain+"/"+hostLabel)
		if err == nil && pid > 0 {
			pids = append(pids, pid)
		} else if err != nil {
			return fmt.Errorf("host process identity could not be read before drain: %w", err)
		}
	}
	needsVMStop := len(pids) > 0
	if !needsVMStop {
		owners, err := stateDiskOpenOwners(ctx, filepath.Join(value.applicationData, "instance", "state.raw"))
		if err != nil {
			return err
		}
		needsVMStop = len(owners) > 0
	}
	if needsVMStop {
		if err := drainVMForUpdate(ctx, value, newOperationID("update-drain")); err != nil {
			if len(pids) > 0 {
				return err
			}
		}
	}
	for _, domain := range domains {
		if err := stopHostServiceAt(ctx, run, domain); err != nil {
			return fmt.Errorf("host service could not be drained: %w", err)
		}
	}
	for _, pid := range pids {
		if err := waitForProcessExit(ctx, pid); err != nil {
			return fmt.Errorf("host process did not exit after launchd removed the service: %w", err)
		}
	}
	stateDisk := filepath.Join(value.applicationData, "instance", "state.raw")
	if err := waitForStateDiskRelease(ctx, stateDisk); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

var drainVMForUpdate = stopVMForUpdate

func stopVMForUpdate(ctx context.Context, value paths, requestID string) error {
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("running Agent VMM Host is not readable; refusing to replace its application bundle")
	}
	defer connection.Close()
	return stopVMForUpdateWith(ctx, hostv1.NewOperatorServiceClient(connection), requestID)
}

type updateVMClient interface {
	GetVM(context.Context, *hostv1.GetVMRequest, ...grpc.CallOption) (*hostv1.VMResponse, error)
	StopVM(context.Context, *hostv1.StopVMRequest, ...grpc.CallOption) (*hostv1.OperationResponse, error)
}

func stopVMForUpdateWith(ctx context.Context, client updateVMClient, requestID string) error {
	vm, err := client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		return errors.New("Agent VMM VM status could not be read before update")
	}
	if vm.GetVm().GetState() == hostv1.VMState_VM_STATE_STOPPED {
		return nil
	}
	fence := vmMutationFence(vm.GetVm())
	gracefulCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	_, gracefulErr := client.StopVM(gracefulCtx, &hostv1.StopVMRequest{RequestId: requestID + "-graceful", Fence: fence})
	cancel()
	if gracefulErr != nil {
		forceCtx, forceCancel := context.WithTimeout(ctx, 20*time.Second)
		latest, latestErr := client.GetVM(forceCtx, &hostv1.GetVMRequest{})
		if latestErr != nil || latest.GetVm() == nil {
			forceCancel()
			return errors.New("VM state could not be read before forced stop")
		}
		if latest.GetVm().GetBootId() != fence.GetBootId() {
			forceCancel()
			return errors.New("VM boot identity changed during update stop")
		}
		_, forceErr := client.StopVM(forceCtx, &hostv1.StopVMRequest{RequestId: requestID + "-force", Force: true, Fence: vmMutationFence(latest.GetVm())})
		forceCancel()
		if forceErr != nil {
			return fmt.Errorf("VM did not reach terminal state before update: graceful stop: %v; force stop: %w", gracefulErr, forceErr)
		}
	}
	vm, err = client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil || vm.GetVm().GetState() != hostv1.VMState_VM_STATE_STOPPED {
		return errors.New("VM stop returned without a terminal stopped state; refusing to bootout the Host")
	}
	return nil
}

func vmMutationFence(vm *hostv1.VM) *hostv1.VMMutationFence {
	return &hostv1.VMMutationFence{State: vm.GetState(), BootId: vm.GetBootId(), WholeVmImpactAcknowledged: true}
}

// A loaded job without a live PID has no Host process to wait for, whatever
// launchd state it reports, for example "spawn scheduled" in a crash loop.
var launchctlPID = func(ctx context.Context, target string) (int, error) {
	service, err := launchctlServiceObserver(ctx, target)
	return service.PID, err
}
var processRunning = func(pid int) bool {
	err := unix.Kill(pid, 0)
	return err == nil || errors.Is(err, unix.EPERM)
}

var stateDiskOpenOwners = fuserOpenOwners

func fuserOpenOwners(ctx context.Context, path string) ([]int, error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read VM state disk: %w", err)
	}
	command := exec.CommandContext(ctx, "/usr/bin/fuser", path)
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("inspect VM state disk owner: %w", err)
	}
	return parseFuserOwners(output), nil
}

func parseFuserOwners(output []byte) []int {
	owners := make([]int, 0)
	for _, field := range strings.Fields(string(output)) {
		pid, parseErr := strconv.Atoi(field)
		if parseErr == nil && pid > 0 {
			owners = append(owners, pid)
		}
	}
	return owners
}

func waitForStateDiskRelease(ctx context.Context, path string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		owners, err := stateDiskOpenOwners(waitCtx, path)
		if err != nil {
			return fmt.Errorf("VM state disk ownership could not be verified: %w", err)
		}
		if len(owners) == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("VM state disk is still open by process IDs %v; stop those exact owners before retrying the update: %w", owners, waitCtx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitForProcessExit(ctx context.Context, pid int) error {
	for processRunning(pid) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return nil
}

func uninstallWith(value paths, purge bool, run func(...string) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return uninstallWithContext(ctx, value, purge, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	})
}

func uninstallWithContext(ctx context.Context, value paths, purge bool, run func(context.Context, ...string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.service.Mode == serviceModeDaemon && os.Geteuid() != 0 {
		return errors.New("uninstalling the Agent VMM daemon requires administrator access")
	}
	if err := stopHostServiceAt(ctx, run, value.serviceDomain()); err != nil {
		return fmt.Errorf("host service could not be stopped before uninstall: %w", err)
	}
	if err := os.Remove(value.hostPlist); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("host service definition could not be removed")
	}
	_ = os.Remove(value.serviceConfigFile)
	if purge {
		if err := os.RemoveAll(value.applicationData); err != nil {
			return errors.New("application data could not be removed")
		}
		if err := os.RemoveAll(value.logs); err != nil {
			return errors.New("logs could not be removed")
		}
	}
	if err := os.RemoveAll(value.app); err != nil {
		return errors.New("application bundle could not be removed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".agent-vmm-*.plist")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func launchctl(arguments ...string) error {
	return launchctlContext(context.Background(), arguments...)
}

func launchctlContext(ctx context.Context, arguments ...string) error {
	command := exec.CommandContext(ctx, "/bin/launchctl", arguments...)
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	message := strings.TrimSpace(string(output))
	if contextErr := ctx.Err(); contextErr != nil {
		return fmt.Errorf("launchctl %s was interrupted: %w", arguments[0], contextErr)
	}
	if launchctlServiceAbsent(arguments[0], message) {
		return fmt.Errorf("%w: %s", errLaunchctlServiceNotFound, message)
	}
	if message == "" {
		return fmt.Errorf("launchctl %s failed: %w", arguments[0], err)
	}
	return fmt.Errorf("launchctl %s failed: %w: %s", arguments[0], err, message)
}

func launchctlServiceAbsent(action, message string) bool {
	return action == "print" && (strings.Contains(message, "Could not find service") ||
		strings.Contains(message, "Could not find domain") ||
		strings.Contains(message, "Domain does not support specified action"))
}

func stopHostService(ctx context.Context, run func(context.Context, ...string) error) error {
	return stopHostServiceAt(ctx, run, "")
}

func stopHostServiceAt(ctx context.Context, run func(context.Context, ...string) error, serviceDomain string) error {
	if serviceDomain == "" {
		serviceDomain = fmt.Sprintf("gui/%d", os.Getuid())
	}
	target := serviceDomain + "/" + hostLabel
	bootoutErr := run(ctx, "bootout", target)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run(ctx, "print", target); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if errors.Is(err, errLaunchctlServiceNotFound) {
			return nil
		}
		return fmt.Errorf("host service status could not be read: %w", err)
	}
	if bootoutErr != nil {
		return fmt.Errorf("launchctl bootout failed while the host service remains loaded: %w", bootoutErr)
	}
	return waitForHostServiceRemovalAt(ctx, run, serviceDomain)
}

// hostStopTimeout bounds a Host stop. launchd kills the Host after the 90 s
// ExitTimeOut of its service definition, so the stop completes within it.
const hostStopTimeout = 90 * time.Second

// waitForHostServiceRemovalAt waits within the caller deadline. The Host
// shutdown after SIGTERM stops the VM and the data worker first.
func waitForHostServiceRemovalAt(ctx context.Context, run func(context.Context, ...string) error, serviceDomain string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("host service did not finish stopping: %w", err)
		}
		if err := run(ctx, "print", serviceDomain+"/"+hostLabel); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("host service did not finish stopping: %w", ctx.Err())
			}
			if errors.Is(err, errLaunchctlServiceNotFound) {
				return nil
			}
			return fmt.Errorf("host service status could not be read: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("host service did not finish stopping: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func regular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
func directory(path string) bool { info, err := os.Stat(path); return err == nil && info.IsDir() }

func xmlEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

// finalizeManagedInstallOperation never infers secret ownership from an error
// or Retryable. Successful completion may clean up locally. A terminal failure
// retains the plan until the provisioner has persisted its report and received
// the Server acknowledgement for this operation.
func finalizeManagedInstallOperation(value paths, operationID string, operation lifecycleOperation, operationErr error) (lifecycleOperation, error) {
	retain := func(err error) (lifecycleOperation, error) {
		operation.SecretDisposition = secretDispositionRetain
		return operation, errors.Join(operationErr, err)
	}
	if operation.SecretDisposition == secretDispositionDestroyCompleted {
		completed, err := completedInstallExists(value, operationID)
		if err != nil {
			return retain(fmt.Errorf("verify managed install completion receipt: %w", err))
		}
		if !completed {
			return retain(errors.New("managed install completion receipt is not durable; retaining install plan"))
		}
	} else if operation.SecretDisposition == secretDispositionDestroyTerminal {
		recorded := readLifecycleOperation(value)
		if recorded == nil || recorded.OperationID != operationID || recorded.RequestID != operationID || recorded.Kind != "install-managed" ||
			recorded.Outcome != "failed" || recorded.SecretDisposition != secretDispositionDestroyTerminal {
			return retain(errors.New("managed install terminal result is not durable; retaining install plan"))
		}
		return operation, operationErr
	} else if operation.SecretDisposition == secretDispositionRetain || operationID == "" {
		return operation, operationErr
	} else {
		return retain(errors.New("managed install secret disposition is unavailable; retaining install plan"))
	}
	if err := cleanupInstallPlan(value, operationID); err != nil {
		return retain(fmt.Errorf("cleanup managed install plan: %w", err))
	}
	return operation, operationErr
}

func classifyRegistrationRead(err error) (string, string) {
	if err == nil {
		return "present", ""
	}
	if status.Code(err) == codes.NotFound {
		return "absent", "registration_not_found"
	}
	return "unreadable", "registration_unreadable"
}

func revokeRemoteRegistration(ctx context.Context, value paths, registrationID, requestID string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	return completeRemoteRevocation(ctx, client, registrationID, requestID)
}

func registrationRevisionConflict(err error) bool {
	if status.Code(err) != codes.Aborted {
		return false
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "REGISTRATION_REVISION_CONFLICT" {
			return true
		}
	}
	return false
}

func completeRemoteRevocation(ctx context.Context, client hostv1.OperatorServiceClient, registrationID, requestID string) error {
	var last error
	for range 3 {
		current, err := client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if current.GetRegistration().GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
			return nil
		}
		begun, err := client.BeginRemoteRevocation(ctx, &hostv1.BeginRemoteRevocationRequest{RegistrationId: registrationID, ExpectedRevision: current.GetRegistration().GetRevision(), RequestId: requestID})
		if err == nil {
			_, err = client.CompleteRemoteRevocation(ctx, &hostv1.CompleteRemoteRevocationRequest{RegistrationId: registrationID, ExpectedRevision: begun.GetRegistration().GetRevision(), RequestId: requestID})
		}
		if !registrationRevisionConflict(err) {
			return err
		}
		last = err
	}
	return last
}
