//go:build darwin || linux

package lifecycle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func runLifecycleOperation(value paths, kind, requestID string, action func() error) (lifecycleOperation, error) {
	return runLifecycleOperationWithFailedRetry(value, kind, requestID, "", "", "", 0, false, action)
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

	owner := lifecycleOperationOwner(value)
	release, err := acquireOperationLockOwned(value, owner)
	if err != nil {
		operation := lifecycleOperation{RequestID: requestID, Kind: kind, Outcome: "unknown"}
		if errors.Is(err, errMaintenanceOwnerBusy) {
			operation.FailureCode = "owner_busy"
			operation.FailureMessage = err.Error()
			operation.Retryable = true
		}
		return operation, err
	}
	defer release()
	if requestID == "" {
		requestID = newOperationID("req")
	}
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return lifecycleOperation{}, err
	}
	state, err := loadMaintenanceState(directory)
	if err != nil {
		directory.Close()
		return lifecycleOperation{}, err
	}

	if state.ActiveRequest != "" && state.ActiveRequest != requestID {
		previous, readErr := loadLifecycleReceipt(directory, state.ActiveRequest)
		if readErr != nil {
			directory.Close()
			return lifecycleOperation{}, readErr
		}
		if lifecycleExclusive(*previous) {
			directory.Close()
			return *previous, ownerBusyError{requestID: state.ActiveRequest}
		}
		// The OS lock proves the old executor exited. Remote waiting is target-local.
		state.ActiveRequest = ""
		if err := writeOwnerJSON(directory, "state.json", state, owner); err != nil {
			directory.Close()
			return lifecycleOperation{}, err
		}
	}
	if next := state.SupersededRequests[requestID]; next != "" {
		previous, err := loadLifecycleReceipt(directory, requestID)
		directory.Close()
		if err != nil {
			return lifecycleOperation{}, err
		}
		previous.Outcome = "superseded"
		previous.Retryable = false
		previous.SupersededByRequestID = next
		return *previous, errors.New("request was superseded by " + next)
	}
	if state.Uninstalled || state.BlockedInstalls[requestID] || targetRegistrationID != "" && state.RemovedRegistrations[targetRegistrationID] && kind != "registration-remove" {
		directory.Close()
		return lifecycleOperation{}, errors.New("the local owner removed this registration or Host installation")
	}
	if kind == "registration-recover" && targetRegistrationID != "" {
		if legacy, err := loadLifecycleReceipt(directory, requestID); err == nil && legacy.Kind == "install-managed" && state.RegistrationRequests[targetRegistrationID] != "" && state.RegistrationRequests[targetRegistrationID] != requestID {
			directory.Close()
			return *legacy, errors.New("a newer request owns this Registration")
		}
		priorID := state.RegistrationRequests[targetRegistrationID]
		if priorID != "" && priorID != requestID {
			prior, err := loadLifecycleReceipt(directory, priorID)
			if err != nil {
				directory.Close()
				return lifecycleOperation{}, err
			}
			if (prior.Kind == "registration-claim" || prior.Kind == "registration-recover") && prior.Outcome != "succeeded" && prior.Outcome != "superseded" {
				prior.SupersededByRequestID = requestID
				prior.Retryable = false
				if err := writeOwnerJSON(directory, maintenanceReceiptName(priorID), prior, owner); err != nil {
					directory.Close()
					return lifecycleOperation{}, err
				}
				state.SupersededRequests[priorID] = requestID
				if err := writeOwnerJSON(directory, "state.json", state, owner); err != nil {
					directory.Close()
					return lifecycleOperation{}, err
				}
			}
		}
	}
	if targetRegistrationID != "" && (kind == "registration-remove" || kind == "registration-drain" || strings.Contains(kind, "DRAINING")) {
		previousID := state.RegistrationRequests[targetRegistrationID]
		if previousID == "" && state.LatestRequest != "" {
			prior, err := loadLifecycleReceipt(directory, state.LatestRequest)
			if err == nil && prior.Kind == "install-managed" {
				var legacy struct {
					Material struct {
						RegistrationID string `json:"registration_id"`
					} `json:"material"`
				}
				data, err := readInstallOperationState(value, prior.RequestID, "plan.json", registrationPlanMaxBytes)
				if err == nil && json.Unmarshal(data, &legacy) == nil && legacy.Material.RegistrationID == targetRegistrationID {
					previousID = prior.RequestID
				}
			}
		}
		if previousID != "" && previousID != requestID {
			prior, err := loadLifecycleReceipt(directory, previousID)
			if err != nil {
				directory.Close()
				return lifecycleOperation{}, err
			}
			if strings.HasPrefix(prior.Kind, "registration-") || prior.Kind == "install-managed" {
				prior.Outcome = "superseded"
				prior.Retryable = false
				prior.SupersededByRequestID = requestID
				if err := writeOwnerJSON(directory, maintenanceReceiptName(previousID), prior, owner); err != nil {
					directory.Close()
					return lifecycleOperation{}, err
				}
				state.SupersededRequests[previousID] = requestID
				state.BlockedInstalls[previousID] = true
			}
		}
		if kind == "registration-remove" {
			state.RemovedRegistrations[targetRegistrationID] = true
		}
		if err := writeOwnerJSON(directory, "state.json", state, owner); err != nil {
			directory.Close()
			return lifecycleOperation{}, err
		}
	}

	previous, readErr := loadLifecycleReceipt(directory, requestID)
	directory.Close()
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return lifecycleOperation{}, readErr
	}
	var operation lifecycleOperation
	previousFailureStage := ""
	if previous != nil {
		if value.correlationRequestID != "" && previous.CorrelationRequestID != "" && previous.CorrelationRequestID != value.correlationRequestID {
			return *previous, errors.New("legacy request already accepted another correlation")
		}
		if previous.Outcome == "superseded" || previous.SupersededByRequestID != "" {
			return *previous, errors.New("request was superseded by " + previous.SupersededByRequestID)
		}
		if kind == "registration-recover" && previous.Kind == "install-managed" {
			if previous.Outcome == "succeeded" {
				return *previous, errors.New("the legacy request already completed")
			}
			if previous.TargetRegistrationID != targetRegistrationID {
				if previous.TargetRegistrationID != "" {
					return *previous, errors.New("legacy request belongs to another registration")
				}
				target, err := legacyRegistrationTarget(value, requestID)
				if err != nil {
					return *previous, err
				}
				if target != targetRegistrationID {
					return *previous, errors.New("legacy request belongs to another registration")
				}
			}
			previous.Kind = kind
			previous.TargetRegistrationID = targetRegistrationID
		}
		if previous.Kind != kind || previous.TargetRegistrationID != targetRegistrationID || previous.TargetReleaseID != targetReleaseID ||
			previous.TargetArtifactSHA256 != targetArtifactSHA256 || previous.TargetArtifactSize != targetArtifactSize {
			return *previous, errors.New("operation request identity was reused for a different target")
		}
		if previous.Outcome == "succeeded" {
			return *previous, nil
		}
		if previous.Outcome == "failed" {
			if !retryFailed || previous.Kind != kind {
				return *previous, errors.New("operation already failed; use a new request id after reviewing status")
			}
			if !previous.Retryable && kind != "registration-recover" && kind != "registration-claim" && previous.FailureCode != "retry_limit_reached" && previous.FailureCode != "update_retry_limit_reached" && !((kind == "registration-revoke" || kind == "registration-remove") && strings.Contains(previous.FailureMessage, "registration revision conflict")) {
				return *previous, errors.New("operation failure requires operator action")
			}
		}
		if retryFailed && previous.Kind == kind {
			previousFailureStage = previous.FailureStage

			operation = *previous
			newCorrelation := value.correlationRequestID != "" && operation.CorrelationRequestID == ""
			if newCorrelation {
				operation.CorrelationRequestID = value.correlationRequestID
			}
			operation.Outcome = "pending"
			operation.Attempt++
			operation.FailureStage, operation.FailureCode, operation.FailureMessage = "", "", ""
			operation.Retryable = false
			operation.UpdatedAt = time.Now().UnixMilli()
			if err := persistOperationOwned(value, operation, owner); err != nil {
				if newCorrelation {
					return lifecycleOperation{RequestID: value.correlationRequestID, Kind: kind, TargetRegistrationID: targetRegistrationID, Outcome: "unknown", FailureCode: "owner_journal_unavailable", Retryable: true}, err
				}
				return operation, err
			}

		} else {
			observed := readStatus(value)
			if operationConfirmed(previous.Kind, observed) {
				previous.Outcome = "succeeded"
				previous.UpdatedAt = time.Now().UnixMilli()
				if err := persistOperationOwned(value, *previous, owner); err != nil {
					previous.Outcome = "unknown"
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
		operation = lifecycleOperation{
			OperationID: operationID, RequestID: requestID, Kind: kind,
			TargetRegistrationID: targetRegistrationID,
			TargetReleaseID:      targetReleaseID,
			TargetArtifactSHA256: targetArtifactSHA256,
			TargetArtifactSize:   targetArtifactSize,
			Outcome:              "pending",
			Attempt:              1, StartedAt: now, UpdatedAt: now,
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
		var terminalFailure maintenanceFailure
		if errors.As(err, &preparationFailure) {
			operation.FailureStage = preparationFailure.descriptor.Stage
			operation.FailureCode = preparationFailure.descriptor.Code
			operation.FailureMessage = preparationFailure.descriptor.Message
			operation.Retryable = preparationFailure.descriptor.Retryable
		} else if errors.As(err, &terminalFailure) {
			operation.FailureCode, operation.Retryable = terminalFailure.code, false
		} else if kind == "update" && (operation.Stage == "stop" || operation.Stage == "replace" || operation.Stage == "restore" || operation.Stage == "settled") {
			// Once an update has entered the stopped/replacement path, the exact
			// request and selected release are the only safe forward-repair key.
			// Expected administrator handoffs advance through distinct stages.
			// The same request can continue each committed cutover stage.
			if operation.Stage != previousFailureStage {
				operation.Attempt = 1
			}
			operation.FailureCode = "update_forward_repair_required"
			operation.Retryable = true
		}

		operation.UpdatedAt = time.Now().UnixMilli()
		if persistErr := persistOperationOwned(value, operation, owner); persistErr != nil {
			operation.Outcome = "unknown"
			return operation, errors.Join(err, persistErr)
		}
		return operation, err
	}
	if current := readLifecycleOperation(value); current != nil && current.RequestID == operation.RequestID && current.Kind == operation.Kind && current.TargetRegistrationID == operation.TargetRegistrationID && current.TargetReleaseID == operation.TargetReleaseID && current.TargetArtifactSHA256 == operation.TargetArtifactSHA256 && current.TargetArtifactSize == operation.TargetArtifactSize {
		operation.Stage = current.Stage
		operation.Cutover = current.Cutover
	}
	operation.Outcome = "succeeded"
	operation.UpdatedAt = time.Now().UnixMilli()
	if err := persistOperationOwned(value, operation, owner); err != nil {
		operation.Outcome = "unknown"
		return operation, err
	}
	return operation, nil
}

func lifecycleFailure(kind string, err error) (string, string, string, bool) {
	var httpError registrationHTTPError
	if errors.As(err, &httpError) {
		code := httpError.code
		if code == "" {
			code = "registration_http_error"
		}
		return kind, code, httpError.Error(), httpError.status == 408 || httpError.status == 429 || httpError.status >= 500
	}

	if reason, ok := hostv1.ErrorReasonOf(err); ok && reason == hostv1.ErrorReason_ERROR_REASON_LOCAL_DISPOSED {
		return kind, "local_disposed", "The local operator deleted this registration. Start a new authorized setup to connect again.", false
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
	retryClass := kind == "install" || kind == "install-managed" || kind == "repair" || kind == "update" || strings.HasPrefix(kind, "registration-")
	if !retryClass {
		return stage, "operation_failed", message, false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return stage, "transient_timeout", message, true
	}
	if (kind == "registration-revoke" || kind == "registration-remove") && registrationRevisionConflict(err) {
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
	if strings.Contains(message, "registration claim request failed") {
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
	lockName := "owner.lock"
	file, err := openStateFileAt(directory, lockName, unix.O_RDWR|unix.O_CREAT|unix.O_NONBLOCK, 0o600, owner)
	if err != nil {
		directory.Close()
		return func() {}, fmt.Errorf("open lifecycle operation lock: %w", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
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
			return func() {}, errMaintenanceOwnerBusy
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

func writeOperation(operation lifecycleOperation) error {
	return json.NewEncoder(os.Stdout).Encode(operation)
}

func lifecycleExclusive(operation lifecycleOperation) bool {
	switch operation.Kind {
	case "update", "repair", "remove", "drain", "reinstall", "uninstall":
		return true
	default:
		return false
	}
}
