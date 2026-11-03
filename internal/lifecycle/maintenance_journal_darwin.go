package lifecycle

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func collectMaintenanceInstallIDs(value paths, state *maintenanceState) error {
	if old := readLifecycleOperation(value); old != nil && old.Kind == "install-managed" {
		state.BlockedInstalls[old.RequestID] = true
	}
	for _, name := range []string{"install-operations", "install-completions"} {
		path := filepath.Join(value.applicationData, name)
		dir, err := openExistingServiceStateDirectory(path, nil)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for {
			entries, readErr := dir.ReadDir(32)
			for _, entry := range entries {
				id := entry.Name()
				if name == "install-completions" {
					id = strings.TrimSuffix(id, ".json")
				}
				if operationIDPattern.MatchString(id) {
					state.BlockedInstalls[id] = true
				}
			}
			if readErr != nil {
				dir.Close()
				if readErr == io.EOF {
					break
				}
				return readErr
			}
		}
	}
	return nil
}

func refuseMaintenance(directory *os.File, name string, value paths, request maintenanceRequest, message string) (maintenanceReceipt, error) {
	now := time.Now().UnixMilli()
	receipt := maintenanceReceipt{maintenanceRequest: request, ServiceMode: value.service.Mode, ServiceUser: value.service.User, DisablePairing: value.service.DisablePairing,
		Stage: "accepted", Outcome: "failed", FailureCode: maintenanceTargetOlderThanData, FailureMessage: message, StartedAt: now, UpdatedAt: now}
	if err := writeMaintenanceJSON(directory, name, receipt); err != nil {
		return receipt, err
	}
	return receipt, errors.New(message)
}

func supersededMaintenanceReceipt(receipt maintenanceReceipt, next string) maintenanceReceipt {
	receipt.Outcome, receipt.Retryable, receipt.SupersededByRequestID = "superseded", false, next
	return receipt
}

// The caller holds owner.lock, so the executor of a pending request is gone.
// A replacement reads only the request identity, outcome, and service fields
// that every version 1 receipt has.
func validateMaintenanceSupersession(directory *os.File, value paths, request maintenanceRequest, state maintenanceState) error {
	previousID := request.SupersedesRequestID
	if state.ActiveRequest != previousID || state.LatestRequest != previousID || state.CompletedRequests[previousID] || state.SupersededRequests[previousID] != "" {
		return errors.New("only the exact active maintenance request can be replaced")
	}
	var previous maintenanceReceipt
	if err := readMaintenanceJSON(directory, maintenanceReceiptName(previousID), &previous); err != nil {
		return err
	}
	if previous.Version != 1 || previous.RequestID != previousID || (previous.Outcome != "failed" || !previous.Retryable) || previous.TargetApp != request.TargetApp || previous.ServiceMode != value.service.Mode || previous.ServiceUser != value.service.User {
		return errors.New("replacement requires a retryable failed receipt for the same Host and service")
	}
	return nil
}

func maintenanceStatus(value paths, id string) (*maintenanceReceipt, error) {
	directory, err := openMaintenanceDirectory(value, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	state, err := loadMaintenanceState(directory)
	if err != nil {
		return nil, err
	}
	if id == "" {
		id = state.LatestRequest
	}
	if id == "" {
		return nil, nil
	}
	var receipt maintenanceReceipt
	if err := readMaintenanceJSON(directory, maintenanceReceiptName(id), &receipt); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if state.ActiveRequest == id || state.LatestRequest == id || state.CompletedRequests[id] || state.SupersededRequests[id] != "" {
				return nil, errors.New("accepted maintenance receipt is unavailable; keep the owner journal for recovery")
			}
			return nil, nil
		}
		return nil, err
	}
	if receipt.Version == 0 && receipt.Action == "" {
		operation, err := loadLifecycleReceipt(directory, id)
		if err != nil {
			return nil, err
		}
		receipt.Version = 1
		receipt.Action = operation.Kind
		receipt.Kind = operation.Kind
		receipt.OperationID = operation.OperationID
		receipt.RequestID = operation.RequestID
		receipt.TargetRegistrationID = operation.TargetRegistrationID
		receipt.TargetReleaseID = operation.TargetReleaseID
		receipt.Stage = operation.Stage
		receipt.Outcome = operation.Outcome
		receipt.Attempt = operation.Attempt
		receipt.Retryable = operation.Retryable
		receipt.FailureCode = operation.FailureCode
		receipt.FailureMessage = operation.FailureMessage
		receipt.StartedAt = operation.StartedAt
		receipt.UpdatedAt = operation.UpdatedAt
		if next := state.SupersededRequests[id]; next != "" {
			receipt.Outcome = "superseded"
			receipt.Retryable = false
			receipt.SupersededByRequestID = next
		}
		return &receipt, nil
	}
	if receipt.Version != 1 || receipt.RequestID != id {
		return nil, errors.New("Host maintenance receipt has an unsupported identity or version")
	}
	if next := state.SupersededRequests[id]; next != "" {
		receipt = supersededMaintenanceReceipt(receipt, next)
	} else if state.CompletedRequests[id] {
		receipt = completedMaintenanceReceipt(receipt)
	} else if receipt.Outcome == "succeeded" {
		return nil, errors.New("maintenance completion is not committed")
	} else if receipt.FailureCode != maintenanceTargetOlderThanData && state.ActiveRequest != id && !(receipt.Outcome == "failed" && !receipt.Retryable && state.LatestRequest == id) {
		// A receipt written before a failed admission commit is recovery
		// material, not an accepted maintenance operation.
		return nil, nil
	}
	return &receipt, nil
}

// A failed reset can already have removed service.json. A replacement keeps
// that operation's service identity instead of reinterpreting auto as agent.
func maintenanceServiceReceipt(value paths, request maintenanceRequest) (*maintenanceReceipt, error) {
	saved, err := maintenanceStatus(value, request.RequestID)
	if err != nil || saved != nil || request.SupersedesRequestID == "" {
		return saved, err
	}
	return maintenanceStatus(value, request.SupersedesRequestID)
}

func checkMaintenanceInstallTarget(value paths, operationID string) error {
	if operationID == "" {
		return nil
	}
	directory, err := openMaintenanceDirectory(value, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()
	state, err := loadMaintenanceState(directory)
	if err != nil {
		return err
	}
	if state.BlockedInstalls[operationID] {
		return errors.New("this old installation was removed by the local owner; start a new authorized setup")
	}
	return nil
}

func completedMaintenanceReceipt(receipt maintenanceReceipt) maintenanceReceipt {
	receipt.Stage, receipt.Outcome, receipt.Retryable = "settled", "succeeded", false
	receipt.FailureCode, receipt.FailureMessage = "", ""
	return receipt
}

// The state write is the completion commit. Status can project it after a crash
// without mutating the journal; it never trusts an uncommitted success receipt.
func settleMaintenance(directory *os.File, name string, state maintenanceState, receipt maintenanceReceipt, commit func(*os.File, maintenanceState) error) (maintenanceReceipt, error) {
	state.ActiveRequest, state.Uninstalled = "", receipt.Action == "uninstall"
	state.CompletedRequests[receipt.RequestID] = true

	if err := commit(directory, state); err != nil {
		receipt.Outcome, receipt.Retryable = "failed", true
		receipt.FailureCode, receipt.FailureMessage = "maintenance_incomplete", "Maintenance effects are verified, but completion could not be committed. Retry this request."
		receipt.UpdatedAt = time.Now().UnixMilli()
		return receipt, errors.Join(err, writeMaintenanceJSON(directory, name, receipt))
	}
	receipt = completedMaintenanceReceipt(receipt)
	receipt.UpdatedAt = time.Now().UnixMilli()
	return receipt, writeMaintenanceJSON(directory, name, receipt)
}
