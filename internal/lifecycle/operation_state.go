//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func persistOperation(value paths, operation lifecycleOperation) error {
	return persistOperationOwned(value, operation, nil)
}

func persistOperationOwned(value paths, operation lifecycleOperation, owner *operationFileOwner) error {
	directory, err := openApplicationDataDirectory(value, owner)
	if err != nil {
		return err
	}
	defer directory.Close()
	state, err := loadMaintenanceState(directory)
	if err != nil {
		return err
	}
	if err := writeOwnerJSON(directory, maintenanceReceiptName(operation.RequestID), operation, owner); err != nil {
		return err
	}
	state.LatestRequest = operation.RequestID
	state.OperationRequests[operation.OperationID] = operation.RequestID
	state.OperationRequests[operation.RequestID] = operation.RequestID
	if operation.TargetRegistrationID != "" {
		state.RegistrationRequests[operation.TargetRegistrationID] = operation.RequestID
	}
	if lifecycleExclusive(operation) && receiptUnfinished(operation.Outcome, operation.Retryable) {
		state.ActiveRequest = operation.RequestID
	} else if state.ActiveRequest == operation.RequestID {
		state.ActiveRequest = ""
	}
	if operation.Outcome == "succeeded" {
		state.CompletedRequests[operation.RequestID] = true
	}
	return writeOwnerJSON(directory, "state.json", state, owner)
}

func readLifecycleRequest(value paths, id string) *lifecycleOperation {
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil
	}
	defer directory.Close()
	operation, err := loadLifecycleReceipt(directory, id)
	if err != nil {
		return nil
	}
	return operation
}

func readLifecycleOperation(value paths) *lifecycleOperation {
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil
	}
	defer directory.Close()
	state, err := loadMaintenanceState(directory)
	if err != nil {
		return nil
	}
	if state.LatestRequest == "" {
		return nil
	}
	operation, err := loadLifecycleReceipt(directory, state.LatestRequest)
	if err != nil {
		return nil
	}
	return operation
}

// versionReport returns this helper's output contract, database schema, and release.
func versionReport() map[string]any {
	return map[string]any{"contractVersion": cliContractVersion, "component": "agent-vmm-host", "version": releaseID, "release_id": releaseID, "currentSchema": currentSchema(), "minimumSchema": store.MinimumSchema()}
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
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	state, err := loadMaintenanceState(directory)
	if err != nil {
		return nil, err
	}
	id := state.OperationRequests[operationID]
	if id == "" {
		id = operationID
	}
	operation, err := loadLifecycleReceipt(directory, id)
	if errors.Is(err, os.ErrNotExist) && (state.OperationRequests[operationID] != "" || state.ActiveRequest == id || state.LatestRequest == id || state.CompletedRequests[id] || state.SupersededRequests[id] != "") {
		return nil, errors.New("accepted lifecycle receipt is unavailable; keep the owner journal for recovery")
	}
	if err != nil {
		return nil, err
	}
	if operation.OperationID != operationID && operation.RequestID != operationID {
		return nil, errors.New("lifecycle operation was not found")
	}
	return operation, nil
}

func readOperationReportByID(value paths, id string) (map[string]any, error) {
	receipt, err := readLifecycleOperationByID(value, id)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	var report map[string]any
	if err := readMaintenanceJSON(directory, maintenanceReceiptName(receipt.RequestID), &report); err != nil {
		return nil, err
	}
	report["operationId"] = receipt.OperationID
	report["requestId"] = receipt.RequestID
	report["kind"] = receipt.Kind
	report["contractVersion"] = cliContractVersion
	return report, nil
}

// Only a committed one-time legacy receipt correlation can satisfy another request ID.
func readCorrelatedOperation(value paths, requested, registrationID string) (map[string]any, error) {
	report, err := readOperationReportByID(value, requested)
	if err != nil {
		return nil, err
	}
	if report != nil {
		if report["targetRegistrationId"] != registrationID {
			return nil, errors.New("request target does not match registration")
		}
		return report, nil
	}
	directory, err := openExistingApplicationDataDirectory(value)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state, err := loadMaintenanceState(directory)
	directory.Close()
	if err != nil {
		return nil, err
	}
	canonical := state.RegistrationRequests[registrationID]
	if canonical == "" {
		return nil, nil
	}
	report, err = readOperationReportByID(value, canonical)
	if err != nil {
		return nil, err
	}
	if report == nil {
		return nil, errors.New("accepted registration receipt is unavailable")
	}
	if report["correlationRequestId"] != requested || report["kind"] != "registration-recover" || report["targetRegistrationId"] != registrationID {
		return nil, nil
	}
	return report, nil
}
