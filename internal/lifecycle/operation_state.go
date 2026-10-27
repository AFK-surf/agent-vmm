//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"

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
	if receiptUnfinished(operation.Outcome, operation.Retryable) {
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

// versionReport returns this build's database schema and release.
func versionReport() map[string]any {
	return map[string]any{"component": "agent-vmm-host", "version": releaseID, "release_id": releaseID, "currentSchema": currentSchema()}
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
	if err != nil || operation.OperationID != operationID {
		return nil, errors.New("lifecycle operation was not found")
	}
	return operation, nil
}
