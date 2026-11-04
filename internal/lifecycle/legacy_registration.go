//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
)

// The old owner wrote these exact files before it applied a Registration.
// They prove identity only. Recovery still requires current authorization and the original root.
func legacyRegistrationTarget(value paths, requestID string) (string, error) {
	data, err := readInstallCompletionState(value, requestID, 4096)
	if err == nil {
		var completed struct {
			Version        int    `json:"version"`
			OperationID    string `json:"operation_id"`
			RegistrationID string `json:"registration_id"`
		}
		if json.Unmarshal(data, &completed) != nil || completed.Version != 1 || completed.OperationID != requestID || !operationIDPattern.MatchString(completed.RegistrationID) {
			return "", errors.New("legacy completion identity is unreadable")
		}
		return completed.RegistrationID, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	data, err = readInstallOperationState(value, requestID, "plan.json", registrationPlanMaxBytes)
	if err != nil {
		return "", err
	}
	var plan struct {
		Version     int    `json:"version"`
		OperationID string `json:"operation_id"`
		Material    struct {
			Version        int    `json:"version"`
			OperationID    string `json:"operation_id"`
			RegistrationID string `json:"registration_id"`
		} `json:"material"`
	}
	if json.Unmarshal(data, &plan) != nil || plan.Version != 1 || plan.OperationID != requestID || plan.Material.Version != 1 || plan.Material.OperationID != requestID || !operationIDPattern.MatchString(plan.Material.RegistrationID) {
		return "", errors.New("legacy plan identity is unreadable")
	}
	return plan.Material.RegistrationID, nil
}

// Convert the original owner facts once. Never advance the active or latest request.
func migrateLegacyRegistration(value paths, requestID, registrationID string) error {
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return err
	}
	state, err := loadMaintenanceState(directory)
	receipt, receiptErr := loadLifecycleReceipt(directory, requestID)
	directory.Close()
	if err != nil {
		return err
	}
	if receiptErr != nil {
		return receiptErr
	}
	if receipt.TargetRegistrationID == registrationID && state.RegistrationRequests[registrationID] != "" {
		return nil
	}
	release, err := acquireOperationLockOwned(value, lifecycleOperationOwner(value))
	if err != nil {
		return err
	}
	defer release()
	directory, err = openExistingApplicationDataDirectory(value)
	if err != nil {
		return err
	}
	defer directory.Close()
	state, err = loadMaintenanceState(directory)
	if err != nil {
		return err
	}
	if state.BlockedInstalls[requestID] {
		return requestBlockedError{requestID: requestID}
	}
	receipt, err = loadLifecycleReceipt(directory, requestID)
	if err != nil {
		return err
	}
	if receipt.Kind != "install-managed" || receipt.RequestID != requestID || receipt.TargetRegistrationID != "" && receipt.TargetRegistrationID != registrationID {
		return errors.New("legacy request identity changed")
	}
	if state.RemovedRegistrations[registrationID] || state.SupersededRequests[requestID] != "" || receipt.SupersededByRequestID != "" || receipt.Outcome == "superseded" {
		return errors.New("legacy request was fenced")
	}
	if owner := state.RegistrationRequests[registrationID]; owner != "" && owner != requestID {
		return errors.New("a newer request owns this Registration")
	}
	target, err := legacyRegistrationTarget(value, requestID)
	if err != nil {
		return err
	}
	if target != registrationID {
		return errors.New("legacy proof belongs to another Registration")
	}
	var rawReceipt, rawState map[string]json.RawMessage
	if err := readMaintenanceJSON(directory, maintenanceReceiptName(requestID), &rawReceipt); err != nil {
		return err
	}
	if err := readMaintenanceJSON(directory, "state.json", &rawState); err != nil {
		return err
	}
	if receipt.TargetRegistrationID == "" {
		rawReceipt["targetRegistrationId"], _ = json.Marshal(target)
		if err := writeOwnerJSON(directory, maintenanceReceiptName(requestID), rawReceipt, lifecycleOperationOwner(value)); err != nil {
			return err
		}
	}
	if state.RegistrationRequests[target] == "" {
		state.RegistrationRequests[target] = requestID
		rawState["registrationRequests"], _ = json.Marshal(state.RegistrationRequests)
		if err := writeOwnerJSON(directory, "state.json", rawState, lifecycleOperationOwner(value)); err != nil {
			return err
		}
	}
	return nil
}
