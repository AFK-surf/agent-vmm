//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func newProductionLifecycleAdapterForResume(value paths, operationID string) (LifecycleAdapter, error) {
	adapter, err := productionLifecycleAdapterForResume(value, operationID)
	if err != nil {
		return nil, err
	}
	return lifecycleAdapterFor(adapter), nil
}

func productionLifecycleAdapterForResume(value paths, operationID string) (*productionLifecycleAdapter, error) {
	data, err := readInstallOperationState(value, operationID, "plan.json", lifecycleOperationHandoffMaxBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errManagedInstallPlanUnavailable
		}
		return nil, fmt.Errorf("read managed install plan: %w", err)
	}
	var plan persistedInstallOperation
	if len(data) > lifecycleOperationHandoffMaxBytes || json.Unmarshal(data, &plan) != nil ||
		plan.Version != 1 || plan.OperationID != operationID || plan.Material.OperationID != operationID ||
		plan.DeadlineUnixMillis <= 0 || !validPersistedAcknowledgement(plan, operationID) {
		return nil, errors.New("managed install plan is invalid")
	}
	adapter := &productionLifecycleAdapter{
		value:      value,
		descriptor: plan.AckDescriptor,
		material:   &plan.Material,
	}
	return adapter, nil
}

func persistOperationHandoff(value paths, directory string, material operationHandoffPlan, deadlineUnixMillis int64, acknowledged bool, descriptors ...*operationHandoffDescriptor) (string, error) {
	return persistOperationHandoffOwned(value, directory, material, deadlineUnixMillis, acknowledged, lifecycleOperationOwner(value), descriptors...)
}

func persistOperationHandoffOwned(value paths, directory string, material operationHandoffPlan, deadlineUnixMillis int64, acknowledged bool, owner *operationFileOwner, descriptors ...*operationHandoffDescriptor) (string, error) {
	if deadlineUnixMillis <= 0 {
		return "", errors.New("managed install deadline is invalid")
	}
	if !operationIDPattern.MatchString(material.OperationID) || filepath.Clean(directory) != filepath.Join(value.applicationData, "install-operations", material.OperationID) {
		return "", errors.New("managed install operation directory is invalid")
	}
	operationDirectory, err := openInstallOperationDirectory(value, material.OperationID, owner)
	if err != nil {
		return "", fmt.Errorf("create install operation directory: %w", err)
	}
	defer operationDirectory.Close()
	var ackDescriptor *operationHandoffDescriptor
	if !acknowledged && len(descriptors) != 0 {
		ackDescriptor = descriptors[0]
	}
	if !acknowledged && !validPersistedAckDescriptor(ackDescriptor, material.OperationID) {
		return "", errors.New("managed install acknowledgement capability is invalid")
	}
	planBytes, _ := json.Marshal(persistedInstallOperation{Version: 1, OperationID: material.OperationID, DeadlineUnixMillis: deadlineUnixMillis, Acknowledged: acknowledged, AckDescriptor: ackDescriptor, Material: material})
	if err := atomicWriteStateFileAt(operationDirectory, "plan.json", planBytes, 0o600, owner); err != nil {
		return "", err
	}
	return filepath.Join(directory, "plan.json"), nil
}

func cleanupInstallPlan(value paths, operationID string) error {
	return removeInstallOperationState(value, operationID)
}

func completedInstallPath(value paths, operationID string) string {
	return filepath.Join(value.applicationData, "install-completions", operationID+".json")
}

func persistCompletedInstall(value paths, adapter *productionLifecycleAdapter) error {
	return persistCompletedInstallOwned(value, adapter, lifecycleOperationOwner(value))
}

func persistCompletedInstallOwned(value paths, adapter *productionLifecycleAdapter, owner *operationFileOwner) error {
	if adapter == nil || adapter.material == nil {
		return errors.New("managed install completion material is unavailable")
	}
	material := adapter.material
	receipt := completedOperationReceipt{
		Version:        1,
		OperationID:    material.OperationID,
		RegistrationID: material.RegistrationID,
	}
	if !validCompletedInstallReceipt(receipt, material.OperationID) {
		return errors.New("managed install completion receipt is invalid")
	}
	directory, err := openInstallCompletionDirectory(value, owner)
	if err != nil {
		return fmt.Errorf("create install completion directory: %w", err)
	}
	defer directory.Close()
	encoded, _ := json.Marshal(receipt)
	return atomicWriteStateFileAt(directory, material.OperationID+".json", encoded, 0o600, owner)
}

func completedInstallExists(value paths, operationID string) (bool, error) {
	data, err := readInstallCompletionState(value, operationID, 4096)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read managed install completion: %w", err)
	}
	var receipt completedOperationReceipt
	if len(data) > 4096 || json.Unmarshal(data, &receipt) != nil || !validCompletedInstallReceipt(receipt, operationID) {
		return false, errors.New("managed install completion receipt is invalid")
	}
	return true, nil
}

func validCompletedInstallReceipt(receipt completedOperationReceipt, operationID string) bool {
	return receipt.Version == 1 && receipt.OperationID == operationID &&
		operationIDPattern.MatchString(operationID) && operationIDPattern.MatchString(receipt.RegistrationID)
}
