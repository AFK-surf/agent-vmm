//go:build darwin || linux

package lifecycle

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

func productionRegistrationAdapterForResume(value paths, requestID string) (*productionLifecycleAdapter, error) {
	plan, err := loadClaimPlan(value, requestID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errRegistrationPlanUnavailable
		}
		return nil, err
	}
	return &productionLifecycleAdapter{value: value, requestID: requestID, plan: plan, material: plan.Material}, nil
}
func loadClaimPlan(value paths, requestID string) (*registrationClaimPlan, error) {
	data, err := readInstallOperationState(value, requestID, "plan.json", registrationPlanMaxBytes)
	if err != nil {
		return nil, err
	}
	var plan registrationClaimPlan
	if json.Unmarshal(data, &plan) != nil || plan.Version != 2 || plan.RequestID != requestID || (plan.RecoveryURL == "" && validateClaimDescriptor(plan.Descriptor) != nil) || !validCandidateCredential(plan.CandidateCredential) || plan.DeviceID == "" || len(plan.RootPublicKey) != 33 || plan.RootKeyRevision == 0 {
		return nil, errRegistrationPlanUnavailable
	}
	if plan.Material != nil {
		if err := validateRegistrationConfiguration(*plan.Material, plan.Descriptor); err != nil {
			return nil, err
		}
	}
	return &plan, nil
}
func persistClaimPlan(value paths, plan *registrationClaimPlan) error {
	if !operationIDPattern.MatchString(plan.RequestID) {
		return errors.New("registration request identity is invalid")
	}
	directory, err := openInstallOperationDirectory(value, plan.RequestID, lifecycleOperationOwner(value))
	if err != nil {
		return fmt.Errorf("create protected registration plan: %w", err)
	}
	defer directory.Close()
	data, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	return atomicWriteStateFileAt(directory, "plan.json", data, 0o600, lifecycleOperationOwner(value))
}

func validCandidateCredential(candidate []byte) bool {
	if len(candidate) != 43 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(candidate))
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == string(candidate)
}
