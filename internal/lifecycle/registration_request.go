//go:build darwin || linux

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

// Registration owns the original local request reference. This projection does not create an alias ledger.
func originalRegistrationRequestID(value paths, registrationID, requested, kind string) (string, error) {
	directory, err := openExistingApplicationDataDirectory(value)
	if errors.Is(err, os.ErrNotExist) {
		return requested, nil
	}
	if err != nil {
		return "", err
	}
	state, err := loadMaintenanceState(directory)
	if err != nil {
		directory.Close()
		return "", err
	}
	if original := state.RegistrationRequests[registrationID]; original != "" {
		prior, err := loadLifecycleReceipt(directory, original)
		if err != nil {
			directory.Close()
			return "", err
		}
		if prior.CorrelationRequestID != "" && prior.CorrelationRequestID != requested {
			directory.Close()
			return requested, nil
		}
		if kind == "registration-recover" && (prior.CorrelationRequestID == requested && requested != "" || prior.Kind == "install-managed" && prior.Outcome != "succeeded" && prior.Outcome != "superseded" && state.SupersededRequests[original] == "" && !state.BlockedInstalls[original]) {
			directory.Close()
			return original, nil
		}
	}
	for _, id := range []string{state.ActiveRequest, state.LatestRequest} {
		if id == "" {
			continue
		}
		prior, err := loadLifecycleReceipt(directory, id)
		if err != nil {
			directory.Close()
			return "", err
		}
		if kind == "registration-recover" && prior.Kind == "registration-recover" && prior.CorrelationRequestID == requested && prior.TargetRegistrationID == registrationID && requested != "" {
			directory.Close()
			return id, nil
		}
		if prior.Kind != "install-managed" || prior.Outcome == "succeeded" || prior.Outcome == "superseded" || kind != "registration-recover" || state.SupersededRequests[id] != "" || state.BlockedInstalls[id] {
			continue
		}
		raw, err := readInstallOperationState(value, id, "plan.json", registrationPlanMaxBytes)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			directory.Close()
			return "", err
		}
		var old struct {
			Material struct {
				RegistrationID string `json:"registration_id"`
			} `json:"material"`
		}
		if json.Unmarshal(raw, &old) == nil && old.Material.RegistrationID == registrationID {
			directory.Close()
			return id, nil
		}
	}
	directory.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		if requested == "" || kind == "registration-recover" {
			return "", err
		}
		return requested, nil
	}
	defer connection.Close()
	response, err := hostv1.NewOperatorServiceClient(connection).GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
	if err != nil {
		if (requested == "" || kind == "registration-recover") && status.Code(err) != codes.NotFound {
			return "", err
		}
		return requested, nil
	}
	original := response.GetRegistration().GetSourceRequestId()

	if original == "" || original == requested {
		return requested, nil
	}
	directory, err = openExistingApplicationDataDirectory(value)
	if err != nil {
		return "", err
	}
	receipt, err := loadLifecycleReceipt(directory, original)
	directory.Close()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if receipt != nil && kind == "registration-recover" && (receipt.CorrelationRequestID == requested && requested != "" || receipt.Kind == "install-managed" && receipt.Outcome != "succeeded" && receipt.Outcome != "superseded" && state.SupersededRequests[original] == "" && !state.BlockedInstalls[original]) {
		return original, nil
	}
	return requested, nil
}
