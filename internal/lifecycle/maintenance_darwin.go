package lifecycle

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

// The owner journal survives bundle replacement and Host data reset.
type maintenanceRequest struct {
	Version             int    `json:"version"`
	RequestID           string `json:"requestId"`
	Action              string `json:"action"`
	DataPolicy          string `json:"dataPolicy"`
	TargetApp           string `json:"targetApp"`
	SourceApp           string `json:"sourceApp,omitempty"`
	TargetReleaseID     string `json:"targetReleaseId,omitempty"`
	SupersedesRequestID string `json:"supersedesRequestId,omitempty"`
	ConfirmSharedHost   bool   `json:"confirmSharedHost"`
	ConfirmDataDeletion bool   `json:"confirmDataDeletion,omitempty"`
}

type maintenanceReceipt struct {
	Attempt int `json:"attempt"`
	maintenanceRequest
	AdmissionPreviousRequestID string      `json:"admissionPreviousRequestId,omitempty"`
	SupersededByRequestID      string      `json:"supersededByRequestId,omitempty"`
	ServiceMode                serviceMode `json:"serviceMode"`
	ServiceUser                string      `json:"serviceUser"`
	DisablePairing             bool        `json:"disablePairing"`
	Stage                      string      `json:"stage"`
	Outcome                    string      `json:"outcome"`
	Retryable                  bool        `json:"retryable"`
	FailureCode                string      `json:"failureCode,omitempty"`
	FailureMessage             string      `json:"failureMessage,omitempty"`
	cutoverProgress
	StartedAt int64 `json:"startedAt"`
	UpdatedAt int64 `json:"updatedAt"`
}

func validateMaintenanceRequest(value paths, request maintenanceRequest) error {
	if request.Version != 1 || !operationIDPattern.MatchString(request.RequestID) || !request.ConfirmSharedHost {
		return errors.New("maintenance requires version 1, a stable requestId and explicit shared Host confirmation")
	}
	if request.Action != "update" && request.Action != "reinstall" && request.Action != "uninstall" {
		return errors.New("unsupported maintenance action")
	}
	if request.DataPolicy != "preserve" && request.DataPolicy != "reset" {
		return errors.New("maintenance dataPolicy must be preserve or reset")
	}
	if request.Action == "update" && request.DataPolicy != "preserve" {
		return errors.New("update must preserve data")
	}
	if request.DataPolicy == "reset" && !request.ConfirmDataDeletion {
		return errors.New("reset requires explicit deletion of all shared Host data")
	}
	if request.DataPolicy == "preserve" && request.ConfirmDataDeletion {
		return errors.New("data deletion confirmation conflicts with preserve")
	}
	if request.SupersedesRequestID != "" && (!operationIDPattern.MatchString(request.SupersedesRequestID) || request.SupersedesRequestID == request.RequestID) {
		return errors.New("maintenance replacement requires an exact prior request")
	}
	standard := filepath.Join(value.home, "Library", "Application Support", "Agent VMM Host", "current", bundle.HostAppName)
	if request.TargetApp != standard || value.app != standard {
		return errors.New("maintenance target must be this service user's standard shared Host app")
	}
	for _, target := range []string{value.app, value.applicationData, value.logs, value.agentPlist} {
		if err := validateMaintenancePath(value.home, target); err != nil {
			return err
		}
	}
	if request.Action == "uninstall" {
		if request.SourceApp != "" || request.TargetReleaseID != "" {
			return errors.New("uninstall does not accept an installation artifact")
		}
	} else {
		if request.TargetReleaseID == "" {
			return errors.New("maintenance requires the selected release")
		}

		if !filepath.IsAbs(request.SourceApp) || filepath.Base(request.SourceApp) != bundle.HostAppName || request.SourceApp == request.TargetApp {
			return errors.New("maintenance requires a separate verified source Host app")
		}
		for _, removed := range []string{value.app, value.applicationData, value.logs} {
			if relative, err := filepath.Rel(removed, request.SourceApp); err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return errors.New("maintenance source must be outside the target and reset data")
			}
		}
	}
	return nil
}

type maintenanceActions struct {
	validateSource  func(maintenanceRequest) error
	targetSchema    func(maintenanceRequest) (targetSchemaRange, error)
	dataSchema      func(paths) (int, error)
	commitState     func(*os.File, maintenanceState) error
	commitAdmission func(*os.File, maintenanceState) error
	stop            func(paths) error
	removeSoftware  func(paths) error
	resetData       func(paths) error
	replace         func(paths, maintenanceRequest) error
	restore         func(paths, string) error
	verify          func(paths, maintenanceRequest) error
}

func runMaintenance(value paths, request maintenanceRequest, actions maintenanceActions) (receipt maintenanceReceipt, err error) {
	if err = validateMaintenanceRequest(value, request); err != nil {
		return receipt, err
	}
	directory, release, err := lockMaintenance(value)
	if err != nil {
		return receipt, err
	}
	defer release()
	state, err := loadMaintenanceState(directory)
	if err != nil {
		return receipt, err
	}
	name := maintenanceReceiptName(request.RequestID)
	readErr := readMaintenanceJSON(directory, name, &receipt)
	if readErr == nil {
		if receipt.Action != request.Action || receipt.DataPolicy != request.DataPolicy || receipt.TargetApp != request.TargetApp || receipt.TargetReleaseID != request.TargetReleaseID || receipt.ServiceMode != value.service.Mode || receipt.ServiceUser != value.service.User {
			return receipt, errors.New("maintenance requestId belongs to a different action, data policy, service or artifact")
		}
		if next := state.SupersededRequests[request.RequestID]; next != "" {
			return supersededMaintenanceReceipt(receipt, next), nil
		}
		if state.CompletedRequests[request.RequestID] {
			return completedMaintenanceReceipt(receipt), nil
		}
		if receipt.FailureCode == maintenanceTargetOlderThanData || receipt.Outcome == "failed" && !receipt.Retryable {
			return receipt, errors.New(receipt.FailureMessage)
		}
		if state.LatestRequest != request.RequestID && request.SupersedesRequestID == "" &&
			(state.ActiveRequest != "" || receipt.Stage != "accepted" || receipt.Outcome != "pending" || receipt.AdmissionPreviousRequestID != state.LatestRequest) {
			return receipt, errors.New("this unfinished request is no longer current; inspect the latest maintenance request")
		}
		if receipt.EffectsVerified && state.ActiveRequest == request.RequestID {
			return settleMaintenance(directory, name, state, receipt, actions.commitState)
		}

	} else if !errors.Is(readErr, os.ErrNotExist) {
		return receipt, readErr
	} else if state.CompletedRequests[request.RequestID] || state.SupersededRequests[request.RequestID] != "" || state.ActiveRequest == request.RequestID || state.LatestRequest == request.RequestID {
		return receipt, errors.New("accepted maintenance receipt is unavailable; keep the owner journal for recovery")
	}
	superseding := request.SupersedesRequestID != "" && state.ActiveRequest != request.RequestID
	if superseding {
		if err = validateMaintenanceSupersession(directory, value, request, state); err != nil {
			return receipt, err
		}
	} else if request.SupersedesRequestID != "" && state.SupersededRequests[request.SupersedesRequestID] != request.RequestID {
		return receipt, errors.New("maintenance supersession is not owned by this request")
	} else if state.ActiveRequest != "" && state.ActiveRequest != request.RequestID {
		return receipt, errors.New("continue the existing maintenance request before starting another")
	}

	if request.Action != "uninstall" {
		if err = actions.validateSource(request); err != nil {
			return receipt, err
		}
	}
	// Check before any effect, and before a supersession commit, that the
	// target opens the existing data. Reset discards the data.
	if request.Action != "uninstall" && request.DataPolicy == "preserve" && readErr != nil {
		refusal, checkErr := maintenanceSchemaRefusal(value, request, actions)
		if checkErr != nil {
			return receipt, checkErr
		}
		if refusal != "" {
			return refuseMaintenance(directory, name, value, request, refusal)
		}
	}
	if readErr != nil {
		now := time.Now().UnixMilli()
		receipt = maintenanceReceipt{maintenanceRequest: request, AdmissionPreviousRequestID: state.LatestRequest, ServiceMode: value.service.Mode, ServiceUser: value.service.User, DisablePairing: value.service.DisablePairing, Stage: "accepted", Outcome: "pending", Retryable: true, StartedAt: now, UpdatedAt: now}
	}
	state.ActiveRequest, state.LatestRequest = request.RequestID, request.RequestID
	if request.DataPolicy == "reset" {
		if err = collectMaintenanceInstallIDs(value, &state); err != nil {
			return receipt, err
		}
	}
	// Stage every new request before the owner pointer. Until the state commit
	// it authorizes no effects and status does not report it as accepted.
	if readErr != nil || superseding {
		if err = writeMaintenanceJSON(directory, name, receipt); err != nil {
			return receipt, err
		}
	}
	if superseding {
		state.SupersededRequests[request.SupersedesRequestID] = request.RequestID
	}

	if err = actions.commitAdmission(directory, state); err != nil {
		return receipt, err
	}

	return executeMaintenance(value, directory, name, state, receipt, actions)
}

func executeMaintenance(value paths, directory *os.File, name string, state maintenanceState, saved maintenanceReceipt, actions maintenanceActions) (receipt maintenanceReceipt, err error) {
	receipt = saved
	request := receipt.maintenanceRequest

	defer func() {
		if err != nil && receipt.Outcome != "succeeded" {
			receipt.Outcome, receipt.Retryable = "failed", true
			receipt.FailureCode, receipt.FailureMessage = "maintenance_incomplete", err.Error()

			var failure maintenanceFailure
			if errors.As(err, &failure) {
				receipt.Retryable, receipt.FailureCode = false, failure.code
			}
			receipt.UpdatedAt = time.Now().UnixMilli()
			if saveErr := writeMaintenanceJSON(directory, name, receipt); saveErr != nil {
				err = errors.Join(err, saveErr)
			}
			if !receipt.Retryable {
				state.ActiveRequest = ""
				if saveErr := actions.commitState(directory, state); saveErr != nil {
					err = errors.Join(err, saveErr)
				}
			}
		}
	}()

	save := func(stage string) error {
		if stage == "restore" {
			if receipt.Attempt >= boundedOperationMaxAttempts {
				return maintenanceFailure{code: maintenanceHostStartFailed, err: errors.New("Host restore retry limit reached")}
			}
			receipt.Attempt++
		}
		receipt.Stage, receipt.Outcome, receipt.Retryable = stage, "pending", true
		receipt.FailureCode, receipt.FailureMessage = "", ""
		receipt.UpdatedAt = time.Now().UnixMilli()
		return writeMaintenanceJSON(directory, name, receipt)
	}
	err = cutover(&receipt.cutoverProgress, request.DataPolicy == "reset", request.Action == "uninstall", save, cutoverActions{
		stop: func() error { return actions.stop(value) }, reset: func() error { return actions.resetData(value) }, remove: func() error { return actions.removeSoftware(value) },
		replace: func() error { return actions.replace(value, request) }, restore: func() error {
			err := actions.restore(value, maintenanceRestoreRequestID(request.RequestID, uint64(receipt.Attempt)))
			if err != nil && receipt.Attempt == boundedOperationMaxAttempts {
				return maintenanceFailure{code: maintenanceHostStartFailed, err: err}
			}
			return err
		},
		verify: func() error { return actions.verify(value, request) },
	})
	if err != nil {
		return receipt, err
	}

	return settleMaintenance(directory, name, state, receipt, actions.commitState)
}

// maintenanceSchemaRefusal returns a refusal message when the target does not
// open the existing local database.
