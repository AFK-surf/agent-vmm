//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

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

type hostServiceFacts struct {
	Loaded         bool
	Running        bool
	PID            int
	LastExitStatus *int
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
		HostJob:          value.hostJob(),
		OperatorSocket:   filepath.Join(value.applicationData, "operator.sock"),
		HostInstalled:    value.hostInstalled(),
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
	if service, err := observeHostService(ctx, value); err == nil {
		result.HostObserved = true
		result.HostLoaded = service.Loaded
		result.HostRunning = service.Running
		result.HostPID = service.PID
		result.HostLastExitStatus = service.LastExitStatus
	} else if errors.Is(err, errHostServiceNotFound) {
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
