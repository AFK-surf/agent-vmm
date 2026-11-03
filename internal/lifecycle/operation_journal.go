//go:build darwin || linux

package lifecycle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

type maintenanceState struct {
	Version              int               `json:"version"`
	ActiveRequest        string            `json:"activeRequest,omitempty"`
	LatestRequest        string            `json:"latestRequest,omitempty"`
	Uninstalled          bool              `json:"uninstalled"`
	CompletedRequests    map[string]bool   `json:"completedRequests,omitempty"`
	SupersededRequests   map[string]string `json:"supersededRequests,omitempty"`
	BlockedInstalls      map[string]bool   `json:"blockedInstalls,omitempty"`
	OperationRequests    map[string]string `json:"operationRequests,omitempty"`
	RegistrationRequests map[string]string `json:"registrationRequests,omitempty"`
	RemovedRegistrations map[string]bool   `json:"removedRegistrations,omitempty"`
}

func readMaintenanceJSON(directory *os.File, name string, result any) error {
	data, err := readStateFileAt(directory, name, 4<<20)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, result); err != nil {
		return errors.New("Host maintenance journal is unreadable")
	}
	return nil
}

func writeMaintenanceJSON(directory *os.File, name string, value any) error {
	var owner *operationFileOwner
	if os.Geteuid() == 0 {
		var stat unix.Stat_t
		if err := unix.Fstat(int(directory.Fd()), &stat); err != nil {
			return err
		}
		if stat.Uid != 0 {
			owner = &operationFileOwner{uid: int(stat.Uid), gid: int(stat.Gid), fchown: unix.Fchown}
		}
	}
	return writeOwnerJSON(directory, name, value, owner)
}

func writeOwnerJSON(directory *os.File, name string, value any, owner *operationFileOwner) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return errors.New("Host maintenance journal exceeds its bounded record size")
	}
	return atomicWriteStateFileAt(directory, name, data, 0o600, owner)
}

func loadMaintenanceState(directory *os.File) (maintenanceState, error) {
	state := maintenanceState{Version: 1, CompletedRequests: map[string]bool{}, BlockedInstalls: map[string]bool{}}
	err := readMaintenanceJSON(directory, "state.json", &state)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err == nil && state.Version != 1 {
		err = errors.New("unsupported Host maintenance state")
	}
	if state.RegistrationRequests == nil {
		state.RegistrationRequests = map[string]string{}
	}
	if state.RemovedRegistrations == nil {
		state.RemovedRegistrations = map[string]bool{}
	}
	if state.OperationRequests == nil {
		state.OperationRequests = map[string]string{}
	}
	if state.CompletedRequests == nil {
		state.CompletedRequests = map[string]bool{}
	}
	if state.SupersededRequests == nil {
		state.SupersededRequests = map[string]string{}
	}
	if state.BlockedInstalls == nil {
		state.BlockedInstalls = map[string]bool{}
	}
	return state, err
}

func maintenanceReceiptName(id string) string {
	hash := sha256.Sum256([]byte(id))
	return "request-" + hex.EncodeToString(hash[:]) + ".json"
}

func writeOwnerState(directory *os.File, state maintenanceState) error {
	return writeMaintenanceJSON(directory, "state.json", state)
}
func maintenancePaths(value paths) string { return value.applicationData }
func openMaintenanceDirectory(value paths, create bool) (*os.File, error) {
	if create {
		return openApplicationDataDirectory(value, lifecycleOperationOwner(value))
	}
	return openExistingApplicationDataDirectory(value)
}
func lockMaintenance(value paths) (*os.File, func(), error) {
	release, err := acquireOperationLock(value)
	if err != nil {
		return nil, nil, err
	}
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		release()
		return nil, nil, err
	}
	return directory, func() { directory.Close(); release() }, nil
}

var errHostUninstalled = errors.New("the local owner removed this Host installation; start a new authorized setup")

var errMaintenanceOwnerBusy = errors.New("owner_busy: another Host lifecycle operation holds the owner lock")

type ownerBusyError struct{ requestID string }

func (err ownerBusyError) Error() string {
	return "exclusive_transition_in_progress: continue request " + err.requestID
}

func receiptUnfinished(outcome string, retryable bool) bool {
	return outcome == "pending" || outcome == "unknown" || outcome == "failed" && retryable
}

func loadLifecycleReceipt(directory *os.File, id string) (*lifecycleOperation, error) {
	var operation lifecycleOperation
	if err := readMaintenanceJSON(directory, maintenanceReceiptName(id), &operation); err != nil {
		return nil, err
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := readMaintenanceJSON(directory, maintenanceReceiptName(id), &version); err != nil {
		return nil, err
	}
	if version.Version != 0 && version.Version != 1 {
		return nil, errors.New("Host receipt has an unsupported version")
	}
	if operation.OperationID == "" {
		var record struct {
			RequestID, Action, Stage, Outcome, TargetReleaseID, FailureCode, FailureMessage string
			Retryable                                                                       bool
		}
		if err := readMaintenanceJSON(directory, maintenanceReceiptName(id), &record); err != nil {
			return nil, err
		}
		operation = lifecycleOperation{OperationID: id, RequestID: id, Kind: record.Action, Stage: record.Stage, Outcome: record.Outcome, TargetReleaseID: record.TargetReleaseID, FailureCode: record.FailureCode, FailureMessage: record.FailureMessage, Retryable: record.Retryable}
	}
	if operation.RequestID != id || operation.Kind == "" || operation.Outcome == "" {
		return nil, errors.New("Host receipt identity or kind is unreadable")
	}
	return &operation, nil
}

func removeHostData(value paths) error {
	directory, err := openExistingApplicationDataDirectory(value)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()
	root, err := os.OpenRoot(value.applicationData)
	if err != nil {
		return err
	}
	defer root.Close()
	opened, err := directory.Stat()
	if err != nil {
		return err
	}
	rooted, err := root.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(opened, rooted) {
		return errors.New("Host data directory changed before removal")
	}
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "owner.lock" || name == "state.json" || strings.HasPrefix(name, "request-") && strings.HasSuffix(name, ".json") {
			continue
		}
		if err := root.RemoveAll(name); err != nil {
			return err
		}
	}
	return nil
}

func acquireMaintenanceMutation(value paths, kind, requestID string) (func(), error) {
	release, err := acquireOperationLock(value)
	if err != nil {
		return nil, err
	}
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		release()
		return nil, err
	}
	state, err := loadMaintenanceState(directory)
	if err == nil && state.ActiveRequest != "" {
		previous, readErr := loadLifecycleReceipt(directory, state.ActiveRequest)
		if readErr != nil {
			err = readErr
		} else if lifecycleExclusive(*previous) {
			err = ownerBusyError{requestID: state.ActiveRequest}
		} else {
			state.ActiveRequest = ""
			err = writeOwnerState(directory, state)
		}
	}
	directory.Close()
	if err == nil && kind == "install-managed" && state.BlockedInstalls[requestID] {
		err = errors.New("the local owner removed this Host installation; start a new authorized setup")
	}
	if err == nil && state.Uninstalled {
		err = errHostUninstalled
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}
