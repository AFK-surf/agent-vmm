package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"golang.org/x/sys/unix"
)

// This journal is deliberately outside both the replaceable bundle and the
// resettable Host data. It is local operator intent, not cloud authorization.
type maintenanceRequest struct {
	Version             int    `json:"version"`
	RequestID           string `json:"requestId"`
	Action              string `json:"action"`
	DataPolicy          string `json:"dataPolicy"`
	TargetApp           string `json:"targetApp"`
	SourceApp           string `json:"sourceApp,omitempty"`
	TargetReleaseID     string `json:"targetReleaseId,omitempty"`
	ArtifactSHA256      string `json:"artifactSha256,omitempty"`
	ArtifactSize        uint64 `json:"artifactSize,omitempty"`
	SupersedesRequestID string `json:"supersedesRequestId,omitempty"`
	ConfirmSharedHost   bool   `json:"confirmSharedHost"`
	ConfirmDataDeletion bool   `json:"confirmDataDeletion,omitempty"`
}

type maintenanceReceipt struct {
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
	DataCleared                bool        `json:"dataCleared,omitempty"`
	SoftwareRemoved            bool        `json:"softwareRemoved,omitempty"`
	SoftwareInstalled          bool        `json:"softwareInstalled,omitempty"`
	EffectsVerified            bool        `json:"effectsVerified,omitempty"`
	RestoreAttempts            uint64      `json:"restoreAttempts,omitempty"`
	StartedAt                  int64       `json:"startedAt"`
	UpdatedAt                  int64       `json:"updatedAt"`
}

type maintenanceState struct {
	Version            int               `json:"version"`
	ActiveRequest      string            `json:"activeRequest,omitempty"`
	LatestRequest      string            `json:"latestRequest,omitempty"`
	Uninstalled        bool              `json:"uninstalled"`
	CompletedRequests  map[string]bool   `json:"completedRequests,omitempty"`
	SupersededRequests map[string]string `json:"supersededRequests,omitempty"`
	BlockedInstalls    map[string]bool   `json:"blockedInstalls,omitempty"`
}

func maintenancePaths(value paths) string {
	return filepath.Join(value.home, "Library", "Application Support", "Agent VMM Maintenance")
}

func openMaintenanceDirectory(value paths, create bool) (*os.File, error) {
	home, err := openExistingServiceStateDirectory(value.home, nil)
	if err != nil {
		return nil, err
	}
	defer home.Close()
	current := home
	for _, name := range []string{"Library", "Application Support", "Agent VMM Maintenance"} {
		var next *os.File
		if create {
			next, err = openOrCreateAncestorDirectoryAt(current, name)
		} else {
			next, err = openExistingStateDirectoryAt(current, name)
		}
		if current != home {
			current.Close()
		}
		if err != nil {
			return nil, err
		}
		current = next
	}
	return current, nil
}

// errMaintenanceOwnerBusy means a live executor holds owner.lock.
var errMaintenanceOwnerBusy = errors.New("owner_busy: another Host lifecycle or maintenance operation holds the owner lock")

func lockMaintenance(value paths) (*os.File, func(), error) {
	directory, err := openMaintenanceDirectory(value, true)
	if err != nil {
		return nil, nil, err
	}
	file, err := openStateFileAt(directory, "owner.lock", unix.O_RDWR|unix.O_CREAT|unix.O_NONBLOCK, 0o600, nil)
	if err != nil {
		directory.Close()
		return nil, nil, err
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if (!errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN)) || time.Now().After(deadline) {
			file.Close()
			directory.Close()
			if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
				return nil, nil, errMaintenanceOwnerBusy
			}
			return nil, nil, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return directory, func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN); file.Close(); directory.Close() }, nil
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
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return errors.New("Host maintenance journal exceeds its bounded record size")
	}
	return atomicWriteStateFileAt(directory, name, data, 0o600, nil)
}

func loadMaintenanceState(directory *os.File) (maintenanceState, error) {
	state := maintenanceState{Version: 1, CompletedRequests: map[string]bool{}, BlockedInstalls: map[string]bool{}}
	err := readMaintenanceJSON(directory, "state.json", &state)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err == nil && state.Version != 1 {
		err = errors.New("unsupported Host maintenance state")
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

func acquireMaintenanceMutation(value paths, kind, requestID string) (func(), error) {
	directory, release, err := lockMaintenance(value)
	if err != nil {
		return nil, err
	}
	state, err := loadMaintenanceState(directory)
	if err == nil && state.ActiveRequest != "" {
		err = errors.New("continue the pending local Host maintenance request first")
	}
	if err == nil && state.Uninstalled {
		err = errors.New("the local owner uninstalled this Host; explicitly reinstall it through Host maintenance")
	}
	if err == nil && kind == "install-managed" && state.BlockedInstalls[requestID] {
		err = errors.New("this old installation was removed by the local owner; start a new authorized setup")
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
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
		if request.SourceApp != "" || request.TargetReleaseID != "" || request.ArtifactSHA256 != "" || request.ArtifactSize != 0 {
			return errors.New("uninstall does not accept an installation artifact")
		}
	} else {
		digest, err := hex.DecodeString(request.ArtifactSHA256)
		if err != nil || len(digest) != 32 || strings.ToLower(request.ArtifactSHA256) != request.ArtifactSHA256 || request.ArtifactSize == 0 || request.ArtifactSize > 2<<30 || request.TargetReleaseID == "" {
			return errors.New("maintenance requires the selected release, archive SHA-256 and bounded size")
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
	validateSource    func(maintenanceRequest) error
	targetSchema      func(maintenanceRequest) (store.SchemaRange, error)
	dataSchema        func(paths) (int, error)
	validateInstalled func(paths, maintenanceRequest) error
	commitState       func(*os.File, maintenanceState) error
	commitAdmission   func(*os.File, maintenanceState) error
	stop              func(paths) error
	removeSoftware    func(paths) error
	resetData         func(paths) error
	replace           func(paths, maintenanceRequest) error
	restore           func(paths, string) error
	verify            func(paths, maintenanceRequest) error
}

const (
	maintenanceTargetOlderThanData = "target_older_than_data"
	maintenanceHostStartFailed     = "host_start_failed"
	// Each restore attempt waits up to defaultCoordinatorInitTimeout (10 min)
	// for a healthy VM, which already covers the Host's 10 min installed-VM
	// resume. Three attempts allow two retries after a transient launchd or VM
	// start failure; a Host that misses three full budgets needs an operator.
	maintenanceRestoreAttempts = 3
)

// maintenanceFailure is a failure that a retry of the same request cannot fix.
type maintenanceFailure struct {
	code string
	err  error
}

func (failure maintenanceFailure) Error() string { return failure.err.Error() }

// targetSupportedSchema asks the verified source bundle's own helper, because
// that bundle replaces the installed Host and must open the existing data.
func targetSupportedSchema(request maintenanceRequest) (store.SchemaRange, error) {
	output, err := exec.Command(filepath.Join(request.SourceApp, "Contents", "Helpers", "agent-vmm-lifecycle"), "version", "--json").Output()
	if err != nil {
		return store.SchemaRange{}, fmt.Errorf("target Host version is unreadable: %w", err)
	}
	var version struct {
		SupportedSchema *store.SchemaRange `json:"supportedSchema"`
	}
	if err := json.NewDecoder(bytes.NewReader(output)).Decode(&version); err != nil {
		return store.SchemaRange{}, errors.New("target Host version is unreadable")
	}
	// A helper without the field predates this check and cannot prove that it
	// opens the existing data.
	if version.SupportedSchema == nil {
		return store.SchemaRange{}, nil
	}
	return *version.SupportedSchema, nil
}

// restoreHost starts the replaced Host and its VM. A Host start failure record
// ends the wait at once, because launchd does not restart that Host.
func restoreHost(value paths, id string, repair func(paths) error, ensureVM func(context.Context, paths, string) error) error {
	// The record belongs to an earlier Host start. Remove it before this start
	// so that only the new Host can report a refusal.
	if err := os.Remove(filepath.Join(value.applicationData, store.StartFailureFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := repair(value); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultCoordinatorInitTimeout)
	defer cancel()
	go func() {
		for ctx.Err() == nil && readHostStartFailure(value) == nil {
			time.Sleep(250 * time.Millisecond)
		}
		cancel()
	}()
	err := ensureVM(ctx, value, id)
	if failure := readHostStartFailure(value); failure != nil {
		return maintenanceFailure{code: maintenanceHostStartFailed, err: fmt.Errorf("Host refused its database: %s (data schema %d, supported %d-%d)",
			failure.Code, failure.DataSchema, failure.SupportedSchema.Min, failure.SupportedSchema.Max)}
	}
	return err
}

func defaultMaintenanceActions() maintenanceActions {
	return maintenanceActions{
		validateInstalled: func(value paths, request maintenanceRequest) error {
			id, err := hostBundleReleaseID(value.app, false)
			if err != nil || id != request.TargetReleaseID {
				return errors.New("selected installed bundle is unavailable")
			}
			return exec.Command("/usr/bin/codesign", "--verify", "--deep", "--strict", value.app).Run()
		},
		validateSource: func(request maintenanceRequest) error {
			canonical, err := filepath.EvalSymlinks(request.SourceApp)
			if err != nil || canonical != request.SourceApp {
				return errors.New("maintenance source must be a canonical bundle path without symlinks")
			}
			id, err := hostBundleReleaseID(request.SourceApp, false)
			if err != nil || id != request.TargetReleaseID {
				return errors.New("verified source bundle does not match the selected release")
			}
			return exec.Command("/usr/bin/codesign", "--verify", "--deep", "--strict", request.SourceApp).Run()
		},
		targetSchema: targetSupportedSchema,
		dataSchema: func(value paths) (int, error) {
			return store.ReadSchemaVersion(filepath.Join(value.applicationData, "host.sqlite"))
		},
		stop: stopForMaintenance,
		removeSoftware: func(value paths) error {
			for _, path := range []string{value.agentPlist, value.daemonPlist} {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			return os.RemoveAll(value.app)
		},
		resetData: func(value paths) error {
			if err := os.RemoveAll(value.applicationData); err != nil {
				return err
			}
			return os.RemoveAll(value.logs)
		},
		replace: func(value paths, request maintenanceRequest) error {
			stage := filepath.Join(filepath.Dir(value.app), ".Agent VMM Host.maintenance-"+request.RequestID+".app")
			if err := os.RemoveAll(stage); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(value.app), 0o700); err != nil {
				return err
			}
			if err := exec.Command("/usr/bin/ditto", request.SourceApp, stage).Run(); err != nil {
				return err
			}
			// The stage is a copy of the verified source; check only its release.
			id, err := hostBundleReleaseID(stage, true)
			if err != nil || id != request.TargetReleaseID {
				return errors.New("prepared replacement does not match selected release")
			}
			if err := atomicReplaceApp(stage, value.app); err != nil {
				return err
			}
			return os.RemoveAll(stage)
		},
		restore: func(value paths, id string) error {
			value.resources = filepath.Join(value.app, "Contents", "Resources")
			return restoreHost(value, id, repair, ensureVMStartedContext)
		},
		verify: func(value paths, request maintenanceRequest) error {
			if request.Action == "uninstall" {
				for _, path := range []string{value.app, value.agentPlist, value.daemonPlist} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						return errors.New("Host software removal is not confirmed")
					}
				}
				if request.DataPolicy == "reset" {
					for _, path := range []string{value.applicationData, value.logs} {
						if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
							return errors.New("Host data deletion is not confirmed")
						}
					}
				}
				return nil
			}
			id, err := hostBundleReleaseID(value.app, false)
			if err != nil || id != request.TargetReleaseID {
				return errors.New("installed Host release is not confirmed")
			}
			facts := readStatus(value)
			if !facts.HostRunning || !facts.HostHealthy {
				return errors.New("installed Host readiness is not confirmed")
			}
			return nil
		},
	}
}

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
		if !reflect.DeepEqual(receipt.maintenanceRequest, request) || receipt.ServiceMode != value.service.Mode || receipt.ServiceUser != value.service.User {
			return receipt, errors.New("maintenance requestId belongs to a different action, data policy, service or artifact")
		}
		if next := state.SupersededRequests[request.RequestID]; next != "" {
			return supersededMaintenanceReceipt(receipt, next), nil
		}
		if state.CompletedRequests[request.RequestID] {
			return completedMaintenanceReceipt(receipt), nil
		}
		if receipt.FailureCode == maintenanceTargetOlderThanData {
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
	// Fence old helpers that predate the outer maintenance lock, then save the
	// accepted request before destructive effects. The outer lock survives reset.
	if existing, statErr := openExistingApplicationDataDirectory(value); statErr == nil {
		existing.Close()
		legacyRelease, lockErr := acquireOperationLock(value)
		if lockErr != nil {
			return receipt, lockErr
		}
		defer legacyRelease()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return receipt, statErr
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
	commitAdmission := actions.commitAdmission
	if commitAdmission == nil {
		commitAdmission = func(dir *os.File, state maintenanceState) error {
			return writeMaintenanceJSON(dir, "state.json", state)
		}
	}
	if err = commitAdmission(directory, state); err != nil {
		return receipt, err
	}
	step := func(stage string, action func() error) error {
		receipt.Stage, receipt.Outcome, receipt.Retryable = stage, "pending", true
		receipt.FailureCode, receipt.FailureMessage = "", ""
		receipt.UpdatedAt = time.Now().UnixMilli()
		if saveErr := writeMaintenanceJSON(directory, name, receipt); saveErr != nil {
			return saveErr
		}
		return action()
	}
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
		}
	}()
	if receipt.SoftwareInstalled && actions.validateInstalled != nil && actions.validateInstalled(value, request) != nil {
		receipt.SoftwareInstalled = false
		if err = writeMaintenanceJSON(directory, name, receipt); err != nil {
			return receipt, err
		}
	}
	// After cutover, retries only restore and verify. Stopping a successful VM
	// again would invalidate its already-completed StartVM receipt.
	if !receipt.SoftwareInstalled {
		if err = step("stopping", func() error { return actions.stop(value) }); err != nil {
			return receipt, err
		}
	}
	if request.DataPolicy == "reset" && !receipt.DataCleared {
		if err = step("resetting", func() error { return actions.resetData(value) }); err != nil {
			return receipt, err
		}
		receipt.DataCleared = true
		if err = writeMaintenanceJSON(directory, name, receipt); err != nil {
			return receipt, err
		}
	}
	if request.Action == "uninstall" {
		if err = step("removing", func() error { return actions.removeSoftware(value) }); err != nil {
			return receipt, err
		}
		receipt.SoftwareRemoved = true
	} else {
		if !receipt.SoftwareInstalled {
			if err = step("replacing", func() error { return actions.replace(value, request) }); err != nil {
				return receipt, err
			}
			receipt.SoftwareInstalled = true
			if err = writeMaintenanceJSON(directory, name, receipt); err != nil {
				return receipt, err
			}
		}
		exhausted := receipt.RestoreAttempts >= maintenanceRestoreAttempts
		if !exhausted {
			receipt.RestoreAttempts++
		}
		if err = step("restoring", func() error {
			if exhausted {
				return maintenanceFailure{code: maintenanceHostStartFailed, err: fmt.Errorf("Host did not start after %d restore attempts", maintenanceRestoreAttempts)}
			}
			err := actions.restore(value, maintenanceRestoreRequestID(request.RequestID, receipt.RestoreAttempts))
			if err != nil && receipt.RestoreAttempts == maintenanceRestoreAttempts && !errors.As(err, new(maintenanceFailure)) {
				err = maintenanceFailure{code: maintenanceHostStartFailed, err: fmt.Errorf("Host did not start after %d restore attempts: %w", maintenanceRestoreAttempts, err)}
			}
			return err
		}); err != nil {
			return receipt, err
		}
	}
	if err = step("verifying", func() error { return actions.verify(value, request) }); err != nil {
		return receipt, err
	}
	receipt.EffectsVerified = true
	if err = writeMaintenanceJSON(directory, name, receipt); err != nil {
		return receipt, err
	}
	return settleMaintenance(directory, name, state, receipt, actions.commitState)
}

// maintenanceSchemaRefusal returns a refusal message when the target does not
// open the existing local database.
func maintenanceSchemaRefusal(value paths, request maintenanceRequest, actions maintenanceActions) (string, error) {
	data, err := actions.dataSchema(value)
	if err != nil {
		return "", fmt.Errorf("local Host database schema is unreadable: %w", err)
	}
	if data == 0 {
		return "", nil
	}
	target, err := actions.targetSchema(request)
	if err != nil {
		return "", err
	}
	if data >= target.Min && data <= target.Max {
		return "", nil
	}
	if target.Max == 0 {
		return fmt.Sprintf("The selected Host release does not report the database schemas it opens, and the local data uses schema %d. Keep the current Host or confirm a reset reinstall.", data), nil
	}
	return fmt.Sprintf("The selected Host release opens database schemas %d-%d, but the local data uses schema %d. Keep the current Host or confirm a reset reinstall.", target.Min, target.Max, data), nil
}

// refuseMaintenance records a terminal refusal that made no effects. It does
// not change the owner state, so the current Host, an active request it named,
// and a new request stay as they were.
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
	if previous.Version != 1 || previous.RequestID != previousID || (previous.Outcome != "failed" && previous.Outcome != "pending") || previous.TargetApp != request.TargetApp || previous.ServiceMode != value.service.Mode || previous.ServiceUser != value.service.User {
		return errors.New("replacement requires a pending or failed receipt for the same Host and service")
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
	if receipt.Version != 1 || receipt.RequestID != id {
		return nil, errors.New("Host maintenance receipt has an unsupported identity or version")
	}
	if next := state.SupersededRequests[id]; next != "" {
		receipt = supersededMaintenanceReceipt(receipt, next)
	} else if state.CompletedRequests[id] {
		receipt = completedMaintenanceReceipt(receipt)
	} else if receipt.Outcome == "succeeded" {
		return nil, errors.New("maintenance completion is not committed")
	} else if receipt.FailureCode != maintenanceTargetOlderThanData && state.ActiveRequest != id {
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

func executeMaintenanceCommand(command string, service serviceRequest, args []string) {
	if len(args) != 0 || service.app != "" {
		fatal("maintenance commands accept a JSON request on stdin and service identity options only")
	}
	account, err := lookupServiceAccount(service.user)
	if err != nil {
		fatal("service user is unavailable")
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil || uid == 0 || (os.Geteuid() != 0 && uid != os.Geteuid()) {
		fatal("maintenance requires the local service user")
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 16385))
	decoder.DisallowUnknownFields()
	if command == "maintenance-status" {
		var request struct {
			RequestID string `json:"requestId,omitempty"`
		}
		if err := decoder.Decode(&request); err != nil && err != io.EOF {
			fatal("invalid maintenance status request")
		}
		if decoder.Decode(&struct{}{}) != io.EOF {
			fatal("invalid maintenance status request")
		}
		result, err := maintenanceStatus(paths{home: account.HomeDir}, request.RequestID)
		if err != nil {
			fatal(err.Error())
		}
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return
	}
	var request maintenanceRequest
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		fatal("invalid maintenance request")
	}
	saved, err := maintenanceServiceReceipt(paths{home: account.HomeDir}, request)
	if err != nil {
		fatal(err.Error())
	}
	// A reset may have removed service.json. Resume the original service mode
	// and pairing policy from the outside journal, rather than choosing anew.
	if saved != nil && (service.mode == "" || service.mode == serviceModeAuto) {
		service.mode = saved.ServiceMode
	}
	config, err := resolveServiceConfig(service)
	if err != nil {
		fatal(err.Error())
	}
	if saved != nil {
		config.DisablePairing = saved.DisablePairing
	}
	if config.UID != os.Geteuid() {
		fatal("maintenance must run as the configured non-root service user")
	}
	executable, err := os.Executable()
	if err != nil {
		fatal(err.Error())
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		fatal(err.Error())
	}
	target := filepath.Join(account.HomeDir, "Library", "Application Support", "Agent VMM Host", "current", bundle.HostAppName)
	value, err := observationPathsForConfig(account.HomeDir, executable, config, target)
	if err != nil {
		fatal(err.Error())
	}
	result, err := runMaintenance(value, request, defaultMaintenanceActions())
	if errors.Is(err, errMaintenanceOwnerBusy) {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"error": map[string]string{"code": "owner_busy", "message": err.Error()}})
		fatal(fmt.Sprintf("Host maintenance: %v", err))
	}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if err != nil {
		fatal(fmt.Sprintf("Host maintenance: %v", err))
	}
}

// Refuse symlink ancestors before any removal. The shared service home is the
// trusted root; missing descendants are valid for bootstrap and retries.
func validateMaintenancePath(home, target string) error {
	relative, err := filepath.Rel(home, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, "../") {
		return errors.New("maintenance target is outside the service home")
	}
	current := home
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("maintenance target contains a symlink")
		}
	}
	return nil
}

// Called inside the outer lifecycle lock. A different retry request ID must
// not turn an old managed installation into a new enrollment.
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

func maintenanceRestoreRequestID(id string, attempt uint64) string {
	hash := sha256.Sum256([]byte(id))
	return fmt.Sprintf("maintenance-restore-%x-%d", hash[:16], attempt)
}

func completedMaintenanceReceipt(receipt maintenanceReceipt) maintenanceReceipt {
	receipt.Stage, receipt.Outcome, receipt.Retryable = "complete", "succeeded", false
	receipt.FailureCode, receipt.FailureMessage = "", ""
	return receipt
}

// The state write is the completion commit. Status can project it after a crash
// without mutating the journal; it never trusts an uncommitted success receipt.
func settleMaintenance(directory *os.File, name string, state maintenanceState, receipt maintenanceReceipt, commit func(*os.File, maintenanceState) error) (maintenanceReceipt, error) {
	state.ActiveRequest, state.Uninstalled = "", receipt.Action == "uninstall"
	state.CompletedRequests[receipt.RequestID] = true
	if commit == nil {
		commit = func(dir *os.File, value maintenanceState) error {
			return writeMaintenanceJSON(dir, "state.json", value)
		}
	}
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
