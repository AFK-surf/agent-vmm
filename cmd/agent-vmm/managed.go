//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

const (
	managedUsage             = "usage: agent-vmm inspect|diagnose|logs|operation|repair|update ... | agent-vmm vm start|status|stop ... | agent-vmm cleanup preview|execute ... | agent-vmm appliance install|update|status|recover|stop|start|activate|restart|revoke|remove ..."
	managedEnvelopeVersion   = 1
	managedObservationBudget = 30 * time.Second
	managedPollInterval      = 2 * time.Second
	managedLogBytes          = 1 << 20
	managedDiagnoseBytes     = 2 << 20
	managedDefaultListLimit  = 50
	managedMaxListLimit      = 100
	managedUpdateRetryDelay  = 30 * time.Second

	managedDispositionContinueNormalWork     = "continue_normal_work"
	managedDispositionExclusiveTransition    = "exclusive_transition_in_progress"
	managedDispositionRetrySameRequestAt     = "retry_same_request_at"
	managedDispositionAdministratorAction    = "administrator_action_required"
	managedDispositionTerminalActionRequired = "terminal_action_required"
)

type managedRuntime struct {
	now              func() time.Time
	runHelper        func(context.Context, string, []string) ([]byte, []byte, error)
	runHelperInput   func(context.Context, string, []string, []byte) ([]byte, []byte, error)
	getOperation     func(context.Context, string, string) (*hostv1.Operation, error)
	getEnvironment   func(context.Context, string, string) (*hostv1.AgentEnvironment, error)
	getRegistration  func(context.Context, string, string) (*hostv1.RemoteRegistration, error)
	getImpactSummary func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error)
	getVM            func(context.Context, string) (*hostv1.VM, error)
	startVM          func(context.Context, string, string, *hostv1.VMMutationFence) (*hostv1.Operation, error)
	stopVM           func(context.Context, string, string, bool, *hostv1.VMMutationFence) (*hostv1.Operation, error)
	planCleanup      func(context.Context, string, string, hostv1.CleanupKind) (*hostv1.CleanupPlan, error)
	executeCleanup   func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error)
	prepareUpdate    func(context.Context, managedOptions, map[string]any) (string, error)
	removePrepared   func(string) error
	readLogs         func(context.Context, string, string, string, string, int, bool) ([]managedLogRecord, string, bool, error)
	sleep            func(context.Context, time.Duration) error
}

type managedOptions struct {
	json           bool
	serviceType    string
	serviceUser    string
	serviceApp     string
	lifecycle      string
	operatorSocket string
	registrationID string
	environmentID  string
	workloadID     string
	containerID    string
	component      string
	owner          string
	operationID    string
	afterCursor    string
	limit          int
	follow         bool
	includeContent bool
	requestID      string
	force          bool
	cleanupKind    string
	planRevision   uint64
	confirmVolumes managedStringList
	confirmCleanup bool
	artifactURL    string
	artifactSHA256 string
	artifactSize   uint64
	targetRelease  string
}

type managedStringList []string

func (values *managedStringList) String() string { return strings.Join(*values, ",") }
func (values *managedStringList) Set(value string) error {
	if !managedIdentifierPattern.MatchString(value) {
		return errors.New("value is not a valid identifier")
	}
	for _, current := range *values {
		if current == value {
			return errors.New("value was repeated")
		}
	}
	*values = append(*values, value)
	return nil
}

type managedEnvelope struct {
	Version      int                 `json:"version"`
	Command      string              `json:"command"`
	Target       map[string]any      `json:"target"`
	Source       []string            `json:"source"`
	ObservedAt   string              `json:"observed_at"`
	Freshness    string              `json:"freshness"`
	State        string              `json:"state"`
	Stage        string              `json:"stage"`
	Disposition  string              `json:"disposition,omitempty"`
	Issue        any                 `json:"issue"`
	OperationRef any                 `json:"operation_ref"`
	NextAction   []managedNextAction `json:"next_action"`
	Partial      bool                `json:"partial"`
	Cursor       string              `json:"cursor,omitempty"`
	Gap          bool                `json:"gap,omitempty"`
	Facts        map[string]any      `json:"facts,omitempty"`
	Logs         []managedLogRecord  `json:"logs,omitempty"`
}

type managedNextAction struct {
	Action     string `json:"action"`
	Permission string `json:"permission"`
}

type managedIssue struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Layer     string `json:"layer"`
	Retryable *bool  `json:"retryable"`
}

type managedLogRecord struct {
	Cursor    string `json:"cursor"`
	Timestamp string `json:"timestamp,omitempty"`
	Stream    string `json:"stream"`
	Data      string `json:"data"`
	Truncated bool   `json:"truncated,omitempty"`
	Dropped   uint64 `json:"dropped_records,omitempty"`
}

func writeManagedEnvelope(writer io.Writer, envelope managedEnvelope, jsonOutput bool, command string) error {
	envelope.Facts = redactManagedMap(envelope.Facts)
	for index := range envelope.Logs {
		envelope.Logs[index].Data = redactManagedText(envelope.Logs[index].Data)
	}
	if issue, ok := envelope.Issue.(managedIssue); ok {
		issue.Message = redactManagedText(issue.Message)
		envelope.Issue = issue
	}
	if command == "logs" {
		envelope = fitManagedLogEnvelope(envelope)
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	if command == "diagnose" && len(data)+1 > managedDiagnoseBytes {
		return errors.New("diagnostic output exceeded 2 MiB")
	}
	if command == "logs" && len(data)+1 > managedLogBytes {
		return errors.New("log output exceeded 1 MiB")
	}
	if jsonOutput {
		_, err = fmt.Fprintln(writer, string(data))
		return err
	}
	if _, err = fmt.Fprintf(writer, "%s: %s", command, envelope.State); err != nil {
		return err
	}
	if envelope.Stage != "unknown" {
		if _, err = fmt.Fprintf(writer, " (%s)", envelope.Stage); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(writer)
	return err
}

func managedHelp(command string) string {
	switch command {
	case "inspect":
		return "usage: agent-vmm inspect [--service-type agent|daemon|auto] [--service-user USER] [--registration-id ID] [--environment-id ID] [--workload-id ID] [--json]\n"
	case "diagnose":
		return "usage: agent-vmm diagnose [inspect options] [--json]\n"
	case "logs":
		return "usage: agent-vmm logs (--component host|guest [--operation-id ID] | --environment-id ID --container-id ID [--include-container-output]) [--after-cursor CURSOR] [--limit 1..100] [--json]\n"
	case "operation":
		return "usage: agent-vmm operation get|follow --owner lifecycle|host --operation-id ID [--json]\n"
	case "vm":
		return "usage: agent-vmm vm status [--operator-socket PATH] [--json] | agent-vmm vm start|stop --request-id ID [--operator-socket PATH] [--force (stop only)] [--json]\nvm start and stop acknowledge an action on the whole VM, including workloads that are not individually listed.\n"
	case "repair":
		return "usage: agent-vmm repair --request-id ID [--registration-id ID] [--json]\n"
	case "update":
		return "usage: agent-vmm update --request-id ID --target-release-id ID --artifact-url HTTPS_URL --artifact-sha256 HEX --artifact-size BYTES [--service-type agent|daemon|auto] [--service-user USER] [--json]\n"
	case "cleanup":
		return "usage: agent-vmm cleanup preview --environment-id ID --kind reset|discard [--operator-socket PATH] [--json] | agent-vmm cleanup execute --environment-id ID --kind reset|discard --plan-revision REVISION --request-id ID [--confirm-volume ID ...] [--confirm-irreversible (discard only)] [--operator-socket PATH] [--json]\n"
	default:
		return managedUsage + "\n"
	}
}

func (runtime *managedRuntime) defaults() {
	if runtime.now == nil {
		runtime.now = time.Now
	}
	if runtime.runHelper == nil {
		runtime.runHelper = runManagedHelper
	}
	if runtime.runHelperInput == nil {
		runtime.runHelperInput = runManagedHelperInput
	}
	if runtime.getOperation == nil {
		runtime.getOperation = getManagedOperation
	}
	if runtime.getEnvironment == nil {
		runtime.getEnvironment = getManagedEnvironment
	}
	if runtime.getRegistration == nil {
		runtime.getRegistration = getManagedRegistration
	}
	if runtime.readLogs == nil {
		runtime.readLogs = readManagedContainerLogs
	}
	if runtime.getImpactSummary == nil {
		runtime.getImpactSummary = getManagedImpactSummary
	}
	if runtime.getVM == nil {
		runtime.getVM = getManagedVM
	}
	if runtime.startVM == nil {
		runtime.startVM = startManagedVM
	}
	if runtime.stopVM == nil {
		runtime.stopVM = stopManagedVM
	}
	if runtime.planCleanup == nil {
		runtime.planCleanup = planManagedCleanup
	}
	if runtime.executeCleanup == nil {
		runtime.executeCleanup = executeManagedCleanup
	}
	if runtime.prepareUpdate == nil {
		runtime.prepareUpdate = prepareManagedUpdate
	}
	if runtime.removePrepared == nil {
		runtime.removePrepared = os.RemoveAll
	}
	if runtime.sleep == nil {
		runtime.sleep = func(ctx context.Context, duration time.Duration) error {
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
}
