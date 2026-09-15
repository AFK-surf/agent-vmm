//go:build darwin

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"golang.org/x/sys/unix"
)

const (
	managedUsage            = "usage: agent-vmm inspect|diagnose|logs|operation|repair|update ... | agent-vmm vm start|status|stop ... | agent-vmm cleanup preview|execute ..."
	managedEnvelopeVersion  = 1
	managedInspectBudget    = 5 * time.Second
	managedDiagnoseBudget   = 30 * time.Second
	managedFollowBudget     = 60 * time.Second
	managedMutationBudget   = 2 * time.Minute
	managedUpdateBudget     = 20 * time.Minute
	managedPollInterval     = 2 * time.Second
	managedLogBytes         = 1 << 20
	managedDiagnoseBytes    = 2 << 20
	managedDefaultListLimit = 50
	managedMaxListLimit     = 100
	managedUpdateRetryDelay = 30 * time.Second

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

func isManagedCommand(command string) bool {
	switch command {
	case "inspect", "diagnose", "logs", "operation", "repair", "update", "vm", "cleanup":
		return true
	default:
		return false
	}
}

func runManagedCLI(arguments []string, stdout, stderr io.Writer, runtime managedRuntime) int {
	if len(arguments) == 0 || !isManagedCommand(arguments[0]) {
		fmt.Fprintln(stderr, managedUsage)
		return 2
	}
	runtime.defaults()
	command := arguments[0]
	subcommand := ""
	remaining := arguments[1:]
	if len(remaining) == 1 && (remaining[0] == "--help" || remaining[0] == "-h") {
		fmt.Fprint(stdout, managedHelp(command))
		return 0
	}
	if command == "operation" {
		if len(remaining) == 0 || remaining[0] != "get" && remaining[0] != "follow" {
			writeManagedError(stderr, command, "invalid_arguments", "operation requires get or follow")
			return 2
		}
		subcommand, remaining = remaining[0], remaining[1:]
		if len(remaining) == 1 && (remaining[0] == "--help" || remaining[0] == "-h") {
			fmt.Fprint(stdout, managedHelp(command))
			return 0
		}
	}
	if command == "vm" {
		if len(remaining) == 0 || remaining[0] != "start" && remaining[0] != "status" && remaining[0] != "stop" {
			writeManagedError(stderr, command, "invalid_arguments", "vm requires start, status, or stop")
			return 2
		}
		subcommand, remaining = remaining[0], remaining[1:]
	}
	if command == "cleanup" {
		if len(remaining) == 0 || remaining[0] != "preview" && remaining[0] != "execute" {
			writeManagedError(stderr, command, "invalid_arguments", "cleanup requires preview or execute")
			return 2
		}
		subcommand, remaining = remaining[0], remaining[1:]
	}
	opts, err := parseManagedOptions(command, subcommand, remaining)
	if err != nil {
		writeManagedError(stderr, command, "invalid_arguments", err.Error())
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), managedInspectBudget)
	if command == "diagnose" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), managedDiagnoseBudget)
	} else if command == "operation" && subcommand == "follow" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), managedFollowBudget)
	} else if command == "update" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), managedUpdateBudget)
	} else if command == "vm" && subcommand != "status" || command == "repair" || command == "cleanup" && subcommand == "execute" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), managedMutationBudget)
	}
	defer cancel()

	var envelopes []managedEnvelope
	switch command {
	case "inspect", "diagnose":
		envelope, runErr := runtime.inspect(ctx, command, opts)
		if runErr != nil {
			writeManagedError(stderr, command, "observation_failed", runErr.Error())
			return 1
		}
		envelopes = []managedEnvelope{envelope}
	case "logs":
		envelope, runErr := runtime.logs(ctx, opts)
		if runErr != nil {
			writeManagedError(stderr, command, "log_read_failed", runErr.Error())
			return 1
		}
		envelopes = []managedEnvelope{envelope}
	case "operation":
		if subcommand == "follow" {
			_, err = runtime.operation(ctx, subcommand, opts, func(envelope managedEnvelope) error {
				return writeManagedEnvelope(stdout, envelope, true, command)
			})
			if err != nil {
				writeManagedError(stderr, command, "operation_observation_failed", err.Error())
				return 1
			}
			return 0
		}
		envelopes, err = runtime.operation(ctx, subcommand, opts, nil)
		if err != nil {
			writeManagedError(stderr, command, "operation_observation_failed", err.Error())
			return 1
		}
	case "vm":
		envelope, runErr := runtime.vm(ctx, subcommand, opts)
		if runErr != nil {
			writeManagedError(stderr, command, "vm_operation_failed", runErr.Error())
			return 1
		}
		envelopes = []managedEnvelope{envelope}
	case "repair":
		envelope, runErr := runtime.repair(ctx, opts)
		if runErr != nil {
			writeManagedError(stderr, command, "repair_failed", runErr.Error())
			return 1
		}
		envelopes = []managedEnvelope{envelope}
	case "update":
		envelope, runErr := runtime.update(ctx, opts)
		if runErr != nil {
			writeManagedErrorAtStage(stderr, command, "update_failed", runErr.Error(), managedUpdateFailureStage(runErr))
			return 1
		}
		envelopes = []managedEnvelope{envelope}
	case "cleanup":
		envelope, runErr := runtime.cleanup(ctx, subcommand, opts)
		if runErr != nil {
			writeManagedError(stderr, command, "cleanup_failed", runErr.Error())
			return 1
		}
		envelopes = []managedEnvelope{envelope}
	}
	for _, envelope := range envelopes {
		if err := writeManagedEnvelope(stdout, envelope, opts.json, command); err != nil {
			writeManagedError(stderr, command, "encoding_failed", err.Error())
			return 1
		}
	}
	return 0
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

func parseManagedOptions(command, subcommand string, arguments []string) (managedOptions, error) {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var opts managedOptions
	flags.BoolVar(&opts.json, "json", false, "write JSON")
	flags.StringVar(&opts.serviceType, "service-type", "", "agent, daemon, or auto")
	flags.StringVar(&opts.serviceUser, "service-user", "", "service user")
	flags.StringVar(&opts.serviceApp, "service-app", "", "installed Agent VMM app")
	flags.StringVar(&opts.lifecycle, "lifecycle-helper", "", "lifecycle helper")
	flags.StringVar(&opts.operatorSocket, "operator-socket", "", "operator socket")
	flags.StringVar(&opts.registrationID, "registration-id", "", "registration id")
	flags.StringVar(&opts.environmentID, "environment-id", "", "environment id")
	flags.StringVar(&opts.workloadID, "workload-id", "", "workload id")
	flags.StringVar(&opts.containerID, "container-id", "", "container id")
	flags.StringVar(&opts.component, "component", "", "host or guest")
	flags.StringVar(&opts.owner, "owner", "", "lifecycle or host")
	flags.StringVar(&opts.operationID, "operation-id", "", "operation id")
	flags.StringVar(&opts.afterCursor, "after-cursor", "", "opaque log cursor")
	flags.IntVar(&opts.limit, "limit", managedDefaultListLimit, "record limit")
	flags.BoolVar(&opts.follow, "follow", false, "follow logs")
	flags.BoolVar(&opts.includeContent, "include-container-output", false, "include redacted container output")
	flags.StringVar(&opts.requestID, "request-id", "", "stable request id")
	flags.BoolVar(&opts.force, "force", false, "force VM stop")
	flags.StringVar(&opts.cleanupKind, "kind", "", "reset or discard")
	flags.Uint64Var(&opts.planRevision, "plan-revision", 0, "cleanup plan revision")
	flags.Var(&opts.confirmVolumes, "confirm-volume", "exact named volume id; repeat for each volume")
	flags.BoolVar(&opts.confirmCleanup, "confirm-irreversible", false, "confirm irreversible discard")
	flags.StringVar(&opts.artifactURL, "artifact-url", "", "exact HTTPS Host artifact URL")
	flags.StringVar(&opts.artifactSHA256, "artifact-sha256", "", "exact Host artifact SHA-256")
	flags.Uint64Var(&opts.artifactSize, "artifact-size", 0, "exact Host artifact size")
	flags.StringVar(&opts.targetRelease, "target-release-id", "", "selected Host release id")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return managedOptions{}, errors.New("unknown or incomplete argument")
	}
	if opts.limit < 1 || opts.limit > managedMaxListLimit {
		return managedOptions{}, errors.New("--limit must be between 1 and 100")
	}
	if opts.serviceType != "" && opts.serviceType != "agent" && opts.serviceType != "daemon" && opts.serviceType != "auto" {
		return managedOptions{}, errors.New("--service-type must be agent, daemon, or auto")
	}
	for name, value := range map[string]string{"registration-id": opts.registrationID, "environment-id": opts.environmentID, "workload-id": opts.workloadID, "container-id": opts.containerID, "operation-id": opts.operationID, "request-id": opts.requestID} {
		if value != "" && !managedIdentifierPattern.MatchString(value) {
			return managedOptions{}, fmt.Errorf("--%s is invalid", name)
		}
	}
	if len(opts.afterCursor) > 4096 {
		return managedOptions{}, errors.New("--after-cursor exceeds 4096 bytes")
	}
	if command == "logs" {
		if hasManagedUpdateOptions(opts) {
			return managedOptions{}, errors.New("logs received an update option")
		}
		local := opts.component == "host" || opts.component == "guest"
		container := opts.environmentID != "" && opts.containerID != ""
		if local == container || opts.component != "" && !local {
			return managedOptions{}, errors.New("logs requires either --component host|guest or both --environment-id and --container-id")
		}
		if opts.follow {
			return managedOptions{}, errors.New("logs does not support --follow; repeat the bounded read with --after-cursor")
		}
		if container && opts.operationID != "" {
			return managedOptions{}, errors.New("container logs do not accept --operation-id")
		}
		if local && opts.includeContent {
			return managedOptions{}, errors.New("--include-container-output applies only to container logs")
		}
		if opts.owner != "" || opts.workloadID != "" {
			return managedOptions{}, errors.New("logs received an option for another command")
		}
		if local && (opts.registrationID != "" || opts.environmentID != "") {
			return managedOptions{}, errors.New("local component logs do not accept BFT scope identifiers")
		}
	}
	if command == "operation" {
		if hasManagedUpdateOptions(opts) {
			return managedOptions{}, errors.New("operation received an update option")
		}
		if opts.operationID == "" || opts.owner != "lifecycle" && opts.owner != "host" {
			return managedOptions{}, errors.New("operation requires --owner lifecycle|host and --operation-id")
		}
		if subcommand == "get" && opts.follow {
			return managedOptions{}, errors.New("operation get does not accept --follow")
		}
		if opts.component != "" || opts.registrationID != "" || opts.environmentID != "" || opts.workloadID != "" || opts.containerID != "" || opts.afterCursor != "" || opts.follow || opts.includeContent || opts.limit != managedDefaultListLimit {
			return managedOptions{}, errors.New("operation received an option for another command")
		}
	}
	if command == "inspect" || command == "diagnose" {
		if hasManagedUpdateOptions(opts) {
			return managedOptions{}, errors.New(command + " received an update option")
		}
		if opts.component != "" || opts.containerID != "" || opts.owner != "" || opts.operationID != "" || opts.afterCursor != "" || opts.follow || opts.includeContent || opts.limit != managedDefaultListLimit {
			return managedOptions{}, errors.New(command + " received an option for another command")
		}
		if opts.workloadID != "" && opts.registrationID == "" {
			return managedOptions{}, errors.New("--workload-id requires the BFT-provided --registration-id")
		}
	}
	if command == "vm" {
		if hasManagedUpdateOptions(opts) {
			return managedOptions{}, errors.New("vm received an update option")
		}
		if subcommand != "status" && opts.requestID == "" {
			return managedOptions{}, errors.New("vm mutation requires --request-id")
		}
		if subcommand == "status" && opts.requestID != "" {
			return managedOptions{}, errors.New("vm status does not accept --request-id")
		}
		if opts.force && subcommand != "stop" {
			return managedOptions{}, errors.New("--force applies only to vm stop")
		}
		if opts.registrationID != "" || opts.environmentID != "" || opts.workloadID != "" || opts.containerID != "" || opts.component != "" || opts.owner != "" || opts.operationID != "" || opts.afterCursor != "" || opts.follow || opts.includeContent || opts.limit != managedDefaultListLimit {
			return managedOptions{}, errors.New("vm received an option for another command")
		}
		if opts.serviceType != "" || opts.serviceUser != "" || opts.serviceApp != "" || opts.lifecycle != "" {
			return managedOptions{}, errors.New("vm received lifecycle service options")
		}
	}
	if command == "repair" {
		if hasManagedUpdateOptions(opts) {
			return managedOptions{}, errors.New("repair received an update option")
		}
		if opts.requestID == "" {
			return managedOptions{}, errors.New("repair requires --request-id")
		}
		if opts.environmentID != "" || opts.workloadID != "" || opts.containerID != "" || opts.component != "" || opts.owner != "" || opts.operationID != "" || opts.afterCursor != "" || opts.follow || opts.force || opts.includeContent || opts.limit != managedDefaultListLimit {
			return managedOptions{}, errors.New("repair received an option for another command")
		}
		if opts.operatorSocket != "" {
			return managedOptions{}, errors.New("repair does not accept --operator-socket")
		}
	}
	if command == "cleanup" {
		if hasManagedUpdateOptions(opts) {
			return managedOptions{}, errors.New("cleanup received an update option")
		}
		if opts.environmentID == "" || opts.cleanupKind != "reset" && opts.cleanupKind != "discard" {
			return managedOptions{}, errors.New("cleanup requires --environment-id and --kind reset|discard")
		}
		if subcommand == "preview" && (opts.planRevision != 0 || opts.requestID != "" || len(opts.confirmVolumes) != 0 || opts.confirmCleanup) {
			return managedOptions{}, errors.New("cleanup preview does not accept execute confirmation options")
		}
		if subcommand == "execute" {
			if opts.planRevision == 0 || opts.requestID == "" {
				return managedOptions{}, errors.New("cleanup execute requires --plan-revision and --request-id")
			}
			if opts.cleanupKind == "discard" && !opts.confirmCleanup {
				return managedOptions{}, errors.New("cleanup discard requires --confirm-irreversible")
			}
			if opts.cleanupKind == "reset" && (opts.confirmCleanup || len(opts.confirmVolumes) != 0) {
				return managedOptions{}, errors.New("cleanup reset does not accept discard confirmations")
			}
		}
		if opts.registrationID != "" || opts.workloadID != "" || opts.containerID != "" || opts.component != "" || opts.owner != "" || opts.operationID != "" || opts.afterCursor != "" || opts.follow || opts.force || opts.includeContent || opts.limit != managedDefaultListLimit {
			return managedOptions{}, errors.New("cleanup received an option for another command")
		}
		if opts.serviceType != "" || opts.serviceUser != "" || opts.serviceApp != "" || opts.lifecycle != "" {
			return managedOptions{}, errors.New("cleanup received lifecycle service options")
		}
	}
	if command == "update" {
		if opts.requestID == "" || opts.targetRelease == "" || !managedIdentifierPattern.MatchString(opts.targetRelease) || opts.artifactURL == "" || opts.artifactSize == 0 || !managedSHA256Pattern.MatchString(opts.artifactSHA256) {
			return managedOptions{}, errors.New("update requires --request-id, --target-release-id, --artifact-url, --artifact-sha256, and --artifact-size")
		}
		if opts.registrationID != "" || opts.environmentID != "" || opts.workloadID != "" || opts.containerID != "" || opts.component != "" || opts.owner != "" || opts.operationID != "" || opts.afterCursor != "" || opts.follow || opts.force || opts.includeContent || opts.limit != managedDefaultListLimit || opts.cleanupKind != "" || opts.planRevision != 0 || len(opts.confirmVolumes) != 0 || opts.confirmCleanup {
			return managedOptions{}, errors.New("update received an option for another command")
		}
		if opts.operatorSocket != "" || opts.serviceApp != "" {
			return managedOptions{}, errors.New("update does not accept --operator-socket or --service-app")
		}
	}
	if opts.lifecycle == "" && command != "vm" && command != "cleanup" {
		executable, _ := os.Executable()
		opts.lifecycle = filepath.Join(filepath.Dir(executable), "agent-vmm-lifecycle")
	}
	if (command == "vm" || command == "cleanup") && opts.operatorSocket == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return managedOptions{}, errors.New("service user home is unavailable")
		}
		opts.operatorSocket = filepath.Join(home, "Library", "Application Support", "AgentVMM", "operator.sock")
	}
	return opts, nil
}

func hasManagedUpdateOptions(opts managedOptions) bool {
	return opts.artifactURL != "" || opts.artifactSHA256 != "" || opts.artifactSize != 0 || opts.targetRelease != ""
}

func readManagedContainerLogs(ctx context.Context, socket, environmentID, containerID, afterCursor string, limit int, follow bool) ([]managedLogRecord, string, bool, error) {
	connection, err := managedOperatorConnection(ctx, socket)
	if err != nil {
		return nil, "", false, err
	}
	defer connection.Close()
	stream, err := hostv1.NewOperatorServiceClient(connection).Logs(ctx, &hostv1.OperatorLogsRequest{EnvironmentId: environmentID, Query: &hostv1.LogsRequest{ContainerId: containerID, TailRecords: uint64(limit), AfterCursor: afterCursor, Follow: follow}})
	if err != nil {
		return nil, "", false, err
	}
	records := make([]managedLogRecord, 0, limit)
	total, cursor, gap := 0, afterCursor, false
	for len(records) < limit && total < managedLogBytes {
		record, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			if ctx.Err() != nil {
				return records, cursor, gap, nil
			}
			return records, cursor, gap, receiveErr
		}
		data := []byte(redactManagedText(string(record.GetData())))
		if len(record.GetCursor()) > 4096 {
			return nil, "", false, errors.New("log cursor exceeded 4096 bytes")
		}
		truncated := record.GetTruncated()
		if len(data) > managedLogBytes-total {
			data = data[:managedLogBytes-total]
			truncated = true
		}
		total += len(data)
		cursor = record.GetCursor()
		gap = gap || record.GetDroppedRecords() != 0
		records = append(records, managedLogRecord{Cursor: cursor, Timestamp: record.GetTimestamp().AsTime().UTC().Format(time.RFC3339Nano), Stream: strings.ToLower(strings.TrimPrefix(record.GetStream().String(), "LOG_STREAM_")), Data: string(data), Truncated: truncated, Dropped: record.GetDroppedRecords()})
	}
	return records, cursor, gap, nil
}

func readManagedLocalLogs(status map[string]any, opts managedOptions, _ time.Time) ([]managedLogRecord, string, bool, error) {
	serviceUser := stringFact(status, "serviceUser")
	account, err := user.Lookup(serviceUser)
	if err != nil {
		return nil, "", false, errors.New("service user log directory is unavailable")
	}
	base := filepath.Join(account.HomeDir, "Library", "Logs", "AgentVMM")
	paths := []string{filepath.Join(base, "host.stderr.log")}
	if opts.component == "guest" {
		paths = []string{filepath.Join(base, "serial.log.previous"), filepath.Join(base, "serial.log")}
	}
	remaining := managedLogBytes
	records := make([]managedLogRecord, 0, len(paths))
	for _, path := range paths {
		if remaining == 0 {
			break
		}
		data, modified, readErr := readManagedLogTail(path, int64(remaining))
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return nil, "", false, readErr
		}
		data = []byte(redactManagedText(string(data)))
		if opts.operationID != "" {
			data = filterManagedLogOperation(data, opts.operationID)
		}
		truncated := false
		if len(data) > remaining {
			data = data[:remaining]
			truncated = true
		}
		remaining -= len(data)
		cursor := strconv.FormatInt(modified.UnixNano(), 10)
		records = append(records, managedLogRecord{Cursor: cursor, Timestamp: modified.UTC().Format(time.RFC3339Nano), Stream: "stderr", Data: string(data), Truncated: truncated})
	}
	latest := ""
	if len(records) != 0 {
		latest = records[len(records)-1].Cursor
	}
	if opts.afterCursor == "" {
		return records, latest, false, nil
	}
	resumed, gap := resumeManagedLogRecords(records, opts.afterCursor)
	return resumed, latest, gap, nil
}

func resumeManagedLogRecords(records []managedLogRecord, afterCursor string) ([]managedLogRecord, bool) {
	for index, record := range records {
		if record.Cursor == afterCursor {
			return records[index+1:], false
		}
	}
	return records, true
}

func filterManagedLogOperation(data []byte, operationID string) []byte {
	lines := strings.Split(string(data), "\n")
	filtered := lines[:0]
	quoted := `"operation_id":"` + operationID + `"`
	for _, line := range lines {
		if strings.Contains(line, quoted) {
			filtered = append(filtered, line)
		}
	}
	return []byte(strings.Join(filtered, "\n"))
}

func readManagedLogTail(path string, limit int64) ([]byte, time.Time, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, time.Time{}, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, time.Time{}, errors.New("log is not a regular file")
	}
	start := info.Size() - limit
	if start < 0 {
		start = 0
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, time.Time{}, err
	}
	data, err := io.ReadAll(io.LimitReader(file, limit))
	return data, info.ModTime(), err
}
