//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type managedCommand struct {
	flags   []string
	run     func(context.Context, managedRuntime, managedOptions, func(managedEnvelope) error) ([]managedEnvelope, error)
	failure string
}

func managedCommandKey(command, subcommand string) string {
	if subcommand != "" {
		return command + " " + subcommand
	}
	return command
}
func singleManaged(envelope managedEnvelope, err error) ([]managedEnvelope, error) {
	return []managedEnvelope{envelope}, err
}

var managedCommands = map[string]managedCommand{
	"inspect": {flags: []string{"operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "registration-id", "environment-id", "workload-id"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.inspect(ctx, "inspect", opts))
	}, failure: "observation_failed"},
	"diagnose": {flags: []string{"operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "registration-id", "environment-id", "workload-id"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.inspect(ctx, "diagnose", opts))
	}, failure: "observation_failed"},
	"logs": {flags: []string{"json", "service-type", "service-user", "service-app", "lifecycle-helper", "operator-socket", "registration-id", "environment-id", "container-id", "component", "operation-id", "after-cursor", "limit", "include-container-output"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.logs(ctx, opts))
	}, failure: "log_read_failed"},
	"operation get": {flags: []string{"operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "owner", "operation-id"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return runtime.operation(ctx, "get", opts, nil)
	}, failure: "operation_observation_failed"},
	"operation follow": {flags: []string{"operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "owner", "operation-id", "follow"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return runtime.operation(ctx, "follow", opts, emit)
	}, failure: "operation_observation_failed"},
	"vm start": {flags: []string{"json", "operator-socket", "request-id"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.vm(ctx, "start", opts))
	}, failure: "vm_operation_failed"},
	"vm status": {flags: []string{"json", "operator-socket"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.vm(ctx, "status", opts))
	}, failure: "vm_operation_failed"},
	"vm stop": {flags: []string{"json", "operator-socket", "request-id", "force"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.vm(ctx, "stop", opts))
	}, failure: "vm_operation_failed"},
	"repair": {flags: []string{"json", "service-type", "service-user", "service-app", "lifecycle-helper", "request-id", "registration-id"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.repair(ctx, opts))
	}, failure: "repair_failed"},
	"update": {flags: []string{"json", "service-type", "service-user", "lifecycle-helper", "request-id", "target-release-id", "artifact-url", "artifact-sha256", "artifact-size"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.update(ctx, opts))
	}, failure: "update_failed"},
	"cleanup preview": {flags: []string{"json", "operator-socket", "environment-id", "kind"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.cleanup(ctx, "preview", opts))
	}, failure: "cleanup_failed"},
	"cleanup execute": {flags: []string{"json", "operator-socket", "environment-id", "kind", "request-id", "plan-revision", "confirm-volume", "confirm-irreversible"}, run: func(ctx context.Context, runtime managedRuntime, opts managedOptions, emit func(managedEnvelope) error) ([]managedEnvelope, error) {
		return singleManaged(runtime.cleanup(ctx, "execute", opts))
	}, failure: "cleanup_failed"},
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
	ctx, cancel := context.WithTimeout(context.Background(), managedObservationBudget)
	defer cancel()

	definition := managedCommands[managedCommandKey(command, subcommand)]
	envelopes, err := definition.run(ctx, runtime, opts, func(envelope managedEnvelope) error { return writeManagedEnvelope(stdout, envelope, true, command) })
	if err != nil {
		stage := "unknown"
		if command == "update" {
			stage = managedUpdateFailureStage(err)
		}
		writeManagedErrorAtStage(stderr, command, definition.failure, err.Error(), stage)
		return 1
	}
	if command == "operation" && subcommand == "follow" {
		return 0
	}

	for _, envelope := range envelopes {
		if err := writeManagedEnvelope(stdout, envelope, opts.json, command); err != nil {
			writeManagedError(stderr, command, "encoding_failed", err.Error())
			return 1
		}
	}
	return 0
}

func parseManagedOptions(command, subcommand string, arguments []string) (managedOptions, error) {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var opts managedOptions
	opts.limit = managedDefaultListLimit
	definition := managedCommands[managedCommandKey(command, subcommand)]
	for _, name := range definition.flags {
		switch name {
		case "json":
			flags.BoolVar(&opts.json, name, false, "write JSON")
		case "service-type":
			flags.StringVar(&opts.serviceType, name, "", "agent, daemon, or auto")
		case "service-user":
			flags.StringVar(&opts.serviceUser, name, "", "service user")
		case "service-app":
			flags.StringVar(&opts.serviceApp, name, "", "installed Host app")
		case "lifecycle-helper":
			flags.StringVar(&opts.lifecycle, name, "", "lifecycle helper")
		case "operator-socket":
			flags.StringVar(&opts.operatorSocket, name, "", "operator socket")
		case "registration-id":
			flags.StringVar(&opts.registrationID, name, "", "registration id")
		case "environment-id":
			flags.StringVar(&opts.environmentID, name, "", "environment id")
		case "workload-id":
			flags.StringVar(&opts.workloadID, name, "", "workload id")
		case "container-id":
			flags.StringVar(&opts.containerID, name, "", "container id")
		case "component":
			flags.StringVar(&opts.component, name, "", "host or guest")
		case "owner":
			flags.StringVar(&opts.owner, name, "", "lifecycle or host")
		case "operation-id":
			flags.StringVar(&opts.operationID, name, "", "operation id")
		case "after-cursor":
			flags.StringVar(&opts.afterCursor, name, "", "opaque cursor")
		case "limit":
			flags.IntVar(&opts.limit, name, managedDefaultListLimit, "record limit")
		case "follow":
			flags.BoolVar(&opts.follow, name, false, "follow operation")
		case "include-container-output":
			flags.BoolVar(&opts.includeContent, name, false, "include redacted output")
		case "request-id":
			flags.StringVar(&opts.requestID, name, "", "stable request id")
		case "force":
			flags.BoolVar(&opts.force, name, false, "force VM stop")
		case "kind":
			flags.StringVar(&opts.cleanupKind, name, "", "reset or discard")
		case "plan-revision":
			flags.Uint64Var(&opts.planRevision, name, 0, "cleanup plan revision")
		case "confirm-volume":
			flags.Var(&opts.confirmVolumes, name, "exact volume id")
		case "confirm-irreversible":
			flags.BoolVar(&opts.confirmCleanup, name, false, "confirm discard")
		case "artifact-url":
			flags.StringVar(&opts.artifactURL, name, "", "exact HTTPS artifact URL")
		case "artifact-sha256":
			flags.StringVar(&opts.artifactSHA256, name, "", "artifact SHA-256")
		case "artifact-size":
			flags.Uint64Var(&opts.artifactSize, name, 0, "artifact size")
		case "target-release-id":
			flags.StringVar(&opts.targetRelease, name, "", "selected release id")
		}
	}
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
		if local && (opts.registrationID != "" || opts.environmentID != "") {
			return managedOptions{}, errors.New("local component logs do not accept controller scope identifiers")
		}
	}
	if command == "operation" {
		if opts.operationID == "" || opts.owner != "lifecycle" && opts.owner != "host" {
			return managedOptions{}, errors.New("operation requires --owner lifecycle|host and --operation-id")
		}
	}
	if command == "inspect" || command == "diagnose" {
		if opts.workloadID != "" && opts.registrationID == "" {
			return managedOptions{}, errors.New("--workload-id requires the controller-provided --registration-id")
		}
	}
	if command == "vm" {
		if subcommand != "status" && opts.requestID == "" {
			return managedOptions{}, errors.New("vm mutation requires --request-id")
		}
		if subcommand == "status" && opts.requestID != "" {
			return managedOptions{}, errors.New("vm status does not accept --request-id")
		}
		if opts.force && subcommand != "stop" {
			return managedOptions{}, errors.New("--force applies only to vm stop")
		}
	}
	if command == "repair" {
		if opts.requestID == "" {
			return managedOptions{}, errors.New("repair requires --request-id")
		}
		if opts.operatorSocket != "" {
			return managedOptions{}, errors.New("repair does not accept --operator-socket")
		}
	}
	if command == "cleanup" {
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
	}
	if command == "update" {
		if opts.requestID == "" || opts.targetRelease == "" || !managedIdentifierPattern.MatchString(opts.targetRelease) || opts.artifactURL == "" || opts.artifactSize == 0 || !managedSHA256Pattern.MatchString(opts.artifactSHA256) {
			return managedOptions{}, errors.New("update requires --request-id, --target-release-id, --artifact-url, --artifact-sha256, and --artifact-size")
		}
		if opts.operatorSocket != "" || opts.serviceApp != "" {
			return managedOptions{}, errors.New("update does not accept --operator-socket or --service-app")
		}
	}
	if opts.lifecycle == "" && command != "vm" && command != "cleanup" {
		executable, _ := os.Executable()
		opts.lifecycle = defaultLifecycleHelper(executable)
	}
	if (command == "vm" || command == "cleanup") && opts.operatorSocket == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return managedOptions{}, errors.New("service user home is unavailable")
		}
		opts.operatorSocket = filepath.Join(serviceDataDirectory(home), "operator.sock")
	}
	return opts, nil
}
