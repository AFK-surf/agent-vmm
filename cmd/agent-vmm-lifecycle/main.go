package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	version   = "0.1.0"
	hostLabel = "com.afk-surf.agent-vmm"
)

type paths struct {
	home            string
	app             string
	resources       string
	hostPlist       string
	applicationData string
	logs            string
	operationFile   string
}

type serviceStatus struct {
	AppInstalled       bool                `json:"appInstalled"`
	HostInstalled      bool                `json:"hostInstalled"`
	HostLoaded         bool                `json:"hostLoaded"`
	ConnectorInstalled bool                `json:"connectorInstalled"`
	ConnectorLoaded    bool                `json:"connectorLoaded"`
	HostReadable       bool                `json:"hostReadable"`
	HostHealthy        bool                `json:"hostHealthy"`
	ApplianceInstalled bool                `json:"applianceInstalled"`
	ApplianceHealthy   bool                `json:"applianceHealthy"`
	ControllerActive   bool                `json:"controllerActive"`
	InventoryComplete  bool                `json:"inventoryComplete"`
	RegistrationID     string              `json:"registrationId,omitempty"`
	SalixReady         bool                `json:"salixReady"`
	Operation          *lifecycleOperation `json:"operation,omitempty"`
}

// The helper is the shared Cue/BFT local operation boundary. Its durable
// request identity and observation-only recovery refine
// Cue's tla/salix/VMMNodeLifecycle.tla; it never grants Salix Workload
// authority from host or runner health alone. The install command delegates
// lifecycle ordering to LifecycleCoordinator; repair remains the narrow
// host-service repair action.

type lifecycleOperation struct {
	OperationID    string `json:"operationId"`
	RequestID      string `json:"requestId"`
	Kind           string `json:"kind"`
	TargetRef      string `json:"targetRef"`
	TargetRevision uint64 `json:"targetRevision"`
	Outcome        string `json:"outcome"`
	StartedAt      int64  `json:"startedAt"`
	UpdatedAt      int64  `json:"updatedAt"`
}

func main() {
	if len(os.Args) < 2 {
		fatal("usage: agent-vmm-lifecycle version --json|status|install|repair [--request-id ID]|drain|uninstall [--purge]")
	}
	if os.Args[1] == "version" {
		if len(os.Args) != 3 || os.Args[2] != "--json" {
			fatal("version requires --json")
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"component": "agent-vmm", "version": version}); err != nil {
			fatal("version encoding failed")
		}
		return
	}
	resolved, err := lifecyclePaths()
	if err != nil {
		fatal(err.Error())
	}
	switch os.Args[1] {
	case "status":
		if err := json.NewEncoder(os.Stdout).Encode(readStatus(resolved)); err != nil {
			fatal("status encoding failed")
		}
	case "install", "repair":
		disablePairing, requestID := parseRepairArgs(os.Args[2:])
		operation, operationErr := runLifecycleOperation(resolved, os.Args[1], requestID, func() error {
			if os.Args[1] == "install" {
				coordinator, err := NewLifecycleCoordinator(newProductionLifecycleAdapter(resolved), LifecycleCoordinatorConfig{})
				if err != nil {
					return err
				}
				return coordinator.Run(context.Background(), LifecycleInstall, LifecycleRequest{
					RequestID:                  requestID,
					DisablePersonalMeshPairing: disablePairing,
				})
			}
			if err := repairWithOptions(resolved, launchctl, disablePairing); err != nil {
				return err
			}
			return ensureVMStarted(resolved, requestID)
		})
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "drain":
		requestID := requestIDArg(os.Args[2:])
		operation, operationErr := runLifecycleOperation(resolved, "drain", requestID, func() error { return drain(resolved) })
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	case "uninstall":
		purge, requestID := parseUninstallArgs(os.Args[2:])
		operation, operationErr := runLifecycleOperation(resolved, "remove", requestID, func() error { return uninstall(resolved, purge) })
		if err := writeOperation(operation); err != nil {
			fatal("operation encoding failed")
		}
		if operationErr != nil {
			fatal(operationErr.Error())
		}
	default:
		fatal("unknown lifecycle command")
	}
}

func lifecyclePaths() (paths, error) {
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return paths{}, errors.New("user home directory is invalid")
	}
	executable, err := os.Executable()
	if err != nil {
		return paths{}, errors.New("helper location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return paths{}, errors.New("helper location is invalid")
	}
	helpers := filepath.Dir(executable)
	if filepath.Base(helpers) != "Helpers" || filepath.Base(executable) != "agent-vmm-lifecycle" {
		return paths{}, errors.New("helper is not inside an Agent VMM application bundle")
	}
	contents := filepath.Dir(helpers)
	if filepath.Base(contents) != "Contents" {
		return paths{}, errors.New("application bundle layout is invalid")
	}
	app := filepath.Dir(contents)
	if filepath.Ext(app) != ".app" {
		return paths{}, errors.New("application bundle is invalid")
	}
	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	return paths{
		home: home, app: app, resources: filepath.Join(contents, "Resources"),
		hostPlist:       filepath.Join(launchAgents, hostLabel+".plist"),
		applicationData: filepath.Join(home, "Library", "Application Support", "AgentVMM"),
		logs:            filepath.Join(home, "Library", "Logs", "AgentVMM"),
		operationFile:   filepath.Join(home, "Library", "Application Support", "AgentVMM", "compute-node-operation.json"),
	}, nil
}

func readStatus(value paths) serviceStatus {
	return readStatusContext(context.Background(), value)
}

func readStatusContext(ctx context.Context, value paths) serviceStatus {
	helper := filepath.Join(value.app, "Contents", "Helpers")
	result := serviceStatus{
		AppInstalled:  directory(value.app),
		HostInstalled: regular(filepath.Join(helper, "agent-vmm-host")) && regular(value.hostPlist),
		HostLoaded:    launchctlContext(ctx, "print", domain()+"/"+hostLabel) == nil,
		Operation:     readOperation(value.operationFile),
	}
	// The local controller channel is the only connector owned by this
	// installation. It is derived from the same operator observation as
	// salixReady; no endpoint or runner process is promoted to readiness.
	result.ConnectorInstalled = result.HostInstalled
	if result.HostLoaded {
		facts, err := readOperatorFactsContext(ctx, filepath.Join(value.applicationData, "operator.sock"))
		if err == nil {
			result.HostReadable = true
			result.HostHealthy = facts.hostHealthy
			result.ApplianceInstalled = facts.applianceInstalled
			result.ApplianceHealthy = facts.applianceHealthy
			result.ControllerActive = facts.controllerActive
			result.InventoryComplete = facts.inventoryComplete
			result.RegistrationID = facts.registrationID
			result.SalixReady = facts.salixReady
			result.ConnectorLoaded = facts.controllerActive && facts.salixReady
		}
	}
	return result
}

func parseRepairArgs(args []string) (bool, string) {
	disablePairing := false
	requestID := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--disable-personal-mesh-pairing":
			disablePairing = true
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		default:
			fatal("install and repair accept --disable-personal-mesh-pairing and --request-id")
		}
	}
	return disablePairing, requestID
}

func requestIDArg(args []string) string {
	for index := 0; index < len(args); index++ {
		if args[index] == "--request-id" && index+1 < len(args) {
			return args[index+1]
		}
	}
	if len(args) != 0 {
		fatal("only --request-id is accepted")
	}
	return ""
}

func parseUninstallArgs(args []string) (bool, string) {
	purge := false
	requestID := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--purge":
			purge = true
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		default:
			fatal("uninstall accepts --purge and --request-id")
		}
	}
	return purge, requestID
}

func runLifecycleOperation(value paths, kind, requestID string, action func() error) (lifecycleOperation, error) {
	release, err := acquireOperationLock(value.operationFile)
	if err != nil {
		return lifecycleOperation{RequestID: requestID, Kind: kind, Outcome: "unknown"}, err
	}
	defer release()
	if requestID == "" {
		requestID = newOperationID("req")
	}
	if previous := readOperation(value.operationFile); previous != nil && previous.RequestID == requestID {
		if previous.Outcome == "succeeded" {
			return *previous, nil
		}
		if previous.Outcome == "failed" {
			return *previous, errors.New("operation already failed; use a new request id after reviewing status")
		}
		observed := readStatus(value)
		if operationConfirmed(previous.Kind, observed) {
			previous.Outcome = "succeeded"
			previous.UpdatedAt = time.Now().UnixMilli()
			if err := persistOperation(value.operationFile, *previous); err != nil {
				return *previous, err
			}
			return *previous, nil
		}
		previous.Outcome = "unknown"
		previous.UpdatedAt = time.Now().UnixMilli()
		_ = persistOperation(value.operationFile, *previous)
		return *previous, errors.New("operation outcome is unknown; read status and use a new request id only after review")
	}
	now := time.Now().UnixMilli()
	operation := lifecycleOperation{
		OperationID: newOperationID("op"), RequestID: requestID, Kind: kind,
		TargetRef: "compute-node/local", TargetRevision: uint64(now), Outcome: "pending",
		StartedAt: now, UpdatedAt: now,
	}
	if err := persistOperation(value.operationFile, operation); err != nil {
		return operation, err
	}
	if err := action(); err != nil {
		operation.Outcome = "failed"
		operation.UpdatedAt = time.Now().UnixMilli()
		_ = persistOperation(value.operationFile, operation)
		return operation, err
	}
	operation.Outcome = "succeeded"
	operation.UpdatedAt = time.Now().UnixMilli()
	if err := persistOperation(value.operationFile, operation); err != nil {
		return operation, err
	}
	return operation, nil
}

func acquireOperationLock(operationPath string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(operationPath), 0o700); err != nil {
		return func() {}, err
	}
	lockPath := operationPath + ".lock"
	deadline := time.Now().Add(2 * time.Second)
	for {
		file, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			if _, writeErr := file.WriteString(fmt.Sprintf("%d\n", os.Getpid())); writeErr != nil {
				_ = file.Close()
				_ = os.Remove(lockPath)
				return func() {}, writeErr
			}
			if closeErr := file.Close(); closeErr != nil {
				_ = os.Remove(lockPath)
				return func() {}, closeErr
			}
			return func() { _ = os.Remove(lockPath) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return func() {}, err
		}
		if info, statErr := os.Stat(lockPath); statErr == nil && time.Since(info.ModTime()) > 30*time.Second {
			_ = os.Remove(lockPath)
			continue
		}
		if time.Now().After(deadline) {
			return func() {}, errors.New("another Agent VMM lifecycle operation is still running")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func operationConfirmed(kind string, status serviceStatus) bool {
	switch kind {
	case "install", "repair":
		return status.HostInstalled && status.HostLoaded && status.SalixReady
	case "drain":
		return !status.HostLoaded
	case "remove":
		return !status.AppInstalled && !status.HostInstalled
	default:
		return false
	}
}

func newOperationID(prefix string) string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(value[:])
}

func readOperation(path string) *lifecycleOperation {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var operation lifecycleOperation
	if json.Unmarshal(data, &operation) != nil || operation.OperationID == "" || operation.RequestID == "" {
		return nil
	}
	return &operation
}

func persistOperation(path string, operation lifecycleOperation) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(operation)
	if err != nil {
		return err
	}
	return atomicWrite(path, data, 0o600)
}

func writeOperation(operation lifecycleOperation) error {
	return json.NewEncoder(os.Stdout).Encode(operation)
}

type operatorFacts struct {
	hostHealthy        bool
	applianceInstalled bool
	applianceHealthy   bool
	controllerActive   bool
	inventoryComplete  bool
	registrationID     string
	salixReady         bool
}

func readOperatorFacts(socket string) (operatorFacts, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return readOperatorFactsContext(ctx, socket)
}

func readOperatorFactsContext(ctx context.Context, socket string) (operatorFacts, error) {
	connection, err := operatorConnection(ctx, socket)
	if err != nil {
		return operatorFacts{}, err
	}
	defer connection.Close()

	snapshot, err := hostv1.NewOperatorServiceClient(connection).GetSystemSnapshot(ctx, &hostv1.GetSystemSnapshotRequest{})
	if err != nil {
		return operatorFacts{}, err
	}
	appliances, err := appliancev1.NewApplianceAdminServiceClient(connection).List(ctx, &appliancev1.ListAppliancesRequest{})
	if err != nil {
		return operatorFacts{}, err
	}
	facts := operatorFacts{
		hostHealthy: snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING &&
			snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY,
	}
	facts = mergeApplianceFacts(facts, appliances.GetAppliances())
	return facts, nil
}

// mergeApplianceFacts keeps diagnostic facts aggregate, but computes SalixReady
// only from one registered appliance. Readiness is an identity-bound
// conjunction; facts from independent appliance/controller instances must not
// be assembled into a synthetic ready result.
func mergeApplianceFacts(facts operatorFacts, appliances []*appliancev1.ApplianceStatus) operatorFacts {
	facts.applianceInstalled = len(appliances) > 0
	for _, appliance := range appliances {
		health := appliance.GetHealth()
		applianceHealthy := appliance.GetApplianceState() == appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING &&
			health.GetHealthy()
		controllerActive := appliance.GetChannelState() == appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE &&
			len(health.GetReadinessNonce()) > 0
		inventoryComplete := appliance.GetInventoryWatermark() > 0
		facts.applianceHealthy = facts.applianceHealthy || applianceHealthy
		facts.controllerActive = facts.controllerActive || controllerActive
		facts.inventoryComplete = facts.inventoryComplete || inventoryComplete
		if facts.registrationID == "" && appliance.GetRegistrationId() != "" {
			facts.registrationID = appliance.GetRegistrationId()
		}
		if appliance.GetRegistrationId() != "" &&
			applianceHealthy &&
			controllerActive &&
			inventoryComplete {
			facts.salixReady = true
		}
	}
	return facts
}

func ensureVMStarted(value paths, requestID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return ensureVMStartedContext(ctx, value, requestID)
}

func ensureVMStartedContext(ctx context.Context, value paths, requestID string) error {
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable")
	}
	defer connection.Close()
	operator := hostv1.NewOperatorServiceClient(connection)
	for {
		vm, err := operator.GetVM(ctx, &hostv1.GetVMRequest{})
		if err == nil && vm.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return errors.New("Agent VMM VM status could not be read")
			}
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if _, err := operator.StartVM(ctx, &hostv1.StartVMRequest{RequestId: requestID}); err != nil {
			return errors.New("Agent VMM VM could not be started")
		}
		for {
			snapshot, snapshotErr := operator.GetSystemSnapshot(ctx, &hostv1.GetSystemSnapshotRequest{})
			if snapshotErr == nil && snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING &&
				snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY {
				return nil
			}
			if ctx.Err() != nil {
				return errors.New("Agent VMM VM did not become healthy")
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func operatorConnection(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
}

func repair(value paths) error { return repairWith(value, launchctl) }

func repairWithPolicy(value paths, disablePairing bool) error {
	return repairWithOptions(value, launchctl, disablePairing)
}

func repairWith(value paths, run func(...string) error) error {
	return repairWithOptions(value, run, false)
}

func repairWithOptions(value paths, run func(...string) error, disablePairing bool) error {
	return repairWithOptionsContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	}, disablePairing)
}

func repairWithOptionsContext(ctx context.Context, value paths, run func(context.Context, ...string) error, disablePairing bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !directory(value.app) {
		return errors.New("Agent VMM application bundle is missing")
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		return errors.New("LaunchAgents directory could not be created")
	}
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		return errors.New("application data directory could not be created")
	}
	if err := os.MkdirAll(value.logs, 0o700); err != nil {
		return errors.New("log directory could not be created")
	}
	for _, item := range []struct{ resource, destination string }{{hostLabel + ".plist", value.hostPlist}} {
		template, err := os.ReadFile(filepath.Join(value.resources, item.resource))
		if err != nil {
			return errors.New("bundled service definition is missing")
		}
		rendered := strings.ReplaceAll(string(template), "__HOME__", xmlEscape(value.home))
		rendered = strings.ReplaceAll(rendered, "__APP__", xmlEscape(value.app))
		if item.destination == value.hostPlist && disablePairing {
			rendered = addProgramArgument(rendered, "--disable-personal-mesh-pairing")
		}
		if err := atomicWrite(item.destination, []byte(rendered), 0o600); err != nil {
			return errors.New("service definition could not be installed")
		}
	}
	_ = run(ctx, "bootout", domain()+"/"+hostLabel)
	if err := run(ctx, "bootstrap", domain(), value.hostPlist); err != nil {
		return errors.New("host service could not be loaded")
	}
	if err := run(ctx, "enable", domain()+"/"+hostLabel); err != nil {
		return errors.New("host service could not be enabled")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func addProgramArgument(plist, argument string) string {
	marker := "</array>"
	index := strings.Index(plist, marker)
	if index < 0 || strings.Contains(plist, "<string>"+argument+"</string>") {
		return plist
	}
	return plist[:index] + "\t\t<string>" + xmlEscape(argument) + "</string>\n\t" + plist[index:]
}

func uninstall(value paths, purge bool) error { return uninstallWith(value, purge, launchctl) }

func drain(value paths) error { return drainWith(value, launchctl) }

// launchctl bootout waits for the host and its supervised workers to terminate.
func drainWith(value paths, run func(...string) error) error {
	return drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	})
}

func drainWithContext(ctx context.Context, value paths, run func(context.Context, ...string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run(ctx, "bootout", domain()+"/"+hostLabel); err != nil && regular(value.hostPlist) {
		return errors.New("host service could not be drained")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func uninstallWith(value paths, purge bool, run func(...string) error) error {
	return uninstallWithContext(context.Background(), value, purge, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	})
}

func uninstallWithContext(ctx context.Context, value paths, purge bool, run func(context.Context, ...string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = run(ctx, "bootout", domain()+"/"+hostLabel)
	if err := os.Remove(value.hostPlist); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("host service definition could not be removed")
	}
	if purge {
		if err := os.RemoveAll(value.applicationData); err != nil {
			return errors.New("application data could not be removed")
		}
		if err := os.RemoveAll(value.logs); err != nil {
			return errors.New("logs could not be removed")
		}
	}
	if err := os.RemoveAll(value.app); err != nil {
		return errors.New("application bundle could not be removed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".agent-vmm-*.plist")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func launchctl(arguments ...string) error {
	return launchctlContext(context.Background(), arguments...)
}

func launchctlContext(ctx context.Context, arguments ...string) error {
	command := exec.CommandContext(ctx, "/bin/launchctl", arguments...)
	command.Stdout = nil
	command.Stderr = nil
	return command.Run()
}

func domain() string { return fmt.Sprintf("gui/%d", os.Getuid()) }
func regular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
func directory(path string) bool { info, err := os.Stat(path); return err == nil && info.IsDir() }

func xmlEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
