package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

func TestXMLEscape(t *testing.T) {
	if got := xmlEscape("/Users/a&b<c>"); got != "/Users/a&amp;b&lt;c&gt;" {
		t.Fatalf("xmlEscape() = %q", got)
	}
}

func TestManagedRepairArgumentIsIdempotent(t *testing.T) {
	input := "<array>\n<string>agent-vmm-host</string>\n</array>"
	once := addProgramArgument(input, "--disable-personal-mesh-pairing")
	twice := addProgramArgument(once, "--disable-personal-mesh-pairing")
	if once != twice || strings.Count(once, "--disable-personal-mesh-pairing") != 1 {
		t.Fatalf("managed argument render is not idempotent: %q", twice)
	}
}

func TestRepairInstallsDefinitionsBeforeLoadingServices(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	template := []byte("<plist><string>__HOME__</string><string>__APP__</string></plist>")
	for _, name := range []string{hostLabel + ".plist"} {
		if err := os.WriteFile(filepath.Join(value.resources, name), template, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	var commands []string
	if err := repairWith(value, func(arguments ...string) error { commands = append(commands, strings.Join(arguments, " ")); return nil }); err != nil {
		t.Fatal(err)
	}
	if !regular(value.hostPlist) {
		t.Fatal("service definitions were not installed")
	}
	want := []string{"bootout", "bootstrap", "enable"}
	if len(commands) != len(want) {
		t.Fatalf("commands=%v", commands)
	}
	for index, prefix := range want {
		if !strings.HasPrefix(commands[index], prefix) {
			t.Fatalf("commands[%d]=%q", index, commands[index])
		}
	}
}

func TestUninstallKeepsDataUnlessPurgeWasConfirmed(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(map[bool]string{false: "keep", true: "purge"}[purge], func(t *testing.T) {
			value := testPaths(t.TempDir())
			for _, directory := range []string{value.app, filepath.Dir(value.hostPlist), value.applicationData, value.logs} {
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for _, plist := range []string{value.hostPlist} {
				if err := os.WriteFile(plist, []byte("plist"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var commands []string
			if err := uninstallWith(value, purge, func(arguments ...string) error { commands = append(commands, strings.Join(arguments, " ")); return nil }); err != nil {
				t.Fatal(err)
			}
			if directory(value.app) || regular(value.hostPlist) {
				t.Fatal("installed product files remain")
			}
			if directory(value.applicationData) == purge || directory(value.logs) == purge {
				t.Fatalf("purge=%v data existence is wrong", purge)
			}
			if len(commands) != 1 || !strings.HasPrefix(commands[0], "bootout") {
				t.Fatalf("commands=%v", commands)
			}
		})
	}
}

func TestDrainStopsHost(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, plist := range []string{value.hostPlist} {
		if err := os.WriteFile(plist, []byte("plist"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var commands []string
	if err := drainWith(value, func(arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || !strings.Contains(commands[0], hostLabel) {
		t.Fatalf("drain order=%v", commands)
	}
}

func TestLifecycleOperationIsDurableAndRequestIdempotent(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	first, err := runLifecycleOperation(value, "install", "request-1", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Outcome != "succeeded" || calls != 1 {
		t.Fatalf("first operation=%+v calls=%d", first, calls)
	}
	second, err := runLifecycleOperation(value, "install", "request-1", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.OperationID != first.OperationID || calls != 1 {
		t.Fatalf("duplicate operation=%+v calls=%d", second, calls)
	}
	observed := readOperation(value.operationFile)
	if observed == nil || observed.OperationID != first.OperationID || observed.Outcome != "succeeded" {
		t.Fatalf("durable operation=%+v", observed)
	}
}

func TestLifecycleOperationFailureIsRecorded(t *testing.T) {
	value := testPaths(t.TempDir())
	_, err := runLifecycleOperation(value, "remove", "request-2", func() error { return os.ErrPermission })
	if err == nil {
		t.Fatal("expected operation failure")
	}
	observed := readOperation(value.operationFile)
	if observed == nil || observed.Outcome != "failed" || observed.Kind != "remove" {
		t.Fatalf("failed operation=%+v", observed)
	}
}

func TestLifecycleOperationDoesNotReplayAnUnconfirmedPendingRequest(t *testing.T) {
	value := testPaths(t.TempDir())
	first, err := runLifecycleOperation(value, "install", "request-3", func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	first.Outcome = "pending"
	if err := persistOperation(value.operationFile, first); err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = runLifecycleOperation(value, "install", "request-3", func() error {
		calls++
		return nil
	})
	if err == nil || calls != 0 {
		t.Fatalf("pending operation was replayed: err=%v calls=%d", err, calls)
	}
	if observed := readOperation(value.operationFile); observed == nil || observed.Outcome != "unknown" {
		t.Fatalf("pending operation outcome=%+v", observed)
	}
}

func TestLifecycleOperationSerializesDuplicateRequests(t *testing.T) {
	value := testPaths(t.TempDir())
	var mu sync.Mutex
	calls := 0
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := runLifecycleOperation(value, "install", "request-concurrent", func() error {
				mu.Lock()
				calls++
				mu.Unlock()
				<-start
				return nil
			})
			results <- err
		}()
	}
	// Let the first action finish; the second must observe its durable result.
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("duplicate request executed %d actions", calls)
	}
}

func testPaths(root string) paths {
	home := filepath.Join(root, "home")
	return paths{
		home: home, app: filepath.Join(root, "Agent VMM.app"), resources: filepath.Join(root, "resources"),
		hostPlist:       filepath.Join(home, "Library", "LaunchAgents", hostLabel+".plist"),
		applicationData: filepath.Join(home, "Library", "Application Support", "AgentVMM"), logs: filepath.Join(home, "Library", "Logs", "AgentVMM"),
		operationFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "compute-node-operation.json"),
	}
}

func TestAtomicWriteReplacesDefinition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.plist")
	if err := atomicWrite(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "second" {
		t.Fatalf("definition=%q err=%v", data, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestApplianceFactsDoNotCrossContaminateReadiness(t *testing.T) {
	healthy := &appliancev1.ApplianceStatus{
		ApplianceState: appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING,
		Health:         &appliancev1.ApplianceHealth{Healthy: true},
	}
	controller := &appliancev1.ApplianceStatus{
		ChannelState:   appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE,
		RegistrationId: "registration-b",
		Health:         &appliancev1.ApplianceHealth{ReadinessNonce: []byte("nonce")},
	}
	inventory := &appliancev1.ApplianceStatus{
		InventoryWatermark: 1,
		RegistrationId:     "registration-c",
		Health:             &appliancev1.ApplianceHealth{},
	}

	facts := mergeApplianceFacts(operatorFacts{}, []*appliancev1.ApplianceStatus{healthy, controller, inventory})
	if !facts.applianceHealthy || !facts.controllerActive || !facts.inventoryComplete {
		t.Fatalf("diagnostic facts=%+v", facts)
	}
	if facts.salixReady {
		t.Fatalf("readiness combined facts across appliances: %+v", facts)
	}
}

func TestApplianceFactsRequireOneRegisteredReadyAppliance(t *testing.T) {
	appliance := &appliancev1.ApplianceStatus{
		ApplianceState:     appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING,
		ChannelState:       appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE,
		InventoryWatermark: 1,
		RegistrationId:     "registration-a",
		Health:             &appliancev1.ApplianceHealth{Healthy: true, ReadinessNonce: []byte("nonce")},
	}

	facts := mergeApplianceFacts(operatorFacts{}, []*appliancev1.ApplianceStatus{appliance})
	if !facts.salixReady {
		t.Fatalf("registered appliance was not ready: %+v", facts)
	}
}
