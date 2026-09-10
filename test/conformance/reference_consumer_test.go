package conformance

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
)

func TestReferenceConformanceCoverageOwners(t *testing.T) {
	repository := filepath.Join("..", "..")
	coverage := []struct {
		name, path string
		anchors    []string
	}{
		{name: "multi-domain revoke", path: "test/e2e/remote_test.go", anchors: []string{"func testThreeRemoteDomains", "BeginRemoteRevocation", "revoked domain runtime session remained usable"}},
		{name: "disconnect and reconnect", path: "test/e2e/remote_test.go", anchors: []string{"func testRemoteWorkflow", "first connector did not stop", "controller did not reconnect"}},
		{name: "frontend", path: "controller/remote/control_test.go", anchors: []string{"HostLoopbackFrontendEvent", "frontend event was not delivered"}},
		{name: "frontend fencing", path: "internal/host/host_loopback_frontend_darwin_test.go", anchors: []string{"TestHostLoopbackFrontendConsumesExactTokenBeforeQueueing", "TestHostLoopbackFrontendCloseIsSynchronousAndGenerationFenced"}},
		{name: "appliance lifecycle", path: "test/e2e/host_test.go", anchors: []string{"func testSystemAppliance", "e2e-reference-a", "ordinary restart"}},
		{name: "new-installation replacement", path: "internal/host/store/appliance_operation_test.go", anchors: []string{"TestNewInstallationReplacementAndRemovalLeaveOtherDomainIntact"}},
		{name: "sealed recovery", path: "internal/host/store/appliance_operation_test.go", anchors: []string{"TestCommitSealedRepairFencesInstallationAndReceiptAtomically"}},
		{name: "reference restart reconciliation", path: "cmd/agent-vmm-reference-appliance/main_test.go", anchors: []string{"TestReferenceControllerReconcilesEmptyAndRestartedInventory", "retained inventory"}},
	}
	for _, item := range coverage {
		t.Run(item.name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(repository, item.path))
			if err != nil {
				t.Fatal(err)
			}
			for _, anchor := range item.anchors {
				if strings.HasPrefix(anchor, "func ") || strings.HasPrefix(anchor, "Test") {
					if !goFunctionExists(t, filepath.Join(repository, item.path), strings.TrimPrefix(anchor, "func ")) {
						t.Errorf("%s does not define %q", item.path, anchor)
					}
					continue
				}
				if !strings.Contains(string(content), anchor) {
					t.Errorf("%s does not own %q", item.path, anchor)
				}
			}
		})
	}
}

func goFunctionExists(t *testing.T, path, name string) bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Errorf("parse %s: %v", path, err)
		return false
	}
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		if declaration, ok := node.(*ast.FuncDecl); ok && declaration.Name.Name == name {
			found = true
		}
		return true
	})
	return found
}

func TestReferenceContractRejectsMalformedControllerInput(t *testing.T) {
	valid := func() *remotev1.HostHello {
		return &remotev1.HostHello{ProtocolVersion: controllerremote.ProtocolVersion, HostApiVersion: "host.v1", RegistrationId: "reference", ConnectionEpoch: 1,
			InventoryWatermark: 1, SupportedFeatures: append([]string(nil), controllerremote.RequiredFeatures...), RequiredFeatures: append([]string(nil), controllerremote.RequiredFeatures...)}
	}
	tests := []struct {
		name   string
		mutate func(*remotev1.HostHello)
	}{
		{name: "missing watermark", mutate: func(value *remotev1.HostHello) { value.InventoryWatermark = 0 }},
		{name: "missing connection epoch", mutate: func(value *remotev1.HostHello) { value.ConnectionEpoch = 0 }},
		{name: "feature downgrade", mutate: func(value *remotev1.HostHello) { value.RequiredFeatures = nil }},
		{name: "duplicate allocation", mutate: func(value *remotev1.HostHello) {
			value.Inventory = []*remotev1.Allocation{{AllocationId: "same"}, {AllocationId: "same"}}
		}},
		{name: "cross registration", mutate: func(value *remotev1.HostHello) { value.RegistrationId = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid()
			test.mutate(value)
			if err := controllerremote.ValidateHello(value, "reference"); err == nil {
				t.Fatal("malformed controller hello was accepted")
			}
		})
	}
}

func TestReferenceConsumerHasNoProductWorkerIdentity(t *testing.T) {
	repository := filepath.Join("..", "..")
	paths := []string{
		"cmd/agent-vmm-reference-appliance/main.go",
		"cmd/agent-vmm-reference-appliance/controller.go",
		"api/remote/v1/remote.proto",
		"spec/tla/ControllerAllocationLifecycle.tla",
		"spec/tla/WorkloadRunLifecycle.tla",
		"spec/tla/DomainAuthorityIsolation.tla",
		"spec/tla/SystemInventorySeparation.tla",
	}
	forbidden := regexp.MustCompile(`(?i)(salix|agentd|worker[_ -]?(id|identity|lifecycle)|executor[_ -]?(id|identity|lifecycle))`)
	for _, path := range paths {
		content, err := os.ReadFile(filepath.Join(repository, path))
		if err != nil {
			t.Fatal(err)
		}
		if match := forbidden.Find(content); match != nil {
			t.Errorf("%s contains product worker vocabulary %q", path, match)
		}
	}
}

func TestControllerModelsAreSeparatedAndWired(t *testing.T) {
	repository := filepath.Join("..", "..")
	modelBytes, err := os.ReadFile(filepath.Join(repository, "spec", "tla", "ControllerAllocationLifecycle.tla"))
	if err != nil {
		t.Fatal(err)
	}
	model := string(modelBytes)
	for _, anchor := range []string{"Allocate(expectedEpoch, expectedSequence)", "InventoryMutation ==", "DeleteAllocation ==", "OpenSession ==", "Disconnect ==", "InventoryEventuallyResettled ==", "CommandFenceExact ==", "CapabilityRequiresCurrentLease =="} {
		if !strings.Contains(model, anchor) {
			t.Errorf("controller model is missing %q", anchor)
		}
	}
	for _, config := range []string{"ControllerAllocationLifecycle.cfg", "ControllerAllocationLifecycleSequenceNegative.cfg"} {
		configBytes, err := os.ReadFile(filepath.Join(repository, "spec", "tla", config))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(configBytes), "MaxRevision = 6") || !strings.Contains(string(configBytes), "EnforceCommandFence") || strings.Contains(string(configBytes), "Domains") {
			t.Errorf("%s does not configure the bounded single-controller command fence", config)
		}
	}
	admissionBytes, err := os.ReadFile(filepath.Join(repository, "spec", "tla", "WorkloadRunLifecycle.cfg"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(admissionBytes), "EnforceAtomicAdmission = TRUE") || !strings.Contains(string(admissionBytes), "EnforceQuiesceFence = TRUE") {
		t.Error("workload lifecycle model does not enforce atomic admission and quiesce fencing")
	}
}
