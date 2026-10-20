//go:build darwin

package vmm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/Code-Hex/vz/v3"
)

func TestNewManagerLeavesHostMemoryReserve(t *testing.T) {
	original := physicalMemoryBytes
	physicalMemoryBytes = func() (uint64, error) { return 16 << 30, nil }
	t.Cleanup(func() { physicalMemoryBytes = original })
	config := Config{BundleDirectory: "bundle", InstanceDirectory: "instance", SerialLog: "serial", CPUCount: 2, StateBytes: 32 << 30}
	config.MemoryBytes = 12 << 30
	if _, err := NewManager(config); err != nil {
		t.Fatalf("12 GiB VM rejected on 16 GiB host: %v", err)
	}
	config.MemoryBytes = (12 << 30) + 1
	if _, err := NewManager(config); err == nil {
		t.Fatal("VM accepted without the 4 GiB host reserve")
	}
}

func TestResourceSnapshotSeparatesHostStateDiskFacts(t *testing.T) {
	directory := t.TempDir()
	state := filepath.Join(directory, "state.raw")
	if err := os.WriteFile(state, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{config: Config{InstanceDirectory: directory}}
	resources := manager.ResourceSnapshot()
	if resources.GetPids() != 1 || resources.MemoryBytes == nil {
		t.Fatalf("host process resources = %+v", resources)
	}
	if resources.OpenFds == nil && !strings.Contains(resources.GetUnavailableReason(), "host file descriptors") {
		t.Fatalf("unknown file descriptor count has no reason: %+v", resources)
	}
	if resources.GetDisk().GetUsedBytes() == 0 || resources.GetDisk().GetCommittedBytes() != 4096 ||
		resources.GetDisk().GetCapacityBytes() < 4096 || resources.GetDisk().GetSystemReserveBytes() != uint64(hostEmergencyBytes) {
		t.Fatalf("host disk resources = %+v", resources.GetDisk())
	}
}

func TestResourceSnapshotKeepsUnavailableCurrentMemoryAbsent(t *testing.T) {
	original := processMemoryBytes
	processMemoryBytes = func() (uint64, error) { return 0, errors.New("current Host process memory is unavailable") }
	t.Cleanup(func() { processMemoryBytes = original })
	resources := (&Manager{config: Config{InstanceDirectory: t.TempDir()}}).ResourceSnapshot()
	if resources.MemoryBytes != nil || !strings.Contains(resources.GetUnavailableReason(), "current Host process memory") {
		t.Fatalf("unknown current memory was represented as a value: %+v", resources)
	}
}

func TestUnexpectedStopHasOneRecoveryOwnerAndFailsClosed(t *testing.T) {
	machine := new(vz.VirtualMachine)
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "boot-one", machine: machine}
	fences := 0
	manager.admissionFence = func() {
		fences++
		if manager.state != hostv1.VMState_VM_STATE_RUNNING || manager.bootID != "boot-one" {
			t.Fatal("admission fence ran after terminal state was published")
		}
	}
	invalidations := 0
	manager.invalidate = func() { invalidations++ }
	recoveries := 0
	manager.recover = func(previousBootID string) {
		recoveries++
		if previousBootID != "boot-one" {
			t.Fatalf("previous boot ID = %q", previousBootID)
		}
	}
	invalidate, recover, bootID := manager.terminalTransition(machine, vz.VirtualMachineStateStopped, true)
	if manager.state != hostv1.VMState_VM_STATE_STOPPED || manager.bootID != "" || !manager.recoveryUsed {
		t.Fatalf("first transition = state %s boot %q used %t", manager.state, manager.bootID, manager.recoveryUsed)
	}
	invalidate()
	recover(bootID)
	if invalidations != 1 || recoveries != 1 || fences != 1 {
		t.Fatalf("first transition callbacks = invalidate %d recover %d", invalidations, recoveries)
	}
	manager.state = hostv1.VMState_VM_STATE_RUNNING
	manager.bootID = "boot-two"
	machine = new(vz.VirtualMachine)
	manager.machine = machine
	manager.admissionFence = func() { fences++ }
	invalidate, recover, _ = manager.terminalTransition(machine, vz.VirtualMachineStateError, true)
	if recover != nil {
		t.Fatal("second unexpected stop acquired another recovery owner")
	}
	invalidate()
	if manager.state != hostv1.VMState_VM_STATE_FAILED || !strings.Contains(manager.lastErr, "operator action required") || manager.bootID != "" {
		t.Fatalf("second transition = %+v", manager.snapshotLocked())
	}
	if invalidations != 2 || recoveries != 1 || fences != 2 {
		t.Fatalf("terminal callbacks = invalidate %d recover %d", invalidations, recoveries)
	}
}

func TestExpectedStopDoesNotConsumeRecovery(t *testing.T) {
	machine := new(vz.VirtualMachine)
	manager := &Manager{state: hostv1.VMState_VM_STATE_STOPPING, bootID: "boot", machine: machine}
	_, recover, _ := manager.terminalTransition(machine, vz.VirtualMachineStateStopped, true)
	if recover != nil || manager.recoveryUsed || manager.state != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("expected stop = state %s used %t", manager.state, manager.recoveryUsed)
	}
}

func TestFailedVMWithMachineIsStillAttached(t *testing.T) {
	manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, machine: new(vz.VirtualMachine)}
	if !manager.HasMachineAttachment() {
		t.Fatal("FAILED VM attachment was treated as terminal")
	}
	manager.machine = nil
	if manager.HasMachineAttachment() {
		t.Fatal("released VM remained attached")
	}
}

func TestStaleMachineNotificationCannotInvalidateCurrentRuntime(t *testing.T) {
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "current", machine: new(vz.VirtualMachine)}
	invalidate, recover, _ := manager.terminalTransition(new(vz.VirtualMachine), vz.VirtualMachineStateStopped, true)
	if invalidate != nil || recover != nil || manager.state != hostv1.VMState_VM_STATE_RUNNING || manager.bootID != "current" {
		t.Fatalf("stale notification changed current runtime: %+v", manager.snapshotLocked())
	}
}

func TestRecoveryFailureRequiresOperatorAction(t *testing.T) {
	manager := &Manager{state: hostv1.VMState_VM_STATE_STOPPED, recoveryUsed: true}
	manager.FailRecovery(errors.New("policy replay failed"))
	if manager.state != hostv1.VMState_VM_STATE_FAILED || !strings.Contains(manager.lastErr, "operator action required") || !strings.Contains(manager.lastErr, "policy replay failed") {
		t.Fatalf("recovery failure = %+v", manager.snapshotLocked())
	}
}

func TestClosedStateNotificationFailsClosed(t *testing.T) {
	machine := new(vz.VirtualMachine)
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "boot", machine: machine}
	invalidated := false
	manager.invalidate = func() { invalidated = true }
	states := make(chan vz.VirtualMachineState)
	close(states)
	manager.observeStateChanges(machine, states)
	if !invalidated || manager.state != hostv1.VMState_VM_STATE_FAILED || manager.bootID != "" {
		t.Fatalf("closed notification channel did not fail closed: %+v invalidated=%t", manager.snapshotLocked(), invalidated)
	}
}

func TestReadVerifiedBundledApplianceMaterialReturnsOuterLockedPair(t *testing.T) {
	directory := t.TempDir()
	manifestBytes := []byte("canonical-appliance-manifest")
	lockedDigest := "sha256:" + strings.Repeat("a", 64)
	files := map[string][]byte{
		"Image":                     []byte("kernel"),
		"initramfs.xz":              []byte("initramfs"),
		"rootfs.squashfs":           []byte("rootfs"),
		"mcp-appliance.digest":      []byte(lockedDigest + "\n"),
		"mcp-appliance/manifest.pb": manifestBytes,
	}
	artifacts := make(map[string]bundle.Artifact)
	for name, path := range map[string]string{
		"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs",
		"mcp_appliance_digest": "mcp-appliance.digest", "mcp_appliance_manifest": "mcp-appliance/manifest.pb",
	} {
		data := files[path]
		if err := os.MkdirAll(filepath.Dir(filepath.Join(directory, path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, path), data, 0o600); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(data)
		artifacts[name] = bundle.Artifact{Path: path, Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
	}
	outer, err := json.Marshal(bundle.Manifest{SchemaVersion: 1, Architecture: "arm64", MinimumMacOS: "14.0", Artifacts: artifacts,
		RootFS: bundle.RootFS{Filesystem: "squashfs", Compressor: "zstd", BlockSize: 1 << 20, Filter: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, bundle.ManifestName), outer, 0o600); err != nil {
		t.Fatal(err)
	}
	outerHash := sha256.Sum256(outer)
	manager := &Manager{config: Config{BundleDirectory: directory, GuestManifestDigest: hex.EncodeToString(outerHash[:])}}
	material, available, err := manager.ReadVerifiedBundledApplianceMaterial()
	if err != nil {
		t.Fatal(err)
	}
	if !available {
		t.Fatal("bundled appliance material was not reported")
	}
	if string(material.Manifest) != string(manifestBytes) || material.ImageDigest != lockedDigest {
		t.Fatalf("material=%+v", material)
	}
	if err := os.WriteFile(filepath.Join(directory, "rootfs.squashfs"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.ReadVerifiedBundledApplianceMaterial(); err != nil {
		t.Fatalf("bundled appliance read reverified unrelated rootfs: %v", err)
	}
}

func TestReadVerifiedBundledApplianceMaterialAllowsRuntimeBundle(t *testing.T) {
	directory := t.TempDir()
	files := map[string][]byte{
		"Image":           []byte("kernel"),
		"initramfs.xz":    []byte("initramfs"),
		"rootfs.squashfs": []byte("rootfs"),
	}
	artifacts := make(map[string]bundle.Artifact)
	for name, path := range map[string]string{"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs"} {
		data := files[path]
		if err := os.WriteFile(filepath.Join(directory, path), data, 0o600); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(data)
		artifacts[name] = bundle.Artifact{Path: path, Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
	}
	outer, err := json.Marshal(bundle.Manifest{SchemaVersion: 1, Architecture: "arm64", MinimumMacOS: "14.0", Artifacts: artifacts,
		RootFS: bundle.RootFS{Filesystem: "squashfs", Compressor: "zstd", BlockSize: 1 << 20, Filter: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, bundle.ManifestName), outer, 0o600); err != nil {
		t.Fatal(err)
	}
	outerHash := sha256.Sum256(outer)
	manager := &Manager{config: Config{BundleDirectory: directory, GuestManifestDigest: hex.EncodeToString(outerHash[:])}}
	if _, available, err := manager.ReadVerifiedBundledApplianceMaterial(); err != nil || available {
		t.Fatalf("runtime bundle material available=%t err=%v", available, err)
	}
}

func TestStateInitializationMarker(t *testing.T) {
	directory := t.TempDir()
	const size = int64(1 << 20)

	initializing, err := prepareState(directory, size)
	if err != nil || !initializing {
		t.Fatalf("prepare new state = %t, %v", initializing, err)
	}
	info, err := os.Stat(filepath.Join(directory, "state.raw"))
	if err != nil || info.Size() != size {
		t.Fatalf("state disk = %v, %v", info, err)
	}
	initializing, err = prepareState(directory, size)
	if err != nil || !initializing {
		t.Fatalf("prepare interrupted state = %t, %v", initializing, err)
	}
	if err := finishStateInitialization(directory); err != nil {
		t.Fatal(err)
	}
	initializing, err = prepareState(directory, size*2)
	if err != nil || initializing {
		t.Fatalf("prepare initialized state = %t, %v", initializing, err)
	}
	info, err = os.Stat(filepath.Join(directory, "state.raw"))
	if err != nil || info.Size() != size {
		t.Fatalf("existing state disk was resized to the new-install default: %v, %v", info, err)
	}
}

func TestExistingSparseStateRestoresPhysicalBacking(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.raw")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(32 << 20); err != nil {
		t.Fatal(err)
	}
	sentinel := []byte("existing-state-data")
	for _, offset := range []int64{0, 8 << 20, (32 << 20) - int64(len(sentinel))} {
		if _, err := file.WriteAt(sentinel, offset); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	initializing, err := prepareState(directory, 32<<20)
	if err != nil || initializing {
		t.Fatalf("sparse state result = %t, %v", initializing, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	allocated, err := allocatedBytes(info)
	if err != nil || allocated < uint64(info.Size()) {
		t.Fatalf("restored allocation = %d for size %d, err=%v", allocated, info.Size(), err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	holes, err := stateHoleBytes(file, info.Size())
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil || holes != 0 {
		t.Fatalf("restored state still has %d logical hole bytes, err=%v", holes, err)
	}
	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for _, offset := range []int64{0, 8 << 20, (32 << 20) - int64(len(sentinel))} {
		contents := make([]byte, len(sentinel))
		if _, err := file.ReadAt(contents, offset); err != nil {
			t.Fatal(err)
		}
		if string(contents) != string(sentinel) {
			t.Fatalf("existing data changed at offset %d", offset)
		}
	}
}

func TestHostDirectorySharesRequireDetachedTerminalVM(t *testing.T) {
	directory, err := vz.NewSharedDirectory(t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		state    hostv1.VMState
		attached bool
		allowed  bool
	}{
		{"stopped", hostv1.VMState_VM_STATE_STOPPED, false, true},
		{"failed", hostv1.VMState_VM_STATE_FAILED, false, true},
		{"starting", hostv1.VMState_VM_STATE_STARTING, false, false},
		{"running", hostv1.VMState_VM_STATE_RUNNING, true, false},
		{"stopping", hostv1.VMState_VM_STATE_STOPPING, true, false},
		{"retained failed machine", hostv1.VMState_VM_STATE_FAILED, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			existing := map[string]*vz.SharedDirectory{"existing": directory}
			manager := &Manager{state: test.state, hostDirectoryShares: existing}
			if test.attached {
				manager.machine = new(vz.VirtualMachine)
			}
			supplied := map[string]*vz.SharedDirectory{"authorized": directory}
			err := manager.SetHostDirectoryShares(supplied)
			if !test.allowed {
				if err == nil || manager.hostDirectoryShares["existing"] != directory {
					t.Fatalf("live share changed: %v", err)
				}
				return
			}
			if err != nil || manager.hostDirectoryShares["authorized"] != directory {
				t.Fatalf("boot share missing: %v", err)
			}
			delete(supplied, "authorized")
			if manager.hostDirectoryShares["authorized"] != directory {
				t.Fatal("caller map changed boot configuration")
			}
			if err := manager.SetHostDirectoryShares(nil); err != nil || len(manager.hostDirectoryShares) != 0 {
				t.Fatalf("clear boot shares: %v", err)
			}
		})
	}
}

func TestConfigurationHasNoNetworkDevice(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"kernel", "initramfs", "rootfs", "state.raw"} {
		if err := os.WriteFile(filepath.Join(directory, name), make([]byte, 4096), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	serial, err := os.OpenFile(filepath.Join(directory, "serial.log"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	manager := &Manager{config: Config{CPUCount: 2, MemoryBytes: 1 << 30, InstanceDirectory: directory}}
	verified := &bundle.Verified{Directory: directory, Manifest: bundle.Manifest{Artifacts: map[string]bundle.Artifact{
		"kernel": {Path: "kernel"}, "initramfs": {Path: "initramfs"}, "rootfs": {Path: "rootfs"},
	}}}
	configuration, err := manager.configuration(verified, serial, false, startObservation{})
	if err != nil {
		t.Fatal(err)
	}
	if devices := configuration.NetworkDevices(); len(devices) != 0 {
		t.Fatalf("configuration has %d network devices", len(devices))
	}
}

func TestKernelArgumentRejectsUnsafeOperationID(t *testing.T) {
	for _, value := range []string{"operation-17", "request_1.2+retry"} {
		if !kernelArgument(value) {
			t.Fatalf("safe kernel argument %q rejected", value)
		}
	}
	for _, value := range []string{"", "contains space", "line\nbreak", strings.Repeat("a", 129)} {
		if kernelArgument(value) {
			t.Fatalf("unsafe kernel argument %q accepted", value)
		}
	}
}

func TestRotateSerialLogRetainsBoundedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serial.log")
	prefix := []byte("discarded-prefix")
	tail := make([]byte, maxSerialLogBytes)
	for index := range tail {
		tail[index] = byte(index % 251)
	}
	if err := os.WriteFile(path, append(prefix, tail...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rotateSerialLog(path); err != nil {
		t.Fatal(err)
	}
	previous, err := os.ReadFile(path + ".previous")
	if err != nil {
		t.Fatal(err)
	}
	if string(previous) != string(tail) {
		t.Fatalf("previous log retained %d bytes with wrong tail", len(previous))
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatalf("current log size = %d, err=%v", info.Size(), err)
	}
}

func TestBundleFailurePrecedesInstanceCreation(t *testing.T) {
	root := t.TempDir()
	instance := filepath.Join(root, "instance")
	manager, err := NewManager(Config{
		BundleDirectory: filepath.Join(root, "invalid-bundle"), InstanceDirectory: instance,
		CPUCount: 2, MemoryBytes: 1 << 30, StateBytes: 1 << 30,
		SerialLog: filepath.Join(root, "serial.log"), GuestManifestDigest: strings.Repeat("0", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with invalid bundle")
	}
	if _, err := os.Stat(instance); !os.IsNotExist(err) {
		t.Fatalf("instance created before verification: %v", err)
	}
}

func TestStopFailedVMUsesExactMachineFact(t *testing.T) {
	for _, force := range []bool{false, true} {
		manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, lastErr: "guest startup failed", bootID: "stale-boot"}
		vm, err := manager.Stop(context.Background(), force)
		if err != nil || vm.GetState() != hostv1.VMState_VM_STATE_STOPPED || vm.GetBootId() != "" || vm.GetError() != "guest startup failed" {
			t.Fatalf("force=%t vm=%v err=%v", force, vm, err)
		}
	}
	machine := &vz.VirtualMachine{}
	manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, machine: machine, lastErr: "failed with retained instance"}
	if _, err := manager.Stop(context.Background(), false); err == nil || manager.machine != machine || manager.state != hostv1.VMState_VM_STATE_FAILED || manager.lastErr != "failed with retained instance" {
		t.Fatal("retained failed instance was treated as stopped")
	}
}
