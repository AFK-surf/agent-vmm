//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
)

func testBoot(t *testing.T) {
	harness := startHost(t)
	vm := harness.start(t)
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING || vm.GetBootId() == "" {
		t.Fatalf("StartVM() = %+v", vm)
	}
	if output := readFileTail(harness.serial, 64<<10); !bytes.Contains(output, []byte(`"stage":"listener","state":"succeeded"`)) {
		t.Fatalf("Guest boot console did not reach Host serial log: %s", output)
	}
	if _, err := os.Stat(harness.runtimeSocket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw runtime socket is exposed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	snapshot, err := harness.client.GetSystemSnapshot(ctx, &hostv1.GetSystemSnapshotRequest{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GetUsage() != nil && (snapshot.GetUsage().GetSampledAt() == nil || snapshot.GetUsage().GetStale() && snapshot.GetUsage().GetUnavailableReason() == "") {
		t.Fatalf("system network usage = %+v", snapshot.GetUsage())
	}
	previousBootID := vm.GetBootId()
	for cycle := 1; cycle < 20; cycle++ {
		harness.stop(t, false)
		vm = harness.start(t)
		if vm.GetBootId() == previousBootID {
			t.Fatalf("cycle %d reused boot ID", cycle)
		}
		previousBootID = vm.GetBootId()
	}
	harness.stop(t, false)
}

func TestStateInitializationE2E(t *testing.T) {
	harness := startHost(t)
	if err := os.MkdirAll(harness.instance, 0o700); err != nil {
		t.Fatal(err)
	}
	state, err := os.OpenFile(filepath.Join(harness.instance, "state.raw"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Truncate(16 << 30); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	marker, err := os.OpenFile(filepath.Join(harness.instance, "state.initializing"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	harness.start(t)
	if _, err := os.Stat(filepath.Join(harness.instance, "state.initializing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initialization marker remains: %v", err)
	}
	info, err := os.Stat(filepath.Join(harness.instance, "state.raw"))
	if err != nil || info.Size() != 16<<30 {
		t.Fatalf("state disk = %v, %v", info, err)
	}
	harness.stop(t, true)
	for index := range harness.processArgs {
		if harness.processArgs[index] == "--state-size" && index+1 < len(harness.processArgs) {
			harness.processArgs[index+1] = fmt.Sprint(24 << 30)
		}
	}
	harness.crashAndRestartHost(t)
	afterRestart, err := os.Stat(filepath.Join(harness.instance, "state.raw"))
	if err != nil || afterRestart.Size() != 16<<30 || !os.SameFile(info, afterRestart) {
		t.Fatalf("existing state disk changed under a new-install size: before=%v after=%v err=%v", info, afterRestart, err)
	}
	database, err := openE2EDatabase(harness.database)
	if err != nil {
		t.Fatal(err)
	}
	var recordedStateBytes int64
	if err := database.QueryRow(`SELECT state_bytes FROM vm_config WHERE id = 1`).Scan(&recordedStateBytes); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if recordedStateBytes != 16<<30 {
		t.Fatalf("persisted state bytes = %d, want actual file length %d", recordedStateBytes, int64(16<<30))
	}
	harness.stop(t, true)

	state, err = os.OpenFile(filepath.Join(harness.instance, "state.raw"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.WriteAt(make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := state.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	summary, err := harness.client.GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
	if err != nil || summary.GetVm() == nil {
		t.Fatalf("VM start impact unavailable: summary=%+v err=%v", summary, err)
	}
	fence := &hostv1.VMMutationFence{State: summary.GetVm().GetState(), BootId: summary.GetVm().GetBootId(), ObservedWorkloads: summary.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}
	if _, err := harness.client.StartVM(ctx, &hostv1.StartVMRequest{RequestId: "start-corrupt-state", Fence: fence}); err == nil {
		t.Fatal("corrupt initialized state disk was reformatted")
	}
	// The pre-listener Guest failure is visible only on the console.
	console := readFileTail(harness.serial, 64<<10)
	if last := bytes.LastIndex(console, []byte("guest_startup=")); last < 0 || !bytes.Contains(console[last:], []byte(`"stage":"bootstrap","state":"failed"`)) {
		t.Fatalf("corrupt state disk failure did not reach the console: %s", console)
	}
	if _, err := os.Stat(filepath.Join(harness.instance, "state.initializing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt disk recreated initialization marker: %v", err)
	}
}

func testVMRestart(t *testing.T, harness *hostHarness) {
	client := harness.runtime(t)
	for _, namespaceID := range []string{"restart-source"} {
		ensureNamespace(t, client, namespaceID, 256<<20)
		importRegistryImage(t, client, namespaceID)
	}
	createVolume(t, client, "restart-source", "data")
	caller := createContainer(t, client, "restart-source", "caller", idleProcess(), []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data"}})
	server := createContainer(t, client, "restart-source", "server", idleProcess(), nil)
	startContainer(t, client, "restart-source", caller.GetId())
	startContainer(t, client, "restart-source", server.GetId())
	startEchoServer(t, client, "restart-source", server.GetId(), 8080)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "echo restart-durable > /data/value; echo root-durable > /root/value; sync; cat /data/value"}); code != 0 || string(output) != "restart-durable\n" {
		t.Fatalf("fsync volume write = %q, exit %d", output, code)
	}
	revokeRoute := createServiceRoute(t, client, "restart-source", caller.GetId(), server.GetId(), "service")
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "printf before | nc -w 5 service 8080"}); code != 0 || string(output) != "before" {
		t.Fatalf("link before restart = %q, exit %d", output, code)
	}
	eventCtx, eventCancel := context.WithTimeout(context.Background(), 30*time.Second)
	events, err := harness.client.WatchEvents(eventCtx, &hostv1.WatchEventsRequest{})
	if err != nil {
		eventCancel()
		t.Fatal(err)
	}
	beforeRestartEvent, err := events.Recv()
	if err != nil || beforeRestartEvent.GetGuestBootId() == "" || beforeRestartEvent.GetSequence() == 0 {
		eventCancel()
		t.Fatalf("event before restart = %+v, %v", beforeRestartEvent, err)
	}
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer streamCancel()
	stream, err := client.Exec(streamCtx, "restart-source")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: execStart(t, streamCtx, client, "restart-source", caller.GetId(), "restart-connection", []string{"nc", "service", "8080"})}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("open")}}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || string(response.GetStdout()) != "open" {
		t.Fatalf("connection before restart = %q, %v", response.GetStdout(), err)
	}
	tty, err := client.Exec(streamCtx, "restart-source")
	if err != nil {
		t.Fatal(err)
	}
	ttyStart := execStart(t, streamCtx, client, "restart-source", caller.GetId(), "restart-tty", []string{"sh"})
	ttyStart.Tty, ttyStart.Rows, ttyStart.Columns = true, 24, 80
	if err := tty.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: ttyStart}}); err != nil {
		t.Fatal(err)
	}
	if err := tty.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("echo tty-open\n")}}); err != nil {
		t.Fatal(err)
	}
	for {
		response, err := tty.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(response.GetStdout(), []byte("tty-open")) {
			break
		}
	}
	harness.stop(t, false)
	for {
		if _, err := events.Recv(); err != nil {
			break
		}
	}
	eventCancel()
	harness.start(t)
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), 30*time.Second)
	snapshot, err := client.Snapshot(snapshotCtx)
	snapshotCancel()
	if err != nil || snapshot.GetVm().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		t.Fatalf("snapshot after event disconnect = %+v, %v", snapshot.GetVm(), err)
	}
	reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer reconnectCancel()
	reconnected, err := harness.client.WatchEvents(reconnectCtx, &hostv1.WatchEventsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	afterRestartEvent, err := reconnected.Recv()
	if err != nil || afterRestartEvent.GetGuestBootId() == "" || afterRestartEvent.GetGuestBootId() == beforeRestartEvent.GetGuestBootId() || afterRestartEvent.GetSequence() == 0 {
		t.Fatalf("event after snapshot-first reconnect = %+v, %v", afterRestartEvent, err)
	}
	for name, oldStream := range map[string]grpc.BidiStreamingClient[hostv1.ExecRequest, hostv1.ExecResponse]{"exec connection": stream, "TTY": tty} {
		marker := []byte("recovered-" + strings.ReplaceAll(name, " ", "-"))
		_ = oldStream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: append([]byte("echo "), append(marker, '\n')...)}})
		for {
			response, err := oldStream.Recv()
			if err != nil {
				break
			}
			if bytes.Contains(response.GetStdout(), marker) {
				t.Fatalf("%s recovered across VM restart", name)
			}
		}
	}
	client = harness.runtime(t)
	waitRestoredContainer(t, client, "restart-source", caller.GetId())
	waitRestoredContainer(t, client, "restart-source", server.GetId())
	startEchoServer(t, client, "restart-source", server.GetId(), 8080)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "cat /data/value /root/value"}); code != 0 || string(output) != "restart-durable\nroot-durable\n" {
		t.Fatalf("filesystem and volume after restart = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"nc", "-z", "-w", "2", containerAddress(t, client, "restart-source", server.GetId()), "8080"}); code != 0 {
		t.Fatal("host policy was not replayed after restart")
	}
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "printf replayed | nc -w 5 service 8080"}); code != 0 || string(output) != "replayed" {
		t.Fatalf("replayed link = %q, exit %d", output, code)
	}
	revokeRoute()
	stopDeleteContainer(t, client, "restart-source", caller.GetId())
	stopDeleteContainer(t, client, "restart-source", server.GetId())
	deleteVolume(t, client, "restart-source", "data")
	deleteNamespace(t, client, "restart-source")
}

func testProviderStateVMRestart(t *testing.T, harness *hostHarness) {
	const (
		namespaceID = "provider-state-restart"
		image       = "agent-vmm.local/provider-state-restart:latest"
		volumeID    = "vol-provider-state-restart"
	)
	client := harness.runtime(t)
	ensureNamespace(t, client, namespaceID, 128<<20)
	archive, digest := buildImportArchive(t)
	importTestImage(t, client, namespaceID, "import-provider-state-restart", image, archive, digest)
	owner, mode := uint32(1000), uint32(0o700)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if _, err := client.CreateVolume(ctx, namespaceID, &hostv1.CreateVolumeRequest{
		RequestId: "create-provider-state-restart", VolumeId: volumeID,
		OwnerUid: &owner, OwnerGid: &owner, Mode: &mode,
	}); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	mount := []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: volumeID}, Destination: "/workspace"}}
	provider := createContainerAsUser(t, client, namespaceID, "provider", image, "1000:1000", []string{"/bin/provider-state", "idle"}, []string{"HOME=/workspace"}, mount)
	startContainer(t, client, namespaceID, provider.GetId())
	if output, code := execContainer(t, client, namespaceID, provider.GetId(), []string{"/bin/provider-state", "write-pi"}); code != 0 || string(output) != "authenticated\n" {
		t.Fatalf("provider state write = %q, exit %d", output, code)
	}
	harness.stop(t, false)
	harness.start(t)
	client = harness.runtime(t)
	waitRestoredContainer(t, client, namespaceID, provider.GetId())
	if output, code := execContainer(t, client, namespaceID, provider.GetId(), []string{"/bin/provider-state", "read-pi"}); code != 0 || string(output) != "authenticated\n" {
		t.Fatalf("provider state after VM restart = %q, exit %d", output, code)
	}
	stopDeleteContainer(t, client, namespaceID, provider.GetId())
	deleteVolume(t, client, namespaceID, volumeID)
	deleteNamespace(t, client, namespaceID)
}

func testForcedVMStop(t *testing.T, harness *hostHarness) {
	client := harness.runtime(t)
	for _, namespaceID := range []string{"force-source"} {
		ensureNamespace(t, client, namespaceID, 256<<20)
		importRegistryImage(t, client, namespaceID)
	}
	createVolume(t, client, "force-source", "data")
	caller := createContainer(t, client, "force-source", "caller", idleProcess(), []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data"}})
	server := createContainer(t, client, "force-source", "server", idleProcess(), nil)
	startContainer(t, client, "force-source", caller.GetId())
	startContainer(t, client, "force-source", server.GetId())
	startEchoServer(t, client, "force-source", server.GetId(), 8080)
	if _, code := execContainer(t, client, "force-source", caller.GetId(), []string{"sh", "-c", "echo force-durable > /data/value; sync"}); code != 0 {
		t.Fatalf("fsync before forced stop exited %d", code)
	}
	if _, code := execContainer(t, client, "force-source", server.GetId(), []string{"sync"}); code != 0 {
		t.Fatalf("server fsync before forced stop exited %d", code)
	}
	revokeRoute := createServiceRoute(t, client, "force-source", caller.GetId(), server.GetId(), "service")
	if _, err := client.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	harness.stop(t, true)
	stopped, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, environmentID := range []string{"force-source"} {
		found := false
		for _, environment := range stopped.GetEnvironments() {
			if environment.GetId() == environmentID {
				found = len(environment.GetContainers()) == 2 && environment.GetUsage().GetStale()
				for _, container := range environment.GetContainers() {
					found = found && container.GetState() == hostv1.ContainerState_CONTAINER_STATE_UNKNOWN
				}
			}
		}
		if !found {
			t.Fatalf("forced-stop snapshot did not expose %s container as UNKNOWN with stale usage", environmentID)
		}
	}
	harness.start(t)
	client = harness.runtime(t)
	waitRestoredContainer(t, client, "force-source", caller.GetId())
	waitRestoredContainer(t, client, "force-source", server.GetId())
	startEchoServer(t, client, "force-source", server.GetId(), 8080)
	if output, code := execContainer(t, client, "force-source", caller.GetId(), []string{"cat", "/data/value"}); code != 0 || string(output) != "force-durable\n" {
		t.Fatalf("fsync data after forced stop = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "force-source", caller.GetId(), []string{"nc", "-z", "-w", "2", "service", "8080"}); code != 0 {
		t.Fatal("host policy was not replayed after forced restart")
	}
	revokeRoute()
	stopDeleteContainer(t, client, "force-source", caller.GetId())
	stopDeleteContainer(t, client, "force-source", server.GetId())
	deleteVolume(t, client, "force-source", "data")
	deleteNamespace(t, client, "force-source")
}

// waitRestoredContainer waits for VM start recovery to restart a container
// whose Host running intent survived the VM restart.
func waitRestoredContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := client.GetContainer(ctx, namespaceID, &hostv1.GetContainerRequest{ContainerId: containerID})
		cancel()
		if err == nil && response.GetContainer().GetState() == hostv1.ContainerState_CONTAINER_STATE_RUNNING && response.GetContainer().GetDesiredRunning() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("container %s/%s was not restored after VM restart: %+v, %v", namespaceID, containerID, response.GetContainer(), err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
