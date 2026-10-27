//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	_ "modernc.org/sqlite"
)

// Keep the fixture on the immutable linux/arm64 child manifest. The mirror's
// multi-platform index has intermittently evicted older child blobs.
const workloadImage = "mirror.gcr.io/library/alpine@sha256:1832327faf048390adc33852575d37c7ba155e064a339e78b9bd81983a8c7a00"

type hostHarness struct {
	archiveCA           string
	client              hostv1.OperatorServiceClient
	connection          *grpc.ClientConn
	process             *exec.Cmd
	binary              string
	processArgs         []string
	serial              string
	instance            string
	database            string
	runtimeSocket       string
	operatorSocket      string
	remoteComputeSocket string
	trustSocket         string
	runtimeClient       *productClient
	starts              int
}

func startHost(t *testing.T) *hostHarness {
	return startHostWithState(t, 16<<30)
}

func openE2EDatabase(path string) (*sql.DB, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if _, err := database.Exec("PRAGMA busy_timeout=10000"); err != nil {
		database.Close()
		return nil, err
	}
	return database, nil
}

func startHostWithState(t *testing.T, stateBytes uint64) *hostHarness {
	t.Helper()
	return startHostWithResources(t, stateBytes, 4<<30)
}

func startHostWithResources(t *testing.T, stateBytes, memoryBytes uint64) *hostHarness {
	t.Helper()
	bundle := os.Getenv("AGENT_VMM_E2E_BUNDLE")
	if bundle == "" {
		t.Fatal("AGENT_VMM_E2E_BUNDLE is required")
	}
	directory, err := os.MkdirTemp("/tmp", "agent-vmm-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	archiveDirectory := filepath.Join(directory, "archives")
	if err := os.MkdirAll(archiveDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	archiveServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if r.Method != http.MethodGet || name != filepath.Base(name) || !strings.HasPrefix(name, "import-") || !strings.HasSuffix(name, ".tar") {
			http.NotFound(w, r)
			return
		}
		path := filepath.Join(archiveDirectory, name)
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, path)
	}))
	t.Cleanup(archiveServer.Close)
	archiveCA := filepath.Join(directory, "archive-ca.pem")
	if err := os.WriteFile(archiveCA, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: archiveServer.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	binary := os.Getenv("AGENT_VMM_E2E_HOST_BINARY")
	if binary == "" {
		binary = filepath.Join(directory, "agent-vmm-host")
		runCommand(t, 2*time.Minute, "go", "build", "-tags=e2e", "-o", binary, "../../cmd/agent-vmm-host")
		runCommand(t, time.Minute, "codesign", "--force", "--sign", "-", "--entitlements", "../../build/host/vz.entitlements.plist", binary)
	}
	operatorSocket := filepath.Join(directory, "operator.sock")
	remoteComputeSocket := filepath.Join(directory, "remote-compute.sock")
	trustSocket := filepath.Join(directory, "trust.sock")
	runtimeSocket := filepath.Join(directory, "runtime.sock")
	database := filepath.Join(directory, "host.sqlite")
	sessions := filepath.Join(directory, "sessions")
	instance := filepath.Join(directory, "instance")
	serial := filepath.Join(directory, "serial.log")
	processArgs := []string{
		"--bundle", bundle, "--instance", instance, "--state-size", fmt.Sprint(stateBytes),
		"--cpus", "4", "--memory", fmt.Sprint(memoryBytes),
		"--serial-log", serial, "--operator-socket", operatorSocket, "--remote-compute-socket", remoteComputeSocket,
		"--database", database, "--session-directory", sessions,
	}
	if dataWorker := os.Getenv("AGENT_VMM_E2E_DATA_BINARY"); dataWorker != "" {
		processArgs = append(processArgs, "--data-worker", dataWorker)
	}
	harness := &hostHarness{archiveCA: archiveCA, binary: binary, processArgs: processArgs, serial: serial, instance: instance, database: database,
		runtimeSocket: runtimeSocket, operatorSocket: operatorSocket, remoteComputeSocket: remoteComputeSocket,
		trustSocket: trustSocket}
	harness.launchHostProcess(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("host log tail:\n%s", readFileTail(serial+".host.log", 64<<10))
			t.Logf("serial log tail:\n%s", readFileTail(serial, 64<<10))
		}
	})
	t.Cleanup(func() { harness.shutdownHostProcess() })
	waitForPath(t, operatorSocket, 10*time.Second)
	waitForPath(t, trustSocket, 10*time.Second)
	connection, err := grpc.NewClient("passthrough:///host", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", operatorSocket)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	operator := hostv1.NewOperatorServiceClient(connection)
	runtimeClient := newProductClient(operator)
	runtimeClient.archiveDirectory, runtimeClient.archiveURL = archiveDirectory, archiveServer.URL
	t.Cleanup(runtimeClient.close)
	harness.client, harness.connection, harness.runtimeClient = operator, connection, runtimeClient
	return harness
}

func (harness *hostHarness) launchHostProcess(t *testing.T) {
	t.Helper()
	process := exec.Command(harness.binary, harness.processArgs...)
	process.Env = append(os.Environ(), "AGENT_VMM_E2E_ARCHIVE_CA="+harness.archiveCA)
	process.Stdout = os.Stdout
	hostLog, err := os.OpenFile(harness.serial+".host.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer hostLog.Close()
	process.Stderr = hostLog
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	harness.process = process
}

func (harness *hostHarness) shutdownHostProcess() {
	process := harness.process
	if process == nil || (process.ProcessState != nil && process.ProcessState.Exited()) {
		return
	}
	_ = process.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = process.Process.Kill()
		<-done
	}
}

func (harness *hostHarness) crashAndRestartHost(t *testing.T) {
	t.Helper()
	if err := harness.process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := harness.process.Wait(); err == nil {
		t.Fatal("SIGKILL host unexpectedly exited successfully")
	}
	for _, path := range []string{harness.operatorSocket, harness.remoteComputeSocket, harness.trustSocket} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	harness.launchHostProcess(t)
	waitForPath(t, harness.operatorSocket, 10*time.Second)
	waitForPath(t, harness.trustSocket, 10*time.Second)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, err := harness.client.GetVM(ctx, &hostv1.GetVMRequest{})
		cancel()
		if err == nil && response.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted host did not resume VM: response=%v err=%v", response, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func readFileTail(path string, limit int64) []byte {
	file, err := os.Open(path)
	if err != nil {
		return []byte(err.Error())
	}
	defer file.Close()
	if info, err := file.Stat(); err == nil && info.Size() > limit {
		_, _ = file.Seek(-limit, io.SeekEnd)
	}
	data, _ := io.ReadAll(io.LimitReader(file, limit))
	return data
}

func (harness *hostHarness) start(t *testing.T) *hostv1.VM {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	summary, err := harness.client.GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
	if err != nil || summary.GetVm() == nil {
		t.Fatalf("VM start impact unavailable: summary=%+v err=%v", summary, err)
	}
	fence := &hostv1.VMMutationFence{State: summary.GetVm().GetState(), BootId: summary.GetVm().GetBootId(), ObservedWorkloads: summary.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}
	_, err = harness.client.StartVM(ctx, &hostv1.StartVMRequest{RequestId: fmt.Sprintf("start-%d", time.Now().UnixNano()), Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	if harness.starts == 0 {
		t.Logf("E2E_COLD_START_MS=%d", time.Since(started).Milliseconds())
	}
	harness.starts++
	response, err := harness.client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetVm()
}

func (harness *hostHarness) stop(t *testing.T, force bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	summary, err := harness.client.GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
	if err != nil || summary.GetVm() == nil {
		t.Fatalf("VM stop impact unavailable: summary=%+v err=%v", summary, err)
	}
	fence := &hostv1.VMMutationFence{State: summary.GetVm().GetState(), BootId: summary.GetVm().GetBootId(), ObservedWorkloads: summary.GetKnownWorkloads(), WholeVmImpactAcknowledged: true}
	_, err = harness.client.StopVM(ctx, &hostv1.StopVMRequest{RequestId: fmt.Sprintf("stop-%d", time.Now().UnixNano()), Force: force, Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	response, err := harness.client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetVm().GetState() != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM() = %+v", response.GetVm())
	}
}

func (harness *hostHarness) runtime(t *testing.T) *productClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := harness.runtimeClient.refreshSessions(ctx); err != nil {
		t.Fatal(err)
	}
	return harness.runtimeClient
}

func ensureNamespace(t *testing.T, client *productClient, namespaceID string, diskBytes uint64) {
	ensureNamespaceWithEgress(t, client, namespaceID, diskBytes, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
}

func ensureNamespaceWithEgress(t *testing.T, client *productClient, namespaceID string, diskBytes uint64, egress hostv1.EgressMode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.EnsureEnvironment(ctx, namespaceID, "ensure-"+namespaceID, &hostv1.ResourceLimits{
		Pids: optional(uint32(512)), DiskBytes: optional(diskBytes),
	}, egress)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetNamespaceId() == "" {
		t.Fatalf("environment allocation = %+v", response)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := client.DeleteEnvironment(ctx, namespaceID, "cleanup-"+namespaceID); err != nil && status.Code(err) != codes.NotFound {
			t.Errorf("cleanup environment %q: %v", namespaceID, err)
		}
	})
}

func createVolume(t *testing.T, client *productClient, namespaceID, volumeID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateVolume(ctx, namespaceID, &hostv1.CreateVolumeRequest{RequestId: "create-" + namespaceID + "-" + volumeID, VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.DeleteVolume(ctx, namespaceID, &hostv1.DeleteVolumeRequest{RequestId: "cleanup-" + namespaceID + "-" + volumeID, VolumeId: volumeID})
	})
}

func deleteVolume(t *testing.T, client *productClient, namespaceID, volumeID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.DeleteVolume(ctx, namespaceID, &hostv1.DeleteVolumeRequest{RequestId: "delete-" + namespaceID + "-" + volumeID, VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
}

func idleProcess() []string {
	return []string{"tail", "-f", "/dev/null"}
}

func stopDeleteContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	stopContainer(t, client, namespaceID, containerID)
	deleteContainerEventually(t, client, namespaceID, containerID)
}

func importRegistryImage(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	digest, err := importRegistryImageReference(ctx, client, namespaceID, "pull-"+namespaceID, workloadImage)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("pull digest = %q", digest)
	}
}

func importRegistryImageReference(ctx context.Context, client *productClient, namespaceID, requestID, reference string) (string, error) {
	archive, err := client.registryArchive(ctx, reference)
	if err != nil {
		return "", err
	}
	file, err := os.Open(archive.path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	image, err := client.ImportImage(ctx, namespaceID, &hostv1.ImportImageHeader{RequestId: requestID, ImageReference: reference, ArchiveSha256: archive.hash, ManifestDigest: archive.digest, Platform: "linux/arm64", ArchiveSize: archive.size}, file)
	if err != nil {
		return "", err
	}
	return image.GetDigest(), nil
}

func createContainer(t *testing.T, client *productClient, namespaceID, containerID string, argv []string, volumes []*hostv1.VolumeMount) *hostv1.Container {
	return createContainerWithImageAndVolumes(t, client, namespaceID, containerID, workloadImage, argv, volumes)
}

func createContainerWithImage(t *testing.T, client *productClient, namespaceID, containerID, image string, argv []string) *hostv1.Container {
	return createContainerWithImageAndVolumes(t, client, namespaceID, containerID, image, argv, nil)
}

func createContainerWithImageAndVolumes(t *testing.T, client *productClient, namespaceID, containerID, image string, argv []string, volumes []*hostv1.VolumeMount) *hostv1.Container {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.CreateContainer(ctx, namespaceID, &hostv1.CreateContainerRequest{
		RequestId:   "create-" + namespaceID + "-" + containerID,
		ContainerId: containerID, Image: image, EntrypointOverride: &hostv1.StringList{Values: argv}, CommandOverride: &hostv1.StringList{}, Volumes: volumes, LogLimitBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		seconds := uint32(1)
		_, _ = client.StopContainer(ctx, namespaceID, &hostv1.StopContainerRequest{RequestId: "cleanup-stop-" + namespaceID + "-" + containerID, ContainerId: containerID, TimeoutSeconds: &seconds})
		if request, err := currentDeleteContainerRequest(ctx, client, namespaceID, containerID, "cleanup-delete-"+namespaceID+"-"+containerID); err == nil {
			_ = client.DeleteContainer(ctx, namespaceID, request)
		}
	})
	return response.GetContainer()
}

func createContainerAsUser(t *testing.T, client *productClient, namespaceID, containerID, image, user string, argv, env []string, volumes []*hostv1.VolumeMount) *hostv1.Container {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.CreateContainer(ctx, namespaceID, &hostv1.CreateContainerRequest{
		RequestId: "create-" + namespaceID + "-" + containerID, ContainerId: containerID,
		Image: image, EntrypointOverride: &hostv1.StringList{Values: argv}, CommandOverride: &hostv1.StringList{},
		Env: env, User: &user, Volumes: volumes, LogLimitBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		seconds := uint32(1)
		_, _ = client.StopContainer(ctx, namespaceID, &hostv1.StopContainerRequest{RequestId: "cleanup-stop-" + namespaceID + "-" + containerID, ContainerId: containerID, TimeoutSeconds: &seconds})
		if request, err := currentDeleteContainerRequest(ctx, client, namespaceID, containerID, "cleanup-delete-"+namespaceID+"-"+containerID); err == nil {
			_ = client.DeleteContainer(ctx, namespaceID, request)
		}
	})
	return response.GetContainer()
}

func startContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.StartContainer(ctx, namespaceID, startContainerRequest(t, ctx, client, namespaceID, containerID))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("StartContainer() = %+v", response.GetContainer())
	}
}

// currentDeleteContainerRequest targets the container's current generation. The
// Host requires the exact generation for start and delete, so a test that
// means a stale generation builds its own request.
func currentDeleteContainerRequest(ctx context.Context, client *productClient, namespaceID, containerID, requestID string) (*hostv1.DeleteContainerRequest, error) {
	current, err := client.GetContainer(ctx, namespaceID, &hostv1.GetContainerRequest{ContainerId: containerID})
	if err != nil {
		return nil, err
	}
	return &hostv1.DeleteContainerRequest{RequestId: requestID, ContainerId: containerID, ExpectedGenerationId: current.GetContainer().GetGenerationId()}, nil
}

func startContainerRequest(t *testing.T, ctx context.Context, client *productClient, namespaceID, containerID string) *hostv1.StartContainerRequest {
	t.Helper()
	request, err := currentDeleteContainerRequest(ctx, client, namespaceID, containerID, "start-"+namespaceID+"-"+containerID+time.Now().Format("150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	return &hostv1.StartContainerRequest{RequestId: request.GetRequestId(), ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId()}
}

// startExitingContainer starts a short-lived process, which can already have
// exited (STOPPED) or be awaiting reaping (UNKNOWN) when Start returns.
func startExitingContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.StartContainer(ctx, namespaceID, startContainerRequest(t, ctx, client, namespaceID, containerID))
	if err != nil {
		t.Fatal(err)
	}
	switch response.GetContainer().GetState() {
	case hostv1.ContainerState_CONTAINER_STATE_RUNNING, hostv1.ContainerState_CONTAINER_STATE_UNKNOWN, hostv1.ContainerState_CONTAINER_STATE_STOPPED:
	default:
		t.Fatalf("StartContainer() = %+v", response.GetContainer())
	}
}

func startAndWaitContainer(t *testing.T, client *productClient, namespaceID, containerID string, expected uint32) uint32 {
	t.Helper()
	startExitingContainer(t, client, namespaceID, containerID)
	return waitContainer(t, client, namespaceID, containerID, expected)
}

func stopContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	timeout := uint32(3)
	response, err := client.StopContainer(ctx, namespaceID, &hostv1.StopContainerRequest{RequestId: "stop-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000"), ContainerId: containerID, TimeoutSeconds: &timeout})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatalf("StopContainer() = %+v", response.GetContainer())
	}
}

func waitContainer(t *testing.T, client *productClient, namespaceID, containerID string, expected uint32) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.WaitContainer(ctx, namespaceID, &hostv1.WaitContainerRequest{ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	if expected == 0 && response.GetExitCode() != 0 {
		t.Fatalf("WaitContainer() exit = %d", response.GetExitCode())
	}
	return response.GetExitCode()
}

func deleteContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := currentDeleteContainerRequest(ctx, client, namespaceID, containerID, "delete-"+namespaceID+"-"+containerID)
	if err == nil {
		err = client.DeleteContainer(ctx, namespaceID, request)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func deleteContainerEventually(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var request *hostv1.DeleteContainerRequest
		request, lastErr = currentDeleteContainerRequest(ctx, client, namespaceID, containerID, fmt.Sprintf("delete-%d", time.Now().UnixNano()))
		if lastErr == nil {
			lastErr = client.DeleteContainer(ctx, namespaceID, request)
		}
		cancel()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("DeleteContainer(%q/%q): %v", namespaceID, containerID, lastErr)
		}
		<-ticker.C
	}
}

func deleteNamespace(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.DeleteEnvironment(ctx, namespaceID, "delete-"+namespaceID); err != nil {
		t.Fatal(err)
	}
}

func deleteNamespaceEventually(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = client.DeleteEnvironment(ctx, namespaceID, fmt.Sprintf("delete-%s-%d", namespaceID, attempt))
		cancel()
		if lastErr == nil {
			return
		}
		if !strings.HasPrefix(lastErr.Error(), "cleanup failed:") {
			t.Fatal(lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("DeleteNamespace(%q): %v", namespaceID, lastErr)
}

func execContainer(t *testing.T, client *productClient, namespaceID, containerID string, argv []string) ([]byte, uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Exec(ctx, namespaceID)
	if err != nil {
		t.Fatalf("exec %s: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
	}
	start := execStart(t, ctx, client, namespaceID, containerID, "exec-"+namespaceID+"-"+containerID+time.Now().Format("150405.000000000"), argv)
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: start}}); err != nil {
		t.Fatalf("exec %s send: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("exec %s close: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
	}
	var output bytes.Buffer
	var exitCode uint32
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return output.Bytes(), exitCode
		}
		if err != nil {
			t.Fatalf("exec %s receive: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
		}
		output.Write(response.GetStdout())
		output.Write(response.GetStderr())
		if _, ok := response.Event.(*hostv1.ExecResponse_ExitCode); ok {
			exitCode = response.GetExitCode()
		}
	}
}

// execStart binds an exec activity to the container's current instance and to
// the caller's context deadline, as the Host requires for every stream activity.
func execStart(t *testing.T, ctx context.Context, client *productClient, namespaceID, containerID, requestID string, argv []string) *hostv1.ExecStart {
	t.Helper()
	current, err := client.GetContainer(ctx, namespaceID, &hostv1.GetContainerRequest{ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("exec context has no deadline")
	}
	return &hostv1.ExecStart{RequestId: requestID, ContainerId: containerID, Argv: argv,
		ExpectedInstanceId: current.GetContainer().GetInstanceId(), ExecutionId: requestID,
		ExecutionKind: hostv1.ExecutionKind_EXECUTION_KIND_EXEC, ExecutionDeadlineUnixNano: deadline.UnixNano()}
}

func readLogs(t *testing.T, client *productClient, namespaceID, containerID string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Logs(ctx, namespaceID, &hostv1.LogsRequest{ContainerId: containerID, TailRecords: 4096})
	if err != nil {
		t.Fatal(err)
	}
	var result bytes.Buffer
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return result.Bytes()
		}
		if err != nil {
			t.Fatal(err)
		}
		result.Write(response.GetData())
	}
}

func waitForPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("wait for %s: timeout", path)
}

func runCommand(t *testing.T, timeout time.Duration, name string, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, name, arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", name, err, output)
	}
}
