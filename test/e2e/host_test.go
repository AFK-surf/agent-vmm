//go:build darwin && e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestE2E(t *testing.T) {
	t.Run("boot", testBoot)
	t.Run("state_initialization", testStateInitialization)

	harness := startHostWithState(t, 8<<30)
	harness.start(t)
	client := harness.vmmd(t)
	parallel := []struct {
		name string
		run  func(*testing.T, vmmdv1.VMMDServiceClient)
	}{
		{"container_lifecycle", testContainerLifecycle},
		{"named_volume", testNamedVolume},
		{"disk_quota", testDiskQuota},
		{"same_namespace", testSameNamespace},
		{"default_isolation", testDefaultIsolation},
		{"partial_link", testPartialLink},
		{"link_revoke", testLinkRevoke},
		{"port_forward", testPortForward},
		{"dockerfile_build", testDockerfileBuild},
		{"parallel_agents", testParallelAgents},
		{"cancellation_cleanup", testCancellationCleanup},
	}
	limit := make(chan struct{}, 4)
	var active, peak atomic.Int32
	t.Run("parallel", func(t *testing.T) {
		for _, test := range parallel {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				limit <- struct{}{}
				current := active.Add(1)
				for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
				}
				defer func() { active.Add(-1); <-limit }()
				test.run(t, client)
			})
		}
	})
	t.Logf("E2E_PARALLEL_PEAK=%d", peak.Load())
	t.Run("egress", func(t *testing.T) { testEgress(t, client) })
	t.Run("VM_restart", func(t *testing.T) { testVMRestart(t, harness) })
	t.Run("forced_VM_stop", func(t *testing.T) { testForcedVMStop(t, harness) })
	t.Run("residual_scan", func(t *testing.T) { testResidualScan(t, harness.vmmd(t)) })
	harness.stop(t, hostv1.StopMode_STOP_MODE_GRACEFUL)
}

func testBoot(t *testing.T) {
	harness := startHost(t)
	vm := harness.start(t)
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING || vm.GetBootId() == "" {
		t.Fatalf("StartVM() = %+v", vm)
	}
	info, err := os.Stat(harness.relaySocket)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("relay socket = %v, %v", info, err)
	}
	previousBootID := vm.GetBootId()
	for cycle := 1; cycle < 20; cycle++ {
		harness.stop(t, hostv1.StopMode_STOP_MODE_GRACEFUL)
		vm = harness.start(t)
		if vm.GetBootId() == previousBootID {
			t.Fatalf("cycle %d reused boot ID", cycle)
		}
		previousBootID = vm.GetBootId()
	}
	harness.stop(t, hostv1.StopMode_STOP_MODE_GRACEFUL)
}

func testStateInitialization(t *testing.T) {
	harness := startHost(t)
	if err := os.MkdirAll(harness.instance, 0o700); err != nil {
		t.Fatal(err)
	}
	state, err := os.OpenFile(filepath.Join(harness.instance, "state.raw"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Truncate(1 << 30); err != nil {
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
	if err != nil || info.Size() != 1<<30 {
		t.Fatalf("state disk = %v, %v", info, err)
	}
	harness.stop(t, hostv1.StopMode_STOP_MODE_FORCE)

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := harness.client.StartVM(ctx, &hostv1.StartVMRequest{}); err == nil {
		t.Fatal("corrupt initialized state disk was reformatted")
	}
	if _, err := os.Stat(filepath.Join(harness.instance, "state.initializing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt disk recreated initialization marker: %v", err)
	}
}

func testContainerLifecycle(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespace(t, client, "lifecycle", 128<<20)
	pullImage(t, client, "lifecycle")
	container := createContainer(t, client, "lifecycle", "persistent", []string{"/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"}, nil)
	startContainer(t, client, "lifecycle", container.GetId())
	if output, code := execContainer(t, client, "lifecycle", container.GetId(), []string{"/bin/sh", "-c", "echo snapshot > /root/value && echo hello"}); code != 0 || string(output) != "hello\n" {
		t.Fatalf("exec = %q, exit %d", output, code)
	}
	if output, code := execContainer(t, client, "lifecycle", container.GetId(), []string{"cat", "/root/value"}); code != 0 || string(output) != "snapshot\n" {
		t.Fatalf("live root = %q, exit %d", output, code)
	}
	stopContainer(t, client, "lifecycle", container.GetId())
	startContainer(t, client, "lifecycle", container.GetId())
	if output, code := execContainer(t, client, "lifecycle", container.GetId(), []string{"cat", "/root/value"}); code != 0 || string(output) != "snapshot\n" {
		t.Fatalf("persistent root = %q, exit %d", output, code)
	}
	stopContainer(t, client, "lifecycle", container.GetId())
	deleteContainer(t, client, "lifecycle", container.GetId())
	deleteNamespace(t, client, "lifecycle")
}

func testNamedVolume(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespace(t, client, "volume-owner", 128<<20)
	ensureNamespace(t, client, "volume-other", 128<<20)
	pullImage(t, client, "volume-owner")
	pullImage(t, client, "volume-other")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	createVolume(t, client, "volume-owner", "data")
	mount := []*vmmdv1.VolumeMount{{VolumeId: "data", Destination: "/data"}}
	writer := createContainer(t, client, "volume-owner", "writer", []string{"/bin/sh", "-c", "echo durable > /data/value"}, mount)
	startContainer(t, client, "volume-owner", writer.GetId())
	waitContainer(t, client, "volume-owner", writer.GetId(), 0)
	deleteContainer(t, client, "volume-owner", writer.GetId())
	reader := createContainer(t, client, "volume-owner", "reader", []string{"cat", "/data/value"}, mount)
	startContainer(t, client, "volume-owner", reader.GetId())
	waitContainer(t, client, "volume-owner", reader.GetId(), 0)
	if logs := readLogs(t, client, "volume-owner", reader.GetId()); !bytes.Contains(logs, []byte("durable")) {
		t.Fatalf("volume contents = %q", logs)
	}
	deleteContainer(t, client, "volume-owner", reader.GetId())
	if _, err := client.CreateContainer(ctx, &vmmdv1.CreateContainerRequest{RequestId: "cross-mount", ActorNamespaceId: "volume-other", ContainerId: "cross", Image: workloadImage, Argv: []string{"true"}, Volumes: mount, LogLimitBytes: 1 << 20}); err == nil {
		t.Fatal("cross-namespace volume mount succeeded")
	}
	listed, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: "volume-other"})
	if err != nil || len(listed.GetContainers()) != 0 {
		t.Fatalf("other namespace containers = %+v, %v", listed, err)
	}
	deleteVolume(t, client, "volume-owner", "data")
	deleteNamespace(t, client, "volume-owner")
	deleteNamespace(t, client, "volume-other")
}

func testDiskQuota(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespace(t, client, "quota", 32<<20)
	ensureNamespace(t, client, "healthy", 64<<20)
	ensureNamespace(t, client, "shared-small", 2<<20)
	pullImage(t, client, "quota")
	pullImage(t, client, "healthy")
	if err := pullImageError(client, "shared-small"); err == nil || !strings.Contains(err.Error(), "ResourceExhausted") {
		t.Fatalf("shared content bypassed logical quota: %v", err)
	}
	createVolume(t, client, "quota", "data")
	full := createContainer(t, client, "quota", "full", []string{"dd", "if=/dev/zero", "of=/data/blob", "bs=1048576", "count=64"}, []*vmmdv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	startContainer(t, client, "quota", full.GetId())
	if code := waitContainer(t, client, "quota", full.GetId(), 1); code == 0 {
		t.Fatal("write beyond project quota succeeded")
	}
	healthy := createContainer(t, client, "healthy", "healthy", []string{"/bin/sh", "-c", "echo unaffected"}, nil)
	startContainer(t, client, "healthy", healthy.GetId())
	waitContainer(t, client, "healthy", healthy.GetId(), 0)
	if logs := readLogs(t, client, "healthy", healthy.GetId()); !bytes.Contains(logs, []byte("unaffected")) {
		t.Fatalf("healthy namespace logs = %q", logs)
	}
	deleteContainer(t, client, "quota", full.GetId())
	deleteContainer(t, client, "healthy", healthy.GetId())
	deleteVolume(t, client, "quota", "data")
	deleteNamespace(t, client, "quota")
	deleteNamespace(t, client, "healthy")
	deleteNamespace(t, client, "shared-small")
}

func testSameNamespace(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespace(t, client, "same", 128<<20)
	pullImage(t, client, "same")
	server := createContainer(t, client, "same", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "same", "caller", idleProcess(), nil)
	startContainer(t, client, "same", server.GetId())
	startContainer(t, client, "same", caller.GetId())
	if output, code := execContainer(t, client, "same", caller.GetId(), []string{"sh", "-c", "printf same | nc -w 5 server.same.agent 8080"}); code != 0 || string(output) != "same" {
		t.Fatalf("same namespace request = %q, exit %d", output, code)
	}
	stopDeleteContainer(t, client, "same", caller.GetId())
	stopDeleteContainer(t, client, "same", server.GetId())
	deleteNamespace(t, client, "same")
}

func testDefaultIsolation(t *testing.T, client vmmdv1.VMMDServiceClient) {
	for _, namespaceID := range []string{"isolated-source", "isolated-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		pullImage(t, client, namespaceID)
	}
	server := createContainer(t, client, "isolated-destination", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "isolated-source", "caller", idleProcess(), nil)
	startContainer(t, client, "isolated-destination", server.GetId())
	startContainer(t, client, "isolated-source", caller.GetId())
	address := containerAddress(t, client, "isolated-destination", server.GetId())
	if _, code := execContainer(t, client, "isolated-source", caller.GetId(), []string{"nc", "-z", "-w", "2", address, "8080"}); code == 0 {
		t.Fatal("cross-namespace connection succeeded without a link")
	}
	stopDeleteContainer(t, client, "isolated-source", caller.GetId())
	stopDeleteContainer(t, client, "isolated-destination", server.GetId())
	deleteNamespace(t, client, "isolated-source")
	deleteNamespace(t, client, "isolated-destination")
}

func testPartialLink(t *testing.T, client vmmdv1.VMMDServiceClient) {
	for _, namespaceID := range []string{"link-source", "link-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		pullImage(t, client, namespaceID)
	}
	server := createContainer(t, client, "link-destination", "server", []string{"sh", "-c", "nc -lk -p 8080 -e /bin/cat & nc -lk -p 9090 -e /bin/cat & wait"}, nil)
	other := createContainer(t, client, "link-destination", "other", echoServer(8080), nil)
	caller := createContainer(t, client, "link-source", "caller", idleProcess(), nil)
	unselected := createContainer(t, client, "link-source", "unselected", idleProcess(), nil)
	for _, item := range []struct{ namespace, id string }{{"link-destination", server.GetId()}, {"link-destination", other.GetId()}, {"link-source", caller.GetId()}, {"link-source", unselected.GetId()}} {
		startContainer(t, client, item.namespace, item.id)
	}
	applyLink(t, client, &vmmdv1.NetworkLink{Id: "partial", SourceNamespaceId: "link-source", DestinationNamespaceId: "link-destination", Source: idsSelector(caller.GetId()), Destination: idsSelector(server.GetId()), TcpPorts: []uint32{8080}, DnsAlias: "service"})
	if output, code := execContainer(t, client, "link-source", caller.GetId(), []string{"sh", "-c", "printf linked | nc -w 5 service.link-source.agent 8080"}); code != 0 || string(output) != "linked" {
		t.Fatalf("allowed link = %q, exit %d", output, code)
	}
	serverIP := containerAddress(t, client, "link-destination", server.GetId())
	otherIP := containerAddress(t, client, "link-destination", other.GetId())
	for _, denial := range []struct{ container, address, port string }{{caller.GetId(), serverIP, "9090"}, {caller.GetId(), otherIP, "8080"}, {unselected.GetId(), serverIP, "8080"}} {
		if _, code := execContainer(t, client, "link-source", denial.container, []string{"nc", "-z", "-w", "2", denial.address, denial.port}); code == 0 {
			t.Fatalf("selector/port denial failed for %+v", denial)
		}
	}
	for _, item := range []struct{ namespace, id string }{{"link-source", unselected.GetId()}, {"link-source", caller.GetId()}, {"link-destination", other.GetId()}, {"link-destination", server.GetId()}} {
		stopDeleteContainer(t, client, item.namespace, item.id)
	}
	deleteLink(t, client, "partial")
	deleteNamespace(t, client, "link-source")
	deleteNamespace(t, client, "link-destination")
}

func testLinkRevoke(t *testing.T, client vmmdv1.VMMDServiceClient) {
	for _, namespaceID := range []string{"revoke-source", "revoke-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		pullImage(t, client, namespaceID)
	}
	server := createContainer(t, client, "revoke-destination", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "revoke-source", "caller", idleProcess(), nil)
	startContainer(t, client, "revoke-destination", server.GetId())
	startContainer(t, client, "revoke-source", caller.GetId())
	applyLink(t, client, &vmmdv1.NetworkLink{Id: "revocable", SourceNamespaceId: "revoke-source", DestinationNamespaceId: "revoke-destination", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "service"})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := client.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Start{Start: &vmmdv1.ExecStart{RequestId: "revoke-exec", ActorNamespaceId: "revoke-source", ContainerId: caller.GetId(), Argv: []string{"nc", "service.revoke-source.agent", "8080"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Stdin{Stdin: []byte("before")}}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || string(response.GetStdout()) != "before" {
		t.Fatalf("established link = %q, %v", response.GetStdout(), err)
	}
	deleteLink(t, client, "revocable")
	_ = stream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Stdin{Stdin: []byte("after")}})
	if response, err := stream.Recv(); err == nil && bytes.Contains(response.GetStdout(), []byte("after")) {
		t.Fatal("established connection remained usable after link deletion")
	}
	stopDeleteContainer(t, client, "revoke-source", caller.GetId())
	stopDeleteContainer(t, client, "revoke-destination", server.GetId())
	deleteNamespace(t, client, "revoke-source")
	deleteNamespace(t, client, "revoke-destination")
}

func testEgress(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespaceWithEgress(t, client, "egress-deny", 128<<20, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL)
	ensureNamespaceWithEgress(t, client, "egress-public", 128<<20, vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET)
	for _, namespaceID := range []string{"egress-deny", "egress-public"} {
		pullImage(t, client, namespaceID)
	}
	deny := createContainer(t, client, "egress-deny", "caller", idleProcess(), nil)
	public := createContainer(t, client, "egress-public", "caller", idleProcess(), nil)
	startContainer(t, client, "egress-deny", deny.GetId())
	startContainer(t, client, "egress-public", public.GetId())
	if _, code := execContainer(t, client, "egress-deny", deny.GetId(), []string{"wget", "-qO-", "-T", "3", "https://example.com"}); code == 0 {
		t.Fatal("DENY_ALL reached the public internet")
	}
	if output, code := execContainer(t, client, "egress-public", public.GetId(), []string{"sh", "-c", "for attempt in 1 2 3; do wget -qO- -T 10 https://example.com && exit 0; done; exit 1"}); code != 0 || !bytes.Contains(output, []byte("Example Domain")) {
		t.Fatalf("PUBLIC_INTERNET request = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"nc", "-z", "-w", "2", "192.168.88.2", "53"}); code == 0 {
		t.Fatal("PUBLIC_INTERNET reached the LAN resolver directly")
	}
	stopDeleteContainer(t, client, "egress-deny", deny.GetId())
	stopDeleteContainer(t, client, "egress-public", public.GetId())
	deleteNamespace(t, client, "egress-deny")
	deleteNamespace(t, client, "egress-public")
}

func testPortForward(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespace(t, client, "forward", 128<<20)
	pullImage(t, client, "forward")
	server := createContainer(t, client, "forward", "server", echoServer(8080), nil)
	startContainer(t, client, "forward", server.GetId())
	waitForContainerPort(t, client, "forward", server.GetId(), 8080)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.ForwardTCP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_Header{Header: &vmmdv1.ForwardTCPHeader{ActorNamespaceId: "forward", ContainerId: server.GetId(), Port: 8080}}}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("forward-data-"), 8192)
	for len(payload) != 0 {
		count := min(len(payload), 32<<10)
		if err := stream.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_Data{Data: payload[:count]}}); err != nil {
			t.Fatal(err)
		}
		payload = payload[count:]
	}
	if err := stream.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}}); err != nil {
		t.Fatal(err)
	}
	var received bytes.Buffer
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if response.GetCloseRead() {
			break
		}
		received.Write(response.GetData())
	}
	want := bytes.Repeat([]byte("forward-data-"), 8192)
	if !bytes.Equal(received.Bytes(), want) {
		t.Fatalf("forwarded %d bytes, want %d", received.Len(), len(want))
	}
	stopDeleteContainer(t, client, "forward", server.GetId())
	deleteNamespace(t, client, "forward")
}

func testDockerfileBuild(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ensureNamespace(t, client, "dockerfile-build", 768<<20)
	tool := buildShell(t)
	contextArchive := dockerfileContext(t, tool)
	secret := []byte("build-secret-7a5f07acbcaf")
	image := "agent.local/dockerfile-result:latest"
	first := buildImage(t, client, "dockerfile-build", image, contextArchive, secret)
	second := buildImage(t, client, "dockerfile-build", image, contextArchive, secret)
	if first.digest != second.digest {
		t.Fatalf("cached build digest changed: %s != %s", first.digest, second.digest)
	}
	if !slices.Contains(second.progress, "namespace cache hit") {
		t.Fatalf("second build progress = %q", second.progress)
	}
	container := createContainerWithImage(t, client, "dockerfile-build", "result", image, []string{"/bin/result", "--assert-absent", string(secret)})
	startContainer(t, client, "dockerfile-build", container.GetId())
	waitContainer(t, client, "dockerfile-build", container.GetId(), 0)
	if output := readLogs(t, client, "dockerfile-build", container.GetId()); string(output) != "build-ok\n" {
		t.Fatalf("built image output = %q", output)
	}
	deleteContainer(t, client, "dockerfile-build", container.GetId())
	deleteNamespace(t, client, "dockerfile-build")
}

func testParallelAgents(t *testing.T, client vmmdv1.VMMDServiceClient) {
	namespaces := []string{"parallel-a", "parallel-b"}
	for _, namespaceID := range namespaces {
		ensureNamespace(t, client, namespaceID, 1<<30)
	}
	contextArchive := dockerfileContext(t, buildShell(t))
	secret := []byte("parallel-build-secret-e291d1")
	type outcome struct {
		namespaceID string
		result      imageBuildResult
		err         error
	}
	outcomes := make(chan outcome, len(namespaces))
	for _, namespaceID := range namespaces {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			result, err := buildImageRequest(ctx, client, namespaceID, "agent.local/shared-name:latest", "build-ok", vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, secret)
			outcomes <- outcome{namespaceID: namespaceID, result: result, err: err}
		}()
	}
	for range namespaces {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatalf("build %s: %v", outcome.namespaceID, outcome.err)
		}
		if slices.Contains(outcome.result.progress, "namespace cache hit") {
			t.Fatalf("namespace %s reused another namespace's cache", outcome.namespaceID)
		}
	}
	for _, namespaceID := range namespaces {
		container := createContainerWithImage(t, client, namespaceID, "result", "agent.local/shared-name:latest", []string{"/bin/result"})
		startContainer(t, client, namespaceID, container.GetId())
	}
	for _, namespaceID := range namespaces {
		waitContainer(t, client, namespaceID, "result", 0)
		if output := readLogs(t, client, namespaceID, "result"); string(output) != "build-ok\n" {
			t.Fatalf("namespace %s output = %q", namespaceID, output)
		}
		deleteContainer(t, client, namespaceID, "result")
		createContainerWithImage(t, client, namespaceID, "worker", "agent.local/shared-name:latest", []string{"/bin/result", "--hold"})
		startContainer(t, client, namespaceID, "worker")
	}
	for index := len(namespaces) - 1; index >= 0; index-- {
		stopDeleteContainer(t, client, namespaces[index], "worker")
		deleteNamespace(t, client, namespaces[index])
	}
}

func testCancellationCleanup(t *testing.T, client vmmdv1.VMMDServiceClient) {
	namespaceID := "cancel-build"
	ensureNamespace(t, client, namespaceID, 768<<20)
	contextArchive := dockerfileContext(t, buildShell(t))
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.BuildImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	header := &vmmdv1.BuildImageHeader{
		RequestId: "cancel-build", ActorNamespaceId: namespaceID, DockerfilePath: "Dockerfile",
		Image: "agent.local/cancelled:latest", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NAMESPACE_EGRESS,
		BuildArgs: []*vmmdv1.BuildArgument{{Name: "MESSAGE", Value: "build-cancel"}}, Secrets: []*vmmdv1.BuildSecret{{Id: "token"}},
	}
	if err := sendBuildInput(stream, header, contextArchive, []byte("build-cancel-secret-053f")); err != nil {
		t.Fatal(err)
	}
	started := false
	for !started {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		started = response.GetProgress() == "builder task started"
	}
	cancelledAt := time.Now()
	cancel()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("cancelled build completed without an error")
	}
	if elapsed := time.Since(cancelledAt); elapsed > 20*time.Second {
		t.Fatalf("build cancellation took %s", elapsed)
	}
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer checkCancel()
	listed, err := client.ListContainers(checkCtx, &vmmdv1.ListContainersRequest{ActorNamespaceId: namespaceID})
	if err != nil || len(listed.GetContainers()) != 0 {
		t.Fatalf("containers after cancellation = %+v, %v", listed.GetContainers(), err)
	}
	if _, err := client.CreateContainer(checkCtx, &vmmdv1.CreateContainerRequest{
		RequestId: "cancelled-image", ActorNamespaceId: namespaceID, ContainerId: "partial", Image: "agent.local/cancelled:latest", Argv: []string{"/bin/result"}, LogLimitBytes: 1 << 20,
	}); err == nil {
		t.Fatal("cancelled image was imported")
	}
	deleteNamespaceEventually(t, client, namespaceID)
	ensureNamespace(t, client, "quota-build", 4<<20)
	quotaCtx, quotaCancel := context.WithTimeout(context.Background(), time.Minute)
	_, err = buildImageRequest(quotaCtx, client, "quota-build", "agent.local/quota:latest", "build-ok", vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, []byte("quota-secret"))
	quotaCancel()
	if err == nil {
		t.Fatal("build exceeded its namespace disk quota")
	}
	deleteNamespaceEventually(t, client, "quota-build")
}

func testVMRestart(t *testing.T, harness *hostHarness) {
	client := harness.vmmd(t)
	for _, namespaceID := range []string{"restart-source", "restart-destination"} {
		ensureNamespace(t, client, namespaceID, 256<<20)
		pullImage(t, client, namespaceID)
	}
	createVolume(t, client, "restart-source", "data")
	caller := createContainer(t, client, "restart-source", "caller", idleProcess(), []*vmmdv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	server := createContainer(t, client, "restart-destination", "server", idleProcess(), nil)
	startContainer(t, client, "restart-source", caller.GetId())
	startContainer(t, client, "restart-destination", server.GetId())
	startEchoServer(t, client, "restart-destination", server.GetId(), 8080)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "echo restart-durable > /data/value; echo root-durable > /root/value; sync; cat /data/value"}); code != 0 || string(output) != "restart-durable\n" {
		t.Fatalf("fsync volume write = %q, exit %d", output, code)
	}
	link := &vmmdv1.NetworkLink{Id: "restart-link", SourceNamespaceId: "restart-source", DestinationNamespaceId: "restart-destination", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "service"}
	applyLink(t, client, link)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "printf before | nc -w 5 service.restart-source.agent 8080"}); code != 0 || string(output) != "before" {
		t.Fatalf("link before restart = %q, exit %d", output, code)
	}
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer streamCancel()
	stream, err := client.Exec(streamCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Start{Start: &vmmdv1.ExecStart{RequestId: "restart-connection", ActorNamespaceId: "restart-source", ContainerId: caller.GetId(), Argv: []string{"nc", "service.restart-source.agent", "8080"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Stdin{Stdin: []byte("open")}}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || string(response.GetStdout()) != "open" {
		t.Fatalf("connection before restart = %q, %v", response.GetStdout(), err)
	}
	tty, err := client.Exec(streamCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tty.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Start{Start: &vmmdv1.ExecStart{RequestId: "restart-tty", ActorNamespaceId: "restart-source", ContainerId: caller.GetId(), Argv: []string{"sh"}, Tty: true, Rows: 24, Columns: 80}}}); err != nil {
		t.Fatal(err)
	}
	if err := tty.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Stdin{Stdin: []byte("echo tty-open\n")}}); err != nil {
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
	harness.stop(t, hostv1.StopMode_STOP_MODE_GRACEFUL)
	harness.start(t)
	for name, oldStream := range map[string]grpc.BidiStreamingClient[vmmdv1.ExecRequest, vmmdv1.ExecResponse]{"exec connection": stream, "TTY": tty} {
		marker := []byte("recovered-" + strings.ReplaceAll(name, " ", "-"))
		_ = oldStream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Stdin{Stdin: append([]byte("echo "), append(marker, '\n')...)}})
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
	client = harness.vmmd(t)
	for _, item := range []struct{ namespaceID, containerID string }{{"restart-source", caller.GetId()}, {"restart-destination", server.GetId()}} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		response, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: item.namespaceID, ContainerId: item.containerID})
		cancel()
		if err != nil || response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			t.Fatalf("process restored after restart: %+v, %v", response.GetContainer(), err)
		}
	}
	startContainer(t, client, "restart-source", caller.GetId())
	startContainer(t, client, "restart-destination", server.GetId())
	startEchoServer(t, client, "restart-destination", server.GetId(), 8080)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "cat /data/value /root/value"}); code != 0 || string(output) != "restart-durable\nroot-durable\n" {
		t.Fatalf("filesystem and volume after restart = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"nc", "-z", "-w", "2", containerAddress(t, client, "restart-destination", server.GetId()), "8080"}); code == 0 {
		t.Fatal("network policy survived without host replay")
	}
	applyLink(t, client, link)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "printf replayed | nc -w 5 service.restart-source.agent 8080"}); code != 0 || string(output) != "replayed" {
		t.Fatalf("replayed link = %q, exit %d", output, code)
	}
	deleteLink(t, client, link.GetId())
	stopDeleteContainer(t, client, "restart-source", caller.GetId())
	stopDeleteContainer(t, client, "restart-destination", server.GetId())
	deleteVolume(t, client, "restart-source", "data")
	deleteNamespace(t, client, "restart-source")
	deleteNamespace(t, client, "restart-destination")
}

func testForcedVMStop(t *testing.T, harness *hostHarness) {
	client := harness.vmmd(t)
	for _, namespaceID := range []string{"force-source", "force-destination"} {
		ensureNamespace(t, client, namespaceID, 256<<20)
		pullImage(t, client, namespaceID)
	}
	createVolume(t, client, "force-source", "data")
	caller := createContainer(t, client, "force-source", "caller", idleProcess(), []*vmmdv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	server := createContainer(t, client, "force-destination", "server", idleProcess(), nil)
	startContainer(t, client, "force-source", caller.GetId())
	startContainer(t, client, "force-destination", server.GetId())
	startEchoServer(t, client, "force-destination", server.GetId(), 8080)
	if _, code := execContainer(t, client, "force-source", caller.GetId(), []string{"sh", "-c", "echo force-durable > /data/value; sync"}); code != 0 {
		t.Fatalf("fsync before forced stop exited %d", code)
	}
	if _, code := execContainer(t, client, "force-destination", server.GetId(), []string{"sync"}); code != 0 {
		t.Fatalf("server fsync before forced stop exited %d", code)
	}
	applyLink(t, client, &vmmdv1.NetworkLink{Id: "force-link", SourceNamespaceId: "force-source", DestinationNamespaceId: "force-destination", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "service"})
	harness.stop(t, hostv1.StopMode_STOP_MODE_FORCE)
	harness.start(t)
	client = harness.vmmd(t)
	startContainer(t, client, "force-source", caller.GetId())
	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), 30*time.Second)
	response, err := client.InspectContainer(inspectCtx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: "force-destination", ContainerId: server.GetId()})
	inspectCancel()
	if err != nil || response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatalf("process restored after forced stop: %+v, %v", response.GetContainer(), err)
	}
	startContainer(t, client, "force-destination", server.GetId())
	startEchoServer(t, client, "force-destination", server.GetId(), 8080)
	if output, code := execContainer(t, client, "force-source", caller.GetId(), []string{"cat", "/data/value"}); code != 0 || string(output) != "force-durable\n" {
		t.Fatalf("fsync data after forced stop = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "force-source", caller.GetId(), []string{"nc", "-z", "-w", "2", containerAddress(t, client, "force-destination", server.GetId()), "8080"}); code == 0 {
		t.Fatal("network policy survived forced restart")
	}
	stopDeleteContainer(t, client, "force-source", caller.GetId())
	stopDeleteContainer(t, client, "force-destination", server.GetId())
	deleteVolume(t, client, "force-source", "data")
	deleteNamespace(t, client, "force-source")
	deleteNamespace(t, client, "force-destination")
}

func testResidualScan(t *testing.T, client vmmdv1.VMMDServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.Health(ctx, &vmmdv1.HealthRequest{}); err != nil {
		t.Fatal(err)
	}
	for _, namespaceID := range []string{
		"lifecycle", "volume-owner", "volume-other", "quota", "healthy", "shared-small", "same",
		"isolated-source", "isolated-destination", "link-source", "link-destination", "revoke-source",
		"revoke-destination", "egress-deny", "egress-public", "forward", "dockerfile-build", "parallel-a",
		"parallel-b", "cancel-build", "quota-build", "restart-source", "restart-destination", "force-source", "force-destination",
	} {
		response, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: namespaceID})
		if err == nil && len(response.GetContainers()) != 0 {
			t.Fatalf("residual namespace %q has %d visible containers", namespaceID, len(response.GetContainers()))
		}
	}
}

const workloadImage = "docker.io/library/alpine@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b"

type hostHarness struct {
	client      hostv1.HostServiceClient
	connection  *grpc.ClientConn
	process     *exec.Cmd
	instance    string
	relaySocket string
	starts      int
}

func startHost(t *testing.T) *hostHarness {
	return startHostWithState(t, 1<<30)
}

func startHostWithState(t *testing.T, stateBytes uint64) *hostHarness {
	t.Helper()
	bundle := os.Getenv("AGENT_VMM_E2E_BUNDLE")
	if bundle == "" {
		bundle = "/tmp/agent-vmm-e2e-bundle"
	}
	directory := t.TempDir()
	binary := os.Getenv("AGENT_VMM_E2E_HOST_BINARY")
	if binary == "" {
		binary = filepath.Join(directory, "agent-vmm-host")
		runCommand(t, 2*time.Minute, "go", "build", "-tags=e2e", "-o", binary, "../../cmd/agent-vmm-host")
		runCommand(t, time.Minute, "codesign", "--force", "--sign", "-", "--entitlements", "../../build/host/vz.entitlements.plist", binary)
	}
	control := filepath.Join(directory, "control.sock")
	relay := filepath.Join(directory, "relay.sock")
	instance := filepath.Join(directory, "instance")
	serial := filepath.Join(directory, "serial.log")
	process := exec.Command(binary,
		"--bundle", bundle, "--instance", instance, "--state-size", fmt.Sprint(stateBytes),
		"--memory", "4294967296",
		"--serial-log", serial, "--control-socket", control, "--relay-socket", relay,
	)
	process.Stdout = os.Stdout
	process.Stderr = os.Stderr
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("serial log tail:\n%s", readFileTail(serial, 64<<10))
		}
	})
	t.Cleanup(func() {
		if process.ProcessState == nil || !process.ProcessState.Exited() {
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
	})
	waitForPath(t, control, 10*time.Second)
	connection, err := grpc.NewClient("passthrough:///host", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", control)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return &hostHarness{client: hostv1.NewHostServiceClient(connection), connection: connection, process: process, instance: instance, relaySocket: relay}
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
	response, err := harness.client.StartVM(ctx, &hostv1.StartVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if harness.starts == 0 {
		t.Logf("E2E_COLD_START_MS=%d", time.Since(started).Milliseconds())
	}
	harness.starts++
	return response.GetVm()
}

func (harness *hostHarness) stop(t *testing.T, mode hostv1.StopMode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := harness.client.StopVM(ctx, &hostv1.StopVMRequest{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetVm().GetState() != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM() = %+v", response.GetVm())
	}
}

func (harness *hostHarness) vmmd(t *testing.T) vmmdv1.VMMDServiceClient {
	t.Helper()
	waitForPath(t, harness.relaySocket, 10*time.Second)
	connection, err := grpc.NewClient("passthrough:///vmmd", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", harness.relaySocket)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return vmmdv1.NewVMMDServiceClient(connection)
}

func ensureNamespace(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID string, diskBytes uint64) {
	ensureNamespaceWithEgress(t, client, namespaceID, diskBytes, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL)
}

func ensureNamespaceWithEgress(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID string, diskBytes uint64, egress vmmdv1.EgressMode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.EnsureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{
		RequestId: "ensure-" + namespaceID, NamespaceId: namespaceID,
		Limits:     &vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 3 << 30, Pids: 512, DiskBytes: diskBytes},
		EgressMode: egress,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetNamespace().GetProjectId() == 0 || response.GetNamespace().GetSubnet() == "" {
		t.Fatalf("namespace allocation = %+v", response.GetNamespace())
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: "cleanup-" + namespaceID, NamespaceId: namespaceID})
	})
}

func createVolume(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, volumeID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateVolume(ctx, &vmmdv1.CreateVolumeRequest{RequestId: "create-" + namespaceID + "-" + volumeID, ActorNamespaceId: namespaceID, VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: "cleanup-" + namespaceID + "-" + volumeID, ActorNamespaceId: namespaceID, VolumeId: volumeID})
	})
}

func deleteVolume(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, volumeID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: "delete-" + namespaceID + "-" + volumeID, ActorNamespaceId: namespaceID, VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
}

func idleProcess() []string {
	return []string{"tail", "-f", "/dev/null"}
}

func echoServer(port uint32) []string {
	return []string{"nc", "-lk", "-p", fmt.Sprint(port), "-e", "/bin/cat"}
}

func allSelector() *vmmdv1.ContainerSelector {
	return &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_AllContainers{AllContainers: true}}
}

func idsSelector(ids ...string) *vmmdv1.ContainerSelector {
	return &vmmdv1.ContainerSelector{Selection: &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: ids}}}
}

func applyLink(t *testing.T, client vmmdv1.VMMDServiceClient, link *vmmdv1.NetworkLink) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.ApplyNetworkLink(ctx, &vmmdv1.ApplyNetworkLinkRequest{RequestId: "apply-" + link.GetId(), Link: link}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: "cleanup-" + link.GetId(), LinkId: link.GetId()})
	})
}

func deleteLink(t *testing.T, client vmmdv1.VMMDServiceClient, linkID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: "delete-" + linkID, LinkId: linkID}); err != nil {
		t.Fatal(err)
	}
}

func containerAddress(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) string {
	t.Helper()
	output, code := execContainer(t, client, namespaceID, containerID, []string{"sh", "-c", "ip -4 -o addr show dev eth0 | awk '{split($4,a,\"/\"); print a[1]}'"})
	address := strings.TrimSpace(string(output))
	if code != 0 || net.ParseIP(address) == nil {
		t.Fatalf("container address = %q, exit %d", output, code)
	}
	return address
}

func waitForContainerPort(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string, port uint32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, code := execContainer(t, client, namespaceID, containerID, []string{"nc", "-z", "-w", "1", "127.0.0.1", fmt.Sprint(port)}); code == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("container %s/%s did not listen on TCP %d", namespaceID, containerID, port)
}

func startEchoServer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string, port uint32) {
	t.Helper()
	if output, code := execContainer(t, client, namespaceID, containerID, []string{"sh", "-c", fmt.Sprintf("nc -lk -p %d -e /bin/cat >/dev/null 2>&1 &", port)}); code != 0 {
		t.Fatalf("start echo server = %q, exit %d", output, code)
	}
	waitForContainerPort(t, client, namespaceID, containerID, port)
}

func stopDeleteContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) {
	t.Helper()
	stopContainer(t, client, namespaceID, containerID)
	deleteContainerEventually(t, client, namespaceID, containerID)
}

func pullImage(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID string) {
	t.Helper()
	var digest string
	var err error
	for attempt := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		digest, err = pullImageAttempt(ctx, client, namespaceID, fmt.Sprintf("pull-%s-%d", namespaceID, attempt))
		cancel()
		if err == nil || strings.Contains(err.Error(), "429 Too Many Requests") {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("pull digest = %q", digest)
	}
}

func pullImageAttempt(ctx context.Context, client vmmdv1.VMMDServiceClient, namespaceID, requestID string) (string, error) {
	stream, err := client.PullImage(ctx, &vmmdv1.PullImageRequest{RequestId: requestID, ActorNamespaceId: namespaceID, Reference: workloadImage})
	if err != nil {
		return "", err
	}
	var digest string
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return digest, nil
		}
		if err != nil {
			return "", err
		}
		if response.Digest != nil {
			digest = response.GetDigest()
		}
	}
}

func pullImageError(client vmmdv1.VMMDServiceClient, namespaceID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stream, err := client.PullImage(ctx, &vmmdv1.PullImageRequest{RequestId: "pull-" + namespaceID, ActorNamespaceId: namespaceID, Reference: workloadImage})
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
	}
}

func createContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string, argv []string, volumes []*vmmdv1.VolumeMount) *vmmdv1.Container {
	return createContainerWithImageAndVolumes(t, client, namespaceID, containerID, workloadImage, argv, volumes)
}

func createContainerWithImage(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID, image string, argv []string) *vmmdv1.Container {
	return createContainerWithImageAndVolumes(t, client, namespaceID, containerID, image, argv, nil)
}

func createContainerWithImageAndVolumes(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID, image string, argv []string, volumes []*vmmdv1.VolumeMount) *vmmdv1.Container {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.CreateContainer(ctx, &vmmdv1.CreateContainerRequest{
		RequestId: "create-" + namespaceID + "-" + containerID, ActorNamespaceId: namespaceID,
		ContainerId: containerID, Image: image, Argv: argv, Volumes: volumes, LogLimitBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		seconds := uint32(1)
		_, _ = client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: "cleanup-stop-" + namespaceID + "-" + containerID, ActorNamespaceId: namespaceID, ContainerId: containerID, TimeoutSeconds: &seconds})
		_, _ = client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: "cleanup-delete-" + namespaceID + "-" + containerID, ActorNamespaceId: namespaceID, ContainerId: containerID})
	})
	return response.GetContainer()
}

func buildShell(t *testing.T) string {
	t.Helper()
	output := filepath.Join(t.TempDir(), "buildshell")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags=-s -w -buildid=", "-o", output, "./testdata/buildshell")
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	if bytes, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build E2E helper: %v: %s", err, bytes)
	}
	return output
}

func dockerfileContext(t *testing.T, tool string) []byte {
	t.Helper()
	binary, err := os.ReadFile(tool)
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := []byte("FROM scratch AS build\nADD buildshell /bin/sh\nARG MESSAGE\nRUN --mount=type=secret,id=token /bin/sh -c build\nFROM scratch\nCOPY --from=build /bin/sh /bin/result\nCOPY --from=build /result /result\nENTRYPOINT [\"/bin/result\"]\n")
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, file := range []struct {
		name string
		mode int64
		data []byte
	}{{"Dockerfile", 0o644, dockerfile}, {"buildshell", 0o755, binary}} {
		if err := writer.WriteHeader(&tar.Header{Name: file.name, Mode: file.mode, Size: int64(len(file.data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(file.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

type imageBuildResult struct {
	digest   string
	progress []string
}

func buildImage(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, image string, contextArchive, secret []byte) imageBuildResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := buildImageRequest(ctx, client, namespaceID, image, "build-ok", vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, secret)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(result.digest, "sha256:") {
		t.Fatalf("build digest = %q", result.digest)
	}
	return result
}

func buildImageRequest(ctx context.Context, client vmmdv1.VMMDServiceClient, namespaceID, image, message string, networkMode vmmdv1.BuildNetworkMode, contextArchive, secret []byte) (imageBuildResult, error) {
	stream, err := client.BuildImage(ctx)
	if err != nil {
		return imageBuildResult{}, err
	}
	header := &vmmdv1.BuildImageHeader{
		RequestId: "build-" + namespaceID + "-" + time.Now().Format("150405.000000000"), ActorNamespaceId: namespaceID,
		DockerfilePath: "Dockerfile", Image: image, NetworkMode: networkMode,
		BuildArgs: []*vmmdv1.BuildArgument{{Name: "MESSAGE", Value: message}}, Secrets: []*vmmdv1.BuildSecret{{Id: "token"}},
	}
	if err := sendBuildInput(stream, header, contextArchive, secret); err != nil {
		return imageBuildResult{}, err
	}
	var result imageBuildResult
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return imageBuildResult{}, err
		}
		if response.GetProgress() != "" {
			result.progress = append(result.progress, response.GetProgress())
		}
		if response.GetResult() != nil {
			result.digest = response.GetResult().GetDigest()
		}
	}
	return result, nil
}

func sendBuildInput(stream grpc.BidiStreamingClient[vmmdv1.BuildImageRequest, vmmdv1.BuildImageResponse], header *vmmdv1.BuildImageHeader, contextArchive, secret []byte) error {
	if err := stream.Send(&vmmdv1.BuildImageRequest{Event: &vmmdv1.BuildImageRequest_Header{Header: header}}); err != nil {
		return err
	}
	for len(contextArchive) > 0 {
		count := min(len(contextArchive), 1<<20)
		chunk := &vmmdv1.BuildContextChunk{Data: contextArchive[:count], Final: count == len(contextArchive)}
		if err := stream.Send(&vmmdv1.BuildImageRequest{Event: &vmmdv1.BuildImageRequest_Context{Context: chunk}}); err != nil {
			return err
		}
		contextArchive = contextArchive[count:]
	}
	if err := stream.Send(&vmmdv1.BuildImageRequest{Event: &vmmdv1.BuildImageRequest_Secret{Secret: &vmmdv1.BuildSecretChunk{Id: "token", Data: secret, Final: true}}}); err != nil {
		return err
	}
	return stream.CloseSend()
}

func startContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.StartContainer(ctx, &vmmdv1.StartContainerRequest{RequestId: "start-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000"), ActorNamespaceId: namespaceID, ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("StartContainer() = %+v", response.GetContainer())
	}
}

func stopContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	timeout := uint32(3)
	response, err := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: "stop-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000"), ActorNamespaceId: namespaceID, ContainerId: containerID, TimeoutSeconds: &timeout})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetContainer().GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatalf("StopContainer() = %+v", response.GetContainer())
	}
}

func waitContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string, expected uint32) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.WaitContainer(ctx, &vmmdv1.WaitContainerRequest{ActorNamespaceId: namespaceID, ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	if expected == 0 && response.GetExitCode() != 0 {
		t.Fatalf("WaitContainer() exit = %d", response.GetExitCode())
	}
	return response.GetExitCode()
}

func deleteContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: "delete-" + namespaceID + "-" + containerID, ActorNamespaceId: namespaceID, ContainerId: containerID}); err != nil {
		t.Fatal(err)
	}
}

func deleteContainerEventually(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, lastErr = client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: fmt.Sprintf("delete-%d", time.Now().UnixNano()), ActorNamespaceId: namespaceID, ContainerId: containerID})
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

func deleteNamespace(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: "delete-" + namespaceID, NamespaceId: namespaceID}); err != nil {
		t.Fatal(err)
	}
}

func deleteNamespaceEventually(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, lastErr = client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: "delete-" + namespaceID, NamespaceId: namespaceID})
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("DeleteNamespace(%q): %v", namespaceID, lastErr)
}

func execContainer(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string, argv []string) ([]byte, uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	requestID := "exec-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000")
	if err := stream.Send(&vmmdv1.ExecRequest{Event: &vmmdv1.ExecRequest_Start{Start: &vmmdv1.ExecStart{RequestId: requestID, ActorNamespaceId: namespaceID, ContainerId: containerID, Argv: argv}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	var exitCode uint32
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return output.Bytes(), exitCode
		}
		if err != nil {
			t.Fatal(err)
		}
		output.Write(response.GetStdout())
		output.Write(response.GetStderr())
		if _, ok := response.Event.(*vmmdv1.ExecResponse_ExitCode); ok {
			exitCode = response.GetExitCode()
		}
	}
}

func readLogs(t *testing.T, client vmmdv1.VMMDServiceClient, namespaceID, containerID string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Logs(ctx, &vmmdv1.LogsRequest{ActorNamespaceId: namespaceID, ContainerId: containerID, TailBytes: 1 << 20})
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
