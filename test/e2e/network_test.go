//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

func testSameNamespace(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "same", 128<<20)
	importRegistryImage(t, client, "same")
	server := createContainer(t, client, "same", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "same", "caller", idleProcess(), nil)
	startContainer(t, client, "same", server.GetId())
	startContainer(t, client, "same", caller.GetId())
	if output, code := execContainer(t, client, "same", caller.GetId(), []string{"sh", "-c", "printf same | nc -w 5 server 8080"}); code != 0 || string(output) != "same" {
		t.Fatalf("same namespace request = %q, exit %d", output, code)
	}
	stopDeleteContainer(t, client, "same", caller.GetId())
	stopDeleteContainer(t, client, "same", server.GetId())
	deleteNamespace(t, client, "same")
}

func testDefaultIsolation(t *testing.T, client *productClient) {
	for _, namespaceID := range []string{"isolated-source", "isolated-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		importRegistryImage(t, client, namespaceID)
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

func testServiceRouteScope(t *testing.T, harness *hostHarness) {
	client, source, destination := serviceRouteRuntime(t, harness, "route-scope")
	for _, namespaceID := range []string{source, destination} {
		importRegistryImage(t, client, namespaceID)
	}
	server := createContainer(t, client, destination, "server", []string{"sh", "-c", "nc -lk -p 8080 -e /bin/cat & nc -lk -p 9090 -e /bin/cat & wait"}, nil)
	caller := createContainer(t, client, source, "caller", idleProcess(), nil)
	unselected := createContainer(t, client, source, "unselected", idleProcess(), nil)
	for _, item := range []struct{ namespace, id string }{{destination, server.GetId()}, {source, caller.GetId()}, {source, unselected.GetId()}} {
		startContainer(t, client, item.namespace, item.id)
	}
	revokeRoute := createServiceRouteAcross(t, client, source, destination, caller.GetId(), server.GetId(), "service")
	if output, code := execContainer(t, client, source, caller.GetId(), []string{"sh", "-c", "printf linked | nc -w 5 service 8080"}); code != 0 || string(output) != "linked" {
		t.Fatalf("allowed link = %q, exit %d", output, code)
	}
	for _, denial := range []struct{ container, port string }{{caller.GetId(), "9090"}, {unselected.GetId(), "8080"}} {
		if _, code := execContainer(t, client, source, denial.container, []string{"nc", "-z", "-w", "2", "service", denial.port}); code == 0 {
			t.Fatalf("service source/port denial failed for %+v", denial)
		}
	}
	revokeRoute()
	for _, item := range []struct{ namespace, id string }{{source, unselected.GetId()}, {source, caller.GetId()}, {destination, server.GetId()}} {
		stopDeleteContainer(t, client, item.namespace, item.id)
	}
}

func testServiceRouteRevoke(t *testing.T, harness *hostHarness) {
	client, source, destination := serviceRouteRuntime(t, harness, "route-revoke")
	for _, namespaceID := range []string{source, destination} {
		importRegistryImage(t, client, namespaceID)
	}
	server := createContainer(t, client, destination, "server", echoServer(8080), nil)
	caller := createContainer(t, client, source, "caller", idleProcess(), nil)
	startContainer(t, client, destination, server.GetId())
	startContainer(t, client, source, caller.GetId())
	revokeRoute := createServiceRouteAcross(t, client, source, destination, caller.GetId(), server.GetId(), "service")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := client.Exec(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: execStart(t, ctx, client, source, caller.GetId(), "revoke-exec", []string{"nc", "service", "8080"})}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("before")}}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || string(response.GetStdout()) != "before" {
		t.Fatalf("established link = %q, %v", response.GetStdout(), err)
	}
	revokeRoute()
	_ = stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("after")}})
	if response, err := stream.Recv(); err == nil && bytes.Contains(response.GetStdout(), []byte("after")) {
		t.Fatal("established connection remained usable after link deletion")
	}
	stopDeleteContainer(t, client, source, caller.GetId())
	stopDeleteContainer(t, client, destination, server.GetId())
}

func testEgress(t *testing.T, client *productClient) {
	ensureNamespaceWithEgress(t, client, "egress-deny", 128<<20, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
	ensureNamespaceWithEgress(t, client, "egress-public", 128<<20, hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET)
	for _, namespaceID := range []string{"egress-deny", "egress-public"} {
		importRegistryImage(t, client, namespaceID)
	}
	deny := createContainer(t, client, "egress-deny", "caller", idleProcess(), nil)
	public := createContainer(t, client, "egress-public", "caller", idleProcess(), nil)
	startContainer(t, client, "egress-deny", deny.GetId())
	startContainer(t, client, "egress-public", public.GetId())
	if _, code := execContainer(t, client, "egress-deny", deny.GetId(), []string{"wget", "-qO-", "-T", "3", "https://example.com"}); code == 0 {
		t.Fatal("DENY_ALL reached the public internet")
	}
	if _, code := execContainer(t, client, "egress-deny", deny.GetId(), []string{"nslookup", "example.com", "1.1.1.1"}); code == 0 {
		t.Fatal("DENY_ALL reached public DNS over UDP")
	}
	if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"sh", "-c", "for attempt in 1 2 3 4 5 6 7 8; do nslookup example.com && exit 0; sleep 3; done; exit 1"}); code != 0 {
		t.Fatal("PUBLIC_INTERNET could not use virtual DNS over UDP")
	}
	if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"sh", "-c", "for attempt in 1 2 3 4 5 6 7 8; do nslookup example.com 1.1.1.1 && exit 0; sleep 3; done; exit 1"}); code != 0 {
		t.Fatal("PUBLIC_INTERNET could not reach public DNS over UDP")
	}
	for _, address := range []string{"192.168.1.1", "169.254.169.254", "100.127.255.1"} {
		if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"nc", "-z", "-w", "2", address, "80"}); code == 0 {
			t.Fatalf("PUBLIC_INTERNET reached blocked destination %s", address)
		}
	}
	stopDeleteContainer(t, client, "egress-deny", deny.GetId())
	stopDeleteContainer(t, client, "egress-public", public.GetId())
	deleteNamespace(t, client, "egress-deny")
	deleteNamespace(t, client, "egress-public")
}

func testPortForward(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "forward", 128<<20)
	importRegistryImage(t, client, "forward")
	server := createContainer(t, client, "forward", "server", echoServer(8080), nil)
	startContainer(t, client, "forward", server.GetId())
	waitForContainerPort(t, client, "forward", server.GetId(), 8080)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.ForwardTCP(ctx, "forward")
	if err != nil {
		t.Fatal(err)
	}
	running, err := client.GetContainer(ctx, "forward", &hostv1.GetContainerRequest{ContainerId: server.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Header{Header: &hostv1.ForwardTCPHeader{ContainerId: server.GetId(), Port: 8080, ContainerInstanceId: running.GetContainer().GetInstanceId()}}}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("forward-data-"), 8192)
	for len(payload) != 0 {
		count := min(len(payload), 32<<10)
		if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Data{Data: payload[:count]}}); err != nil {
			t.Fatal(err)
		}
		payload = payload[count:]
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}}); err != nil {
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

func echoServer(port uint32) []string {
	return []string{"nc", "-lk", "-p", fmt.Sprint(port), "-e", "/bin/cat"}
}

func serviceRouteRuntime(t *testing.T, harness *hostHarness, prefix string) (*productClient, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	controller := &remoteE2EController{sessions: make(chan remoteE2ESession, 4), finish: make(chan struct{}), connections: make(chan int, 8), failures: make(chan string, 8)}
	endpoint, roots, stopController := startRemoteController(t, controller)
	t.Cleanup(stopController)
	pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: isolationLimits(256, 2<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, StopGrace: durationpb.New(2 * time.Second)}
	_, err := harness.client.ApplyRemoteRegistration(ctx, &hostv1.ApplyRemoteRegistrationRequest{Registration: &hostv1.RemoteRegistrationConnection{RequestId: prefix, RegistrationId: prefix, Endpoint: endpoint, DisplayName: prefix, Pool: pool}, Credential: []byte("credential")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: prefix}); err != nil {
		t.Fatal(err)
	}
	controller.registrationID = prefix
	connectorContext, stopConnector := context.WithCancel(ctx)
	t.Cleanup(stopConnector)
	connectorDone := make(chan error, 1)
	go func() {
		connectorDone <- remoteconnector.Run(connectorContext, remoteconnector.Config{RegistrationID: prefix, RemoteComputeSocket: harness.remoteComputeSocket, Transport: remoteconnector.ExternalTransport{Endpoint: endpoint, Credentials: &e2eCredentialStore{value: []byte("remote-e2e")}, Roots: roots}})
	}()
	client := newProductClient(harness.client)
	client.archiveDirectory, client.archiveURL = harness.runtimeClient.archiveDirectory, harness.runtimeClient.archiveURL
	source, destination := prefix+"-source", prefix+"-destination"
	t.Cleanup(func() {
		client.close()
		controller.retired.Store("remote-a", true)
		controller.retired.Store("remote-b", true)
		close(controller.finish)
		cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			current, err := harness.client.GetRemoteRegistration(cleanupContext, &hostv1.GetRemoteRegistrationRequest{RegistrationId: prefix})
			if err == nil && len(current.GetRegistration().GetAllocations()) == 0 {
				break
			}
			select {
			case failure := <-controller.failures:
				t.Errorf("service route cleanup failed: %s", failure)
				stopConnector()
				return
			case <-ticker.C:
			case <-cleanupContext.Done():
				t.Error("service route allocations did not clean up", err)
				stopConnector()
				return
			}
		}
		stopConnector()
		select {
		case <-connectorDone:
		case <-cleanupContext.Done():
			t.Error("service route connector did not stop")
		}
	})
	for len(client.sessions) != 2 {
		select {
		case session := <-controller.sessions:
			connection, err := grpc.NewClient("passthrough:///service-route-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return session.connection, nil }))
			if err != nil {
				t.Fatal(err)
			}
			name := source
			if session.allocationID == "remote-b" {
				name = destination
			}
			client.sessions[name] = &productSession{connection: connection, client: hostv1.NewAgentRuntimeServiceClient(connection)}
		case failure := <-controller.failures:
			t.Fatalf("service route controller failed: %s", failure)
		case <-ctx.Done():
			t.Fatal("service route runtime sessions did not open", ctx.Err())
		}
	}

	return client, source, destination
}

func createServiceRoute(t *testing.T, client *productClient, environment, sourceID, destinationID, name string) func() {
	return createServiceRouteAcross(t, client, environment, environment, sourceID, destinationID, name)
}

func createServiceRouteAcross(t *testing.T, client *productClient, environment, destinationEnvironment, sourceID, destinationID, name string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := client.session(environment)
	if err != nil {
		t.Fatal(err)
	}
	source, err := client.GetContainer(ctx, environment, &hostv1.GetContainerRequest{ContainerId: sourceID})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := client.GetContainer(ctx, destinationEnvironment, &hostv1.GetContainerRequest{ContainerId: destinationID})
	if err != nil {
		t.Fatal(err)
	}
	destinationSession, err := client.session(destinationEnvironment)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := destinationSession.client.CreateServiceExport(ctx, &hostv1.CreateServiceExportRequest{RequestId: environment + "-export", ContainerId: destinationID, ContainerInstanceId: destination.GetContainer().GetInstanceId(), Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP})
	if err != nil {
		t.Fatal(err)
	}
	imported, err := session.client.CreateServiceImport(ctx, &hostv1.CreateServiceImportRequest{RequestId: environment + "-import", SourceContainerId: sourceID, SourceContainerInstanceId: source.GetContainer().GetInstanceId(), DestinationDeviceId: "local", DestinationExportId: exported.GetExport().GetExportId(), VirtualServiceName: name})
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		current, err := client.session(environment)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := current.client.RevokeServiceImport(ctx, &hostv1.RevokeServiceImportRequest{RequestId: environment + "-revoke-import", ImportId: imported.GetImport().GetImportId(), ExpectedRevision: imported.GetImport().GetRevision()}); err != nil {
			t.Fatal(err)
		}
	}
}

func containerAddress(t *testing.T, client *productClient, namespaceID, containerID string) string {
	t.Helper()
	output, code := execContainer(t, client, namespaceID, containerID, []string{"sh", "-c", "ip -4 -o addr show dev eth0 | awk '{split($4,a,\"/\"); print a[1]}'"})
	address := strings.TrimSpace(string(output))
	if code != 0 || net.ParseIP(address) == nil {
		t.Fatalf("container address = %q, exit %d", output, code)
	}
	return address
}

func waitForContainerPort(t *testing.T, client *productClient, namespaceID, containerID string, port uint32) {
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

func startEchoServer(t *testing.T, client *productClient, namespaceID, containerID string, port uint32) {
	t.Helper()
	if output, code := execContainer(t, client, namespaceID, containerID, []string{"sh", "-c", fmt.Sprintf("nc -lk -p %d -e /bin/cat >/dev/null 2>&1 &", port)}); code != 0 {
		t.Fatalf("start echo server = %q, exit %d", output, code)
	}
	waitForContainerPort(t, client, namespaceID, containerID, port)
}
