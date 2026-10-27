//go:build darwin || linux

package servicenet

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestListenServiceAddressEnforcesExposureBoundary(t *testing.T) {
	ctx := context.Background()
	loopback, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer loopback.Close()
	loopbackAddress := loopback.Addr().(*net.TCPAddr)
	if !loopbackAddress.AddrPort().Addr().IsLoopback() {
		t.Fatalf("device listener bound %s", loopbackAddress)
	}
	if _, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, "0.0.0.0", 0); err == nil {
		t.Fatal("device listener accepted wildcard exposure")
	}
	if _, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, "127.0.0.1", 0); err == nil {
		t.Fatal("LAN listener accepted loopback exposure")
	}
	if _, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, "0.0.0.0", 0); err == nil {
		t.Fatal("LAN listener accepted wildcard exposure")
	}
}

func TestListenServiceAddressAcceptsExplicitLocalPrivateInterface(t *testing.T) {
	address, ok := firstLocalPrivateAddress()
	if !ok {
		t.Skip("no local private interface is available")
	}
	listener, err := listenServiceAddress(context.Background(), hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, address.String(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if actual := listener.Addr().(*net.TCPAddr).AddrPort().Addr().Unmap(); actual != address.Unmap() {
		t.Fatalf("listener bound %s, want %s", actual, address)
	}
}

func firstLocalPrivateAddress() (netip.Addr, bool) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return netip.Addr{}, false
	}
	for _, current := range interfaces {
		addresses, _ := current.Addrs()
		for _, address := range addresses {
			prefix, err := netip.ParsePrefix(address.String())
			if err == nil && prefix.Addr().IsPrivate() && !prefix.Addr().IsLoopback() {
				return prefix.Addr().Unmap(), true
			}
		}
	}
	return netip.Addr{}, false
}

func TestLocalServiceListenerSurvivesCallerAndRestoresExactAddress(t *testing.T) {
	t.Run("default user socket", func(t *testing.T) {
		testLocalServiceListener(t, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_UNSPECIFIED, "")
	})
	t.Run("loopback", func(t *testing.T) {
		testLocalServiceListener(t, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, "127.0.0.1")
	})
	t.Run("LAN", func(t *testing.T) {
		address, ok := firstLocalPrivateAddress()
		if !ok {
			t.Skip("no private local interface")
		}
		testLocalServiceListener(t, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, address.String())
	})
}

func testLocalServiceListener(t *testing.T, exposure hostv1.ServiceListenerExposure, bindAddress string) {
	databasePath := filepath.Join(t.TempDir(), "host.db")
	database, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	controller := newTestServiceOwner(t, &Owner{store: database, serviceListeners: make(map[string]*serviceListenerRuntime)}, listenerWorkloadState{workloadRuns: map[string]*listenerWorkloadRun{workloadRunKey("service", "gitea"): {environmentID: "service", namespaceID: "service", containerID: "gitea", instanceID: "instance"}}})
	directory, err := os.MkdirTemp("", "vmm-svc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	controller.serviceDirectory = directory
	network := "tcp"
	if exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_UNSPECIFIED {
		network = "unix"
	}
	defer controller.CloseAllServiceListeners()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", DisplayName: "service", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	_, err = database.CreateServiceExport(ctx, "export", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "service", ContainerId: "gitea", ContainerGenerationId: "generation", ContainerInstanceId: "instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	call := func(ctx context.Context, request *hostv1.CreateLocalServiceListenerRequest) (*hostv1.CreateLocalServiceListenerResponse, error) {
		value, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return controller.CreateLocalServiceListener(ctx, request)
		})
		if err != nil {
			return nil, err
		}
		return value.(*hostv1.CreateLocalServiceListenerResponse), nil
	}
	caller, cancel := context.WithCancel(ctx)
	request := &hostv1.CreateLocalServiceListenerRequest{RequestId: "open", EnvironmentId: "service", ContainerId: "gitea", TargetPort: 3000, Exposure: exposure, BindAddress: bindAddress}
	created, err := call(caller, request)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	value := created.Listener
	if !value.Local || value.ExpiresAt != nil || value.Available == nil || !value.GetAvailable() || value.GetUnavailableReason() != "" {
		t.Fatalf("listener=%v", value)
	}
	replay, err := call(ctx, request)
	if err != nil || replay.Listener.ListenAddress != value.ListenAddress {
		t.Fatalf("replay=%v %v", replay, err)
	}
	record, err := database.GetServiceListener(ctx, value.Id)
	if err != nil || record.Listener.ExpiresAt != nil || record.Listener.Available != nil || record.Listener.GetUnavailableReason() != "" {
		t.Fatalf("record=%v %v", record, err)
	}
	conflicting := proto.Clone(request).(*hostv1.CreateLocalServiceListenerRequest)
	conflicting.ContainerId = "other"
	if _, err := call(ctx, conflicting); status.Code(err) != codes.Aborted {
		t.Fatalf("conflict=%v", err)
	}
	old := controller.serviceListeners[value.Id]
	if _, err := net.Listen(network, value.ListenAddress); err == nil {
		t.Fatal("caller cancellation released address")
	}
	controller.CloseAllServiceListeners()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	controller.store = database
	if network == "unix" {
		if value.GetExposure() != hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET || filepath.Dir(value.GetListenAddress()) != directory {
			t.Fatalf("default endpoint = %v", value)
		}
		if err := controller.RestoreLocalServiceListeners(ctx); err != nil {
			t.Fatal(err)
		}
		restored := controller.serviceListeners[value.Id]
		if restored == nil || restored.listener.Addr().Network() != "unix" || restored.listener.Addr().String() != value.ListenAddress {
			t.Fatal("socket address changed after restore")
		}
		invalid := proto.Clone(request).(*hostv1.CreateLocalServiceListenerRequest)
		invalid.RequestId = "invalid"
		invalid.BindPort = 8080
		if _, err := call(ctx, invalid); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("socket bind port = %v", err)
		}
		connection, err := net.Dial("unix", value.ListenAddress)
		if err != nil {
			t.Fatal(err)
		}
		connection.Close()
		return
	}
	occupied, err := net.Listen(network, value.ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreLocalServiceListeners(ctx); err != nil {
		t.Fatal("occupied listener failed unrelated reconciliation", err)
	}
	if len(controller.serviceListeners) != 0 {
		t.Fatal("bound a replacement address")
	}
	replay, err = call(ctx, request)
	if err != nil || replay.GetListener().GetAvailable() || replay.GetListener().GetUnavailableReason() == "" || replay.GetListener().GetId() != value.GetId() || replay.GetListener().GetListenAddress() != value.GetListenAddress() {
		t.Fatalf("occupied replay reported live listener or changed identity: %v %v", replay, err)
	}
	healthy, err := call(ctx, &hostv1.CreateLocalServiceListenerRequest{RequestId: "healthy", EnvironmentId: "service", ContainerId: "gitea", TargetPort: 3001, Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, BindAddress: "127.0.0.1"})
	if err != nil || !healthy.GetListener().GetAvailable() {
		t.Fatal(healthy, err)
	}
	listed, err := controller.ListServiceListeners(ctx, &hostv1.ListServiceListenersRequest{})
	if err != nil || len(listed.GetListeners()) != 2 {
		t.Fatal(listed, err)
	}
	for _, observation := range listed.GetListeners() {
		if observation.GetId() == value.GetId() && (observation.GetAvailable() || observation.GetUnavailableReason() == "") {
			t.Fatal("list hid unavailable socket", observation)
		}
		if observation.GetId() == healthy.GetListener().GetId() && (!observation.GetAvailable() || observation.GetUnavailableReason() != "") {
			t.Fatal("one failed listener hid healthy socket", observation)
		}
	}
	occupied.Close()
	listed, err = controller.ListServiceListeners(ctx, &hostv1.ListServiceListenersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range listed.GetListeners() {
		if observation.GetId() == value.GetId() && (!observation.GetAvailable() || observation.GetListenAddress() != value.GetListenAddress() || observation.GetUnavailableReason() != "") {
			t.Fatal("released port did not restore original publication", observation)
		}
	}
	if err := database.RevokeServiceListener(ctx, healthy.GetListener().GetId(), healthy.GetListener().GetRevision()); err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreLocalServiceListeners(ctx); err != nil {
		t.Fatal(err)
	}
	restored := controller.serviceListeners[value.Id]
	if restored == old || restored.listener.Addr().String() != value.ListenAddress {
		t.Fatal("listener not restored at same address")
	}
	local, peer := net.Pipe()
	defer peer.Close()
	if !restored.track(local) {
		t.Fatal("live runtime rejected stream")
	}
	drained := make(chan struct{})
	go func() { defer close(drained); defer restored.release(local); var b [1]byte; _, _ = local.Read(b[:]) }()
	setServiceTestInstance(controller, "service", "gitea", "replacement")
	if _, err := database.ProjectServiceContainer(ctx, "service", "gitea", "replacement-generation", "replacement"); err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreLocalServiceListeners(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("old instance stream remained open")
	}
	if restored.track(local) {
		t.Fatal("closed runtime accepted stream")
	}
	if controller.serviceListeners[value.Id].instance != "replacement" {
		t.Fatal("listener retained old instance")
	}
	if err := database.RevokeServiceListener(ctx, value.Id, 1); err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreLocalServiceListeners(ctx); err != nil {
		t.Fatal(err)
	}
	if len(controller.serviceListeners) != 0 {
		t.Fatal("revoked service still listening")
	}
	replay, err = call(ctx, request)
	if err != nil || !replay.Listener.Revoked || replay.Listener.GetAvailable() || replay.Listener.GetUnavailableReason() == "" {
		t.Fatalf("revoked replay=%v %v", replay, err)
	}
	setServiceTestInstance(controller, "service", "gitea", "")
	stoppedRequest := proto.Clone(request).(*hostv1.CreateLocalServiceListenerRequest)
	stoppedRequest.RequestId = "open-stopped"
	stopped, err := call(ctx, stoppedRequest)
	if err != nil || stopped.GetListener().GetAvailable() || stopped.GetListener().GetUnavailableReason() == "" || len(controller.serviceListeners) != 0 {
		t.Fatalf("stopped service returned an active publication: %v %v", stopped, err)
	}
	setServiceTestInstance(controller, "service", "gitea", "replacement")
	replay, err = call(ctx, stoppedRequest)
	if err != nil || !replay.GetListener().GetAvailable() || replay.GetListener().GetId() != stopped.GetListener().GetId() || replay.GetListener().GetListenAddress() != stopped.GetListener().GetListenAddress() {
		t.Fatalf("resumed service did not restore original publication: %v %v", replay, err)
	}
}

func TestPersistedLANListenerReportsMissingLocalAddress(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", DisplayName: "service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	address := netip.MustParseAddr("10.255.255.254")
	for localInterfaceContains(address) {
		address = address.Prev()
	}
	value := &hostv1.ServiceListener{Id: "missing-interface", Local: true, EnvironmentId: "service", ContainerId: "gitea", TargetPort: 3000, Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, ListenAddress: net.JoinHostPort(address.String(), "32019")}
	if _, err := database.CreateServiceListener(ctx, "open", value, &trustv1.RouteCapability{}, &trustv1.EndpointObservation{}); err != nil {
		t.Fatal(err)
	}
	controller := newTestServiceOwner(t, &Owner{store: database, serviceListeners: make(map[string]*serviceListenerRuntime)}, listenerWorkloadState{workloadRuns: map[string]*listenerWorkloadRun{workloadRunKey("service", "gitea"): {environmentID: "service", containerID: "gitea", instanceID: "running"}}})
	server := controller
	defer controller.CloseAllServiceListeners()
	if err := controller.RestoreLocalServiceListeners(ctx); err != nil {
		t.Fatal("missing interface failed unrelated reconciliation", err)
	}
	listed, err := server.ListServiceListeners(ctx, &hostv1.ListServiceListenersRequest{})
	if err != nil || len(listed.GetListeners()) != 1 || listed.GetListeners()[0].GetAvailable() || listed.GetListeners()[0].GetUnavailableReason() == "" {
		t.Fatal(listed, err)
	}
	replay, err := callAsOperator(t, func(ctx context.Context) (*hostv1.CreateLocalServiceListenerResponse, error) {
		return server.CreateLocalServiceListener(ctx, &hostv1.CreateLocalServiceListenerRequest{RequestId: "open", EnvironmentId: "service", ContainerId: "gitea", TargetPort: 3000, Exposure: value.GetExposure(), BindAddress: address.String(), BindPort: 32019})
	})
	if err != nil || replay.GetListener().GetAvailable() || replay.GetListener().GetListenAddress() != value.GetListenAddress() || replay.GetListener().GetUnavailableReason() == "" || len(controller.serviceListeners) != 0 {
		t.Fatalf("missing interface published fallback: %v %v", replay, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := controller.RestoreLocalServiceListeners(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("context error was hidden: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := controller.RestoreLocalServiceListeners(ctx); err == nil {
		t.Fatal("Store failure was hidden by listener unavailability")
	}
}

func TestHostEndpointGrantCannotReachServiceListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	endpoint := netip.MustParseAddrPort(listener.Addr().String())
	owner := &Owner{sources: emptyServiceSources{}, serviceListeners: map[string]*serviceListenerRuntime{"service": {listener: listener}}}
	if !owner.HostTCPInfrastructure(netip.AddrPortFrom(netip.MustParseAddr("192.168.1.10"), endpoint.Port())) {
		t.Fatal("service listener bypassed infrastructure exclusion")
	}
}
