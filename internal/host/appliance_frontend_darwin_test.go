//go:build darwin

package host

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestApplianceFrontendProjectsExactBoundedLoopbackRoute(t *testing.T) {
	support := t.TempDir()
	controller := &Controller{supportDirectory: support, applianceFrontends: make(map[string]*applianceFrontendRuntime)}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-a", BootId: "boot-a",
		NamespaceId: "system.installation-a", ContainerInstanceId: "instance-a", BootNonce: make([]byte, 32)}
	frontend := store.ApplianceFrontend{Name: "mcp", Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM),
		Exposure:      int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK),
		MaxFrameBytes: 8, MaxTotalBytes: 64, DeadlineSeconds: 10, IdleSeconds: 2, MaxConcurrentStreams: 1, MaxUseCount: 1}
	headerSeen := make(chan *vmmdv1.SystemStreamHeader, 1)
	runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(_ context.Context, header *vmmdv1.SystemStreamHeader) (net.Conn, error) {
		host, appliance := net.Pipe()
		headerSeen <- header
		go func() {
			defer appliance.Close()
			_, _ = io.Copy(appliance, appliance)
		}()
		return host, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	controller.applianceFrontends[frontendRuntimeKey(identity.GetInstallationId(), frontend.Name)] = runtime
	go runtime.serve()
	t.Cleanup(controller.stopAllApplianceFrontends)

	endpointBytes, err := os.ReadFile(applianceFrontendEndpointPath(support, identity.GetInstallationId(), frontend.Name))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := string(endpointBytes[:len(endpointBytes)-1])
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host != "127.0.0.1" || port == "" {
		t.Fatalf("endpoint = %q, %v", endpoint, err)
	}
	info, err := os.Stat(applianceFrontendEndpointPath(support, identity.GetInstallationId(), frontend.Name))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("endpoint mode = %v, %v", info.Mode().Perm(), err)
	}

	connection, err := net.DialTimeout("tcp4", endpoint, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(connection, response); err != nil || string(response) != "ping" {
		t.Fatalf("response = %q, %v", response, err)
	}
	header := <-headerSeen
	if header.GetIdentity().GetInstallationId() != identity.GetInstallationId() || header.GetIdentity().GetContainerInstanceId() != identity.GetContainerInstanceId() ||
		header.GetName() != frontend.Name || header.GetKind() != vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND ||
		header.GetMaxFrameBytes() != frontend.MaxFrameBytes || header.GetMaxTotalBytes() != frontend.MaxTotalBytes ||
		header.GetMaxConcurrentStreams() != frontend.MaxConcurrentStreams || header.GetMaxUseCount() != frontend.MaxUseCount {
		t.Fatalf("header = %+v", header)
	}
	if endpoint, available := controller.applianceFrontendProjection(identity.GetInstallationId(), frontend.Name, identity.GetContainerInstanceId()); endpoint == "" || available {
		t.Fatalf("exhausted projection = %q, %v", endpoint, available)
	}
	if _, err := os.Stat(applianceFrontendEndpointPath(support, identity.GetInstallationId(), frontend.Name)); !os.IsNotExist(err) {
		t.Fatalf("exhausted endpoint file remains: %v", err)
	}
}

func TestStoppingApplianceFrontendClosesConnectionsAndDiscovery(t *testing.T) {
	controller := &Controller{supportDirectory: t.TempDir(), applianceFrontends: make(map[string]*applianceFrontendRuntime)}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-b", ContainerInstanceId: "instance-b", BootNonce: make([]byte, 32)}
	frontend := store.ApplianceFrontend{Name: "http", Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP),
		Exposure: int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK), MaxFrameBytes: 1024,
		MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2, MaxUseCount: 4}
	opened := make(chan struct{})
	runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(ctx context.Context, _ *vmmdv1.SystemStreamHeader) (net.Conn, error) {
		host, appliance := net.Pipe()
		close(opened)
		go func() {
			<-ctx.Done()
			appliance.Close()
		}()
		return host, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	controller.applianceFrontends[frontendRuntimeKey(identity.GetInstallationId(), frontend.Name)] = runtime
	go runtime.serve()
	connection, err := net.Dial("tcp4", runtime.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	<-opened
	controller.stopApplianceFrontends(identity.GetInstallationId())
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bufio.NewReader(connection).ReadByte(); err == nil {
		t.Fatal("connection remained open after frontend stop")
	}
	connection.Close()
	if _, err := os.Stat(runtime.endpointFile); !os.IsNotExist(err) {
		t.Fatalf("endpoint file remains after stop: %v", err)
	}
	if _, err := net.DialTimeout("tcp4", runtime.endpoint, 100*time.Millisecond); err == nil {
		t.Fatal("listener remained open after frontend stop")
	}
	if got := filepath.Dir(runtime.endpointFile); got != filepath.Join(controller.supportDirectory, "appliance-frontends", identity.GetInstallationId()) {
		t.Fatalf("endpoint directory = %q", got)
	}
}

func TestApplianceFrontendRejectsUnboundedProjection(t *testing.T) {
	controller := &Controller{supportDirectory: t.TempDir()}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-c"}
	base := store.ApplianceFrontend{Name: "api", MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10,
		IdleSeconds: 5, MaxConcurrentStreams: 1, MaxUseCount: 1}
	for name, mutate := range map[string]func(*store.ApplianceFrontend){
		"frame":       func(value *store.ApplianceFrontend) { value.MaxFrameBytes = (1 << 20) + 1 },
		"total":       func(value *store.ApplianceFrontend) { value.MaxTotalBytes = (1 << 30) + 1 },
		"deadline":    func(value *store.ApplianceFrontend) { value.DeadlineSeconds = 24*60*60 + 1 },
		"idle":        func(value *store.ApplianceFrontend) { value.IdleSeconds = 301 },
		"concurrency": func(value *store.ApplianceFrontend) { value.MaxConcurrentStreams = 17 },
		"uses":        func(value *store.ApplianceFrontend) { value.MaxUseCount = 1025 },
	} {
		t.Run(name, func(t *testing.T) {
			frontend := base
			mutate(&frontend)
			if runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, nil); err == nil {
				runtime.close()
				t.Fatal("unbounded frontend was accepted")
			}
		})
	}
}
