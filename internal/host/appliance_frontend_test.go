//go:build darwin || linux

package host

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestApplianceFrontendProjectsExactLoopbackRoute(t *testing.T) {
	for _, maxTotal := range []uint64{64, 0} {
		t.Run(fmt.Sprint(maxTotal), func(t *testing.T) {
			support := t.TempDir()
			controller := &Controller{supportDirectory: support, applianceFrontends: make(map[string]*applianceFrontendRuntime)}
			identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-a", BootId: "boot-a",
				NamespaceId: "system.installation-a", ContainerInstanceId: "instance-a", BootNonce: make([]byte, 32)}
			frontend := store.ApplianceFrontend{Name: "mcp", Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM),
				Exposure:      int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK),
				MaxFrameBytes: 8, MaxTotalBytes: maxTotal, DeadlineSeconds: 10, IdleSeconds: 2, MaxConcurrentStreams: 1}
			if maxTotal == 0 {
				frontend.MaxFrameBytes = 64 << 10
			}
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
			if maxTotal == 0 {
				const size = (1 << 30) + (64 << 10)
				_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
				written := make(chan error, 1)
				go func() {
					buffer := make([]byte, 64<<10)
					for n := 0; n < size; n += len(buffer) {
						if _, err := connection.Write(buffer); err != nil {
							written <- err
							return
						}
					}
					written <- nil
				}()
				count, err := io.CopyN(io.Discard, connection, size)
				if err != nil || count != size {
					t.Fatalf("unlimited frontend echoed %d bytes: %v", count, err)
				}
				if err := <-written; err != nil {
					t.Fatal(err)
				}
			}
			header := <-headerSeen
			if header.GetIdentity().GetInstallationId() != identity.GetInstallationId() || header.GetIdentity().GetContainerInstanceId() != identity.GetContainerInstanceId() ||
				header.GetName() != frontend.Name || header.GetKind() != vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND ||
				header.GetMaxFrameBytes() != frontend.MaxFrameBytes || header.GetMaxTotalBytes() != frontend.MaxTotalBytes ||
				header.GetMaxConcurrentStreams() != frontend.MaxConcurrentStreams {
				t.Fatalf("header = %+v", header)
			}
			if endpoint, available := controller.applianceFrontendProjection(identity.GetInstallationId(), frontend.Name, identity.GetContainerInstanceId()); endpoint == "" || !available {
				t.Fatalf("available projection = %q, %v", endpoint, available)
			}
			if _, err := os.Stat(applianceFrontendEndpointPath(support, identity.GetInstallationId(), frontend.Name)); err != nil {
				t.Fatalf("available endpoint file missing: %v", err)
			}
		})
	}
}

func TestStoppingApplianceFrontendClosesConnectionsAndDiscovery(t *testing.T) {
	controller := &Controller{supportDirectory: t.TempDir(), applianceFrontends: make(map[string]*applianceFrontendRuntime)}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-b", ContainerInstanceId: "instance-b", BootNonce: make([]byte, 32)}
	frontend := store.ApplianceFrontend{Name: "http", Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP),
		Exposure: int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK), MaxFrameBytes: 1024,
		MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2}
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

func TestApplianceFrontendReusesStaticPort(t *testing.T) {
	for _, exposure := range []appliancev1.FrontendExposure{appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP} {
		t.Run(exposure.String(), func(t *testing.T) {
			if exposure == appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP {
				if _, err := applianceLANBindHost(); err != nil {
					t.Skip(err)
				}
			}
			controller := &Controller{supportDirectory: t.TempDir(), applianceFrontends: make(map[string]*applianceFrontendRuntime)}
			identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-port", BootId: "boot", ContainerInstanceId: "instance"}
			frontend := store.ApplianceFrontend{Name: "mcp", Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP),
				Exposure: int32(exposure), MaxFrameBytes: 1024,
				MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 1}
			first, err := controller.newApplianceFrontendRuntime(frontend, identity, nil)
			if err != nil {
				t.Fatal(err)
			}
			endpoint := first.endpoint
			first.close()
			second, err := controller.newApplianceFrontendRuntime(frontend, identity, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer second.close()
			if second.endpoint != endpoint {
				t.Fatalf("endpoint changed across runtime restart: %q -> %q", endpoint, second.endpoint)
			}
		})
	}
}

func TestApplianceFrontendRejectsInvalidProjection(t *testing.T) {
	controller := &Controller{supportDirectory: t.TempDir()}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-c"}
	base := store.ApplianceFrontend{Name: "api", MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10,
		IdleSeconds: 5, MaxConcurrentStreams: 1}
	for name, mutate := range map[string]func(*store.ApplianceFrontend){
		"frame":       func(value *store.ApplianceFrontend) { value.MaxFrameBytes = (1 << 20) + 1 },
		"total":       func(value *store.ApplianceFrontend) { value.MaxTotalBytes = 1 },
		"deadline":    func(value *store.ApplianceFrontend) { value.DeadlineSeconds = 24*60*60 + 1 },
		"idle":        func(value *store.ApplianceFrontend) { value.IdleSeconds = 301 },
		"concurrency": func(value *store.ApplianceFrontend) { value.MaxConcurrentStreams = 17 },
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

func TestHostEndpointGrantCannotReachFrontendListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	endpoint := netip.MustParseAddrPort(listener.Addr().String())
	controller := &Controller{applianceFrontends: map[string]*applianceFrontendRuntime{"frontend": {listener: listener}}}
	controller.hostFrontends = newHostLoopbackFrontendManager(controller)
	if !controller.hostTCPInfrastructure(netip.AddrPortFrom(netip.MustParseAddr("192.168.1.10"), endpoint.Port())) {
		t.Fatal("wildcard frontend did not block LAN Host endpoint")
	}
	if controller.hostTCPInfrastructure(netip.AddrPortFrom(netip.MustParseAddr("192.168.1.10"), endpoint.Port()+1)) {
		t.Fatal("frontend blocked unrelated TCP port")
	}
	delete(controller.applianceFrontends, "frontend")
	controller.hostFrontends.leases["lease"] = &hostLoopbackFrontendLease{listener: listener}
	if !controller.hostTCPInfrastructure(netip.AddrPortFrom(netip.MustParseAddr("192.168.1.10"), endpoint.Port())) {
		t.Fatal("dynamic frontend bypassed infrastructure exclusion")
	}
	delete(controller.hostFrontends.leases, "lease")
	controller.serviceListeners = map[string]*serviceListenerRuntime{"service": {listener: listener}}
	if !controller.hostTCPInfrastructure(netip.AddrPortFrom(netip.MustParseAddr("192.168.1.10"), endpoint.Port())) {
		t.Fatal("personal service listener bypassed infrastructure exclusion")
	}

}

func TestApplianceFrontendServiceSharesBoundsAndCancellation(t *testing.T) {
	for _, stopKind := range []string{"route", "installation"} {
		t.Run(stopKind, func(t *testing.T) {
			controller := &Controller{supportDirectory: t.TempDir(), applianceFrontends: make(map[string]*applianceFrontendRuntime)}
			identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "callback", BootId: "boot", ContainerInstanceId: "instance"}
			frontend := store.ApplianceFrontend{Name: "webhook", Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP), Exposure: int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK), MaxFrameBytes: 8, MaxTotalBytes: 64, DeadlineSeconds: 10, IdleSeconds: 2, MaxConcurrentStreams: 1}
			opened := make(chan *vmmdv1.SystemStreamHeader, 1)
			runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(ctx context.Context, header *vmmdv1.SystemStreamHeader) (net.Conn, error) {
				host, guest := net.Pipe()
				opened <- header
				go func() {
					defer guest.Close()
					stop := context.AfterFunc(ctx, func() { guest.Close() })
					defer stop()
					_, _ = io.Copy(guest, guest)
				}()
				return host, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.close()
			controller.applianceFrontends[frontendRuntimeKey("callback", "webhook")] = runtime
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			connection, err := runtime.dialService(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if _, err := connection.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			reply := make([]byte, 4)
			if _, err := io.ReadFull(connection, reply); err != nil || string(reply) != "ping" {
				t.Fatalf("reply=%q err=%v", reply, err)
			}
			header := <-opened
			if header.GetMaxTotalBytes() != 64 || header.GetKind() != vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND || header.GetIdentity().GetContainerInstanceId() != "instance" {
				t.Fatalf("header=%v", header)
			}
			if second, err := runtime.dialService(ctx); err == nil {
				second.Close()
				t.Fatal("callback bypassed frontend concurrency")
			}
			if stopKind == "route" {
				cancel()
			} else {
				controller.stopApplianceFrontends("callback")
			}
			_ = connection.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := connection.Read(reply); err == nil {
				t.Fatal("cancelled callback retained connection")
			}
		})
	}
}

func TestApplianceFrontendRemainsAvailableAcrossRepeatedConnections(t *testing.T) {
	controller := &Controller{supportDirectory: t.TempDir()}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation", ContainerInstanceId: "instance"}
	frontend := store.ApplianceFrontend{Name: "management", Transport: 2, Exposure: 1, MaxFrameBytes: 1024, DeadlineSeconds: 10, IdleSeconds: 2, MaxConcurrentStreams: 1}
	runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(context.Context, *vmmdv1.SystemStreamHeader) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() { defer remote.Close(); _, _ = io.Copy(remote, remote) }()
		return local, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	for index := range 1100 {
		connection, err := runtime.dialService(context.Background())
		if err != nil {
			t.Fatalf("connection %d: %v", index, err)
		}
		connection.SetDeadline(time.Now().Add(time.Second))
		if _, err := connection.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(connection, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		connection.Close()
		deadline := time.Now().Add(time.Second)
		for {
			runtime.mu.Lock()
			active := len(runtime.connections)
			runtime.mu.Unlock()
			if active == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("released frontend stayed active")
			}
			time.Sleep(time.Microsecond)
		}
	}
	if _, err := os.Stat(runtime.endpointFile); err != nil {
		t.Fatal(err)
	}
}
