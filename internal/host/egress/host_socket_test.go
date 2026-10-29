package egress

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"gvisor.dev/gvisor/pkg/tcpip"
)

func TestHostSocketsAdmitOnlyTheirPortsOfTheHostVirtualAddress(t *testing.T) {
	subsystem := New()
	source := Source{EnvironmentID: "env", NamespaceID: "ns", Subnet: netip.MustParsePrefix("10.64.1.0/24"), Mode: DenyAll}
	sourceAddress := tcpip.AddrFromSlice(source.Subnet.Addr().Next().AsSlice())
	host := tcpip.AddrFromSlice(netip.MustParseAddr(egresslink.HostAddress).AsSlice())
	guest := tcpip.AddrFromSlice(netip.MustParseAddr(egresslink.GuestAddress).AsSlice())
	publish := func(revision uint64) {
		t.Helper()
		if err := subsystem.Policies.Publish(Snapshot{BootID: "boot", Revision: revision, Sources: []Source{source}}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(from tcpip.Address, port uint16, tcp, dnsAnswer, want bool) {
		t.Helper()
		if _, _, got, _ := subsystem.authorizeEndpoint("boot", from, host, false, port, tcp, dnsAnswer); got != want {
			t.Fatalf("from=%v port=%v TCP=%v DNS answer=%v got=%v want=%v", from, port, tcp, dnsAnswer, got, want)
		}
	}
	publish(1)
	if _, allowed := subsystem.Policies.Resolve("boot", source.Subnet.Addr().Next()); allowed {
		t.Fatal("a deny-all source without Host sockets reached the egress")
	}
	check(sourceAddress, 7480, true, false, false)
	source.HostSockets = []HostSocket{{Port: 7480, Path: "/run/outflow.sock"}}
	publish(2)
	if _, allowed := subsystem.Policies.Resolve("boot", source.Subnet.Addr().Next()); !allowed {
		t.Fatal("a source with a Host socket did not reach the egress")
	}
	check(sourceAddress, 7480, true, false, true)
	check(sourceAddress, 7481, true, false, false)
	check(sourceAddress, 7480, false, false, false)
	check(sourceAddress, 7480, true, true, false)
	check(guest, 7480, true, false, false)

	// A flow admitted before the socket was removed ends with it.
	_, _, _, authorized := subsystem.authorizeEndpoint("boot", sourceAddress, host, false, 7480, true, false)
	flow, cancel := subsystem.Policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), authorized)
	defer cancel()
	source.HostSockets = nil
	publish(3)
	<-flow.Done()
	check(sourceAddress, 7480, true, false, false)
}

func TestAuthorizedHostSocketFlowConnectsToTheSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "hs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "o.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan string, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			accepted <- err.Error()
			return
		}
		defer connection.Close()
		buffer := make([]byte, 5)
		_, err = io.ReadFull(connection, buffer)
		if err != nil {
			accepted <- err.Error()
			return
		}
		accepted <- string(buffer)
	}()
	subsystem := New()
	source := Source{EnvironmentID: "env", HostSockets: []HostSocket{{Port: 7480, Path: path}}}
	connection, err := subsystem.dialAuthorized(context.Background(), source, netip.MustParseAddr(egresslink.HostAddress), 7480)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := <-accepted; got != "hello" {
		t.Fatalf("the socket read %q", got)
	}
	_ = connection.Close()
	if _, err := subsystem.dialAuthorized(context.Background(), source, netip.MustParseAddr(egresslink.HostAddress), 7481); err == nil {
		t.Fatal("a port without a Host socket was connected")
	}
}
