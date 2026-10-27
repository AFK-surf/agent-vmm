//go:build linux

package network

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/sys/unix"
)

func TestKernelDNSInputUsesIngressBridge(t *testing.T) {
	if os.Getenv("AGENT_VMM_NFTABLES_TEST") != "1" {
		t.Skip("requires an isolated kernel network namespace")
	}
	switch os.Getenv("AGENT_VMM_DNS_INPUT_HELPER") {
	case "client":
		var start [1]byte
		if _, err := io.ReadFull(os.Stdin, start[:]); err != nil {
			t.Fatal(err)
		}
		kernelDNSCommand(t, "link", "set", "lo", "up")
		kernelDNSCommand(t, "addr", "add", "10.96.0.2/24", "dev", "dns-peer")
		kernelDNSCommand(t, "link", "set", "dns-peer", "up")
		kernelDNSCommand(t, "route", "add", "default", "via", "10.96.0.1")
		query, err := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 1}, Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName("example.com."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}).Pack()
		if err != nil {
			t.Fatal(err)
		}
		for _, transport := range []string{"udp4", "tcp4"} {
			for _, target := range []struct {
				address string
				allow   bool
			}{{"10.96.0.1:53", true}, {"10.97.0.1:53", false}, {"10.96.0.1:1053", false}} {
				connection, err := net.DialTimeout(transport, target.address, 300*time.Millisecond)
				if err == nil && transport == "tcp4" && !target.allow {
					connection.Close()
					t.Fatalf("unauthorized TCP INPUT connected: %s", target.address)
				}
				if err == nil {
					connection.SetDeadline(time.Now().Add(300 * time.Millisecond))
					if transport == "tcp4" {
						size := make([]byte, 2)
						binary.BigEndian.PutUint16(size, uint16(len(query)))
						_, err = connection.Write(append(size, query...))
						if err == nil {
							_, err = io.ReadFull(connection, size)
						}
					} else {
						_, err = connection.Write(query)
						if err == nil {
							_, err = connection.Read(make([]byte, 512))
						}
					}
					connection.Close()
				}
				if target.allow && err != nil {
					t.Fatalf("own bridge DNS %s: %v", transport, err)
				}
				if !target.allow {
					timeout, ok := err.(net.Error)
					if !ok || !timeout.Timeout() {
						t.Fatalf("unauthorized INPUT %s %s: %v", transport, target.address, err)
					}
				}
			}
		}
		return
	case "root":
		kernelDNSCommand(t, "link", "set", "lo", "up")
		for index, bridge := range []string{"avm-dns-a", "avm-dns-b"} {
			kernelDNSCommand(t, "link", "add", bridge, "type", "bridge")
			kernelDNSCommand(t, "addr", "add", fmt.Sprintf("10.%d.0.1/24", 96+index), "dev", bridge)
			kernelDNSCommand(t, "link", "set", bridge, "up")
		}
		kernelDNSCommand(t, "link", "add", "dns-root", "type", "veth", "peer", "name", "dns-peer")
		kernelDNSCommand(t, "link", "set", "dns-root", "master", "avm-dns-a")
		kernelDNSCommand(t, "link", "set", "dns-root", "up")
		manager := &Manager{namespaces: map[string]Namespace{
			"source": {ID: "source", Bridge: "avm-dns-a", Gateway: netip.MustParseAddr("10.96.0.1"), EgressMode: vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL},
			"other":  {ID: "other", Bridge: "avm-dns-b", Gateway: netip.MustParseAddr("10.97.0.1"), EgressMode: vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET},
		}, dns: make(map[string]*dnsServer), logger: slog.Default().With("component", "guest-network"),
		}
		defer manager.Close()
		manager.publishDNSLocked()
		for id := range manager.namespaces {
			if err := manager.startDNSLocked(id); err != nil {
				t.Fatal(err)
			}
		}
		if err := manager.reconcileLocked(); err != nil {
			t.Fatal(err)
		}
		udp, err := net.ListenPacket("udp4", "10.96.0.1:1053")
		if err != nil {
			t.Fatal(err)
		}
		defer udp.Close()
		go func() {
			buffer := make([]byte, 512)
			for {
				count, peer, err := udp.ReadFrom(buffer)
				if err != nil {
					return
				}
				udp.WriteTo(buffer[:count], peer)
			}
		}()
		tcp, err := net.Listen("tcp4", "10.96.0.1:1053")
		if err != nil {
			t.Fatal(err)
		}
		defer tcp.Close()
		client := exec.Command(os.Args[0], "-test.run=^TestKernelDNSInputUsesIngressBridge$", "-test.v")
		client.Env = append(os.Environ(), "AGENT_VMM_DNS_INPUT_HELPER=client")
		client.SysProcAttr = &unix.SysProcAttr{Cloneflags: unix.CLONE_NEWNET}
		start, err := client.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		client.Stdout, client.Stderr = &output, &output
		if err := client.Start(); err != nil {
			t.Fatal(err)
		}
		defer client.Process.Kill()
		kernelDNSCommand(t, "link", "set", "dns-peer", "netns", fmt.Sprint(client.Process.Pid))
		if _, err := start.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		start.Close()
		if err := client.Wait(); err != nil {
			t.Fatalf("client: %v\n%s", err, &output)
		}
		t.Logf("%s", &output)
		return
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := os.Open("/proc/thread-self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
		t.Fatal(err)
	}
	defer unix.Setns(int(original.Fd()), unix.CLONE_NEWNET)
	command := exec.Command(os.Args[0], "-test.run=^TestKernelDNSInputUsesIngressBridge$", "-test.v")
	command.Env = append(os.Environ(), "AGENT_VMM_DNS_INPUT_HELPER=root")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("private network helper: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}

func kernelDNSCommand(t *testing.T, arguments ...string) {
	t.Helper()
	if output, err := exec.Command("ip", arguments...).CombinedOutput(); err != nil {
		t.Fatalf("ip %v: %v: %s", arguments, err, output)
	}
}
