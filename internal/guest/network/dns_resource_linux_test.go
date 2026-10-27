//go:build linux

package network

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/sys/unix"
)

func TestDNSResourceIsolationMeasurement(t *testing.T) {
	if os.Getenv("AGENT_VMM_DNS_RESOURCE_TEST") != "1" {
		t.Skip("requires isolated DNS load measurement")
	}
	if os.Getenv("AGENT_VMM_DNS_RESOURCE_HELPER") != "1" {
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
		fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(fd)
		request, err := unix.NewIfreq("lo")
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, request); err != nil {
			t.Fatal(err)
		}
		request.SetUint16(request.Uint16() | unix.IFF_UP)
		if err := unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, request); err != nil {
			t.Fatal(err)
		}
		address, err := unix.NewIfreq("lo:egress")
		if err != nil {
			t.Fatal(err)
		}
		if err := address.SetInet4Addr([]byte{100, 127, 255, 1}); err != nil {
			t.Fatal(err)
		}
		if err := unix.IoctlIfreq(fd, unix.SIOCSIFADDR, address); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(os.Args[0], "-test.run=^TestDNSResourceIsolationMeasurement$", "-test.v")
		command.Env = append(os.Environ(), "AGENT_VMM_DNS_RESOURCE_HELPER=1")
		output, err := command.CombinedOutput()
		t.Logf("%s", output)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("100.127.255.1"), Port: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	manager := &Manager{namespaces: map[string]Namespace{
		"load":  {ID: "load", Gateway: netip.MustParseAddr("127.64.1.1"), EgressMode: vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET},
		"other": {ID: "other", Gateway: netip.MustParseAddr("127.64.2.1"), EgressMode: vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL},
	}, dns: make(map[string]*dnsServer), logger: slog.Default().With("component", "guest-network"),
	}
	manager.publishDNSLocked()
	if err := manager.startDNSLocked("load"); err != nil {
		t.Fatal(err)
	}
	if err := manager.startDNSLocked("other"); err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	query, err := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 19}, Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName("missing.other.agent."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}).Pack()
	if err != nil {
		t.Fatal(err)
	}
	measure := func() time.Duration {
		samples := make([]time.Duration, 100)
		for index := range samples {
			started := time.Now()
			connection, err := net.DialTimeout("tcp4", "127.64.2.1:53", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			connection.SetDeadline(time.Now().Add(time.Second))
			size := make([]byte, 2)
			binary.BigEndian.PutUint16(size, uint16(len(query)))
			if _, err := connection.Write(append(size, query...)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(connection, size); err != nil {
				t.Fatal(err)
			}
			if _, err := io.CopyN(io.Discard, connection, int64(binary.BigEndian.Uint16(size))); err != nil {
				t.Fatal(err)
			}
			connection.Close()
			samples[index] = time.Since(started)
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return samples[98]
	}
	baseline := measure()
	before := runtime.NumGoroutine()
	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	connections := make([]net.Conn, 0, 1000)
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	for range 1000 {
		connection, err := net.DialTimeout("tcp4", "127.64.1.1:53", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
	}
	loadQuery, err := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 20}, Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName("example.com."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}).Pack()
	if err != nil {
		t.Fatal(err)
	}
	sender, err := net.Dial("udp4", "127.64.1.1:53")
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	for range 1000 {
		if _, err := sender.Write(loadQuery); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	loaded := measure()
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)
	rss, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(rss), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			rss = []byte(line)
			break
		}
	}
	t.Logf("slow_tcp=%d slow_udp=1000 goroutines_before=%d goroutines_after=%d heap_before=%d heap_after=%d other_dns_p99_before=%s other_dns_p99_after=%s status=%s", len(connections), before, runtime.NumGoroutine(), memoryBefore.HeapAlloc, memoryAfter.HeapAlloc, baseline, loaded, rss)
}
