//go:build linux

package vmmprocess

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
)

type childHarness struct {
	child     *child
	path      string
	cancelled atomic.Int32
}

func serveChild(t *testing.T) *childHarness {
	t.Helper()
	listener := listenTestUnix(t)
	h := &childHarness{path: listener.Addr().String()}
	h.child = &child{
		config: Config{KernelPath: "kernel", InitramfsPath: "initrd", RootFSPath: "root", StatePath: "state", InstanceDirectory: filepath.Dir(h.path), SerialLog: "serial", CommandLine: "console=ttyS0 boot_nonce=first", GuestManifestDigest: "sha256:bundle", CPUCount: 1, MemoryBytes: 64 << 20},
		state:  hostv1.VMState_VM_STATE_RUNNING, streams: make(map[net.Conn]bool), cancel: func() { h.cancelled.Add(1) }, done: make(chan struct{}), observed: make(chan struct{}), incoming: make(chan net.Conn),
	}
	var handlers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() { defer close(acceptDone); _ = h.child.accept(listener, &handlers) }()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		h.child.mu.Lock()
		h.child.state = hostv1.VMState_VM_STATE_STOPPED
		select {
		case <-h.child.done:
		default:
			close(h.child.done)
		}
		for conn := range h.child.streams {
			_ = conn.Close()
		}
		h.child.mu.Unlock()
		handlers.Wait()
	})
	return h
}
func (h *childHarness) exchange(t *testing.T, req request) (reply, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, r, err := exchange(ctx, h.path, req)
	if conn != nil {
		_ = conn.Close()
	}
	return r, err
}
func (h *childHarness) claim(t *testing.T, session string) {
	t.Helper()
	if _, err := h.exchange(t, request{Operation: "claim", Session: session, Config: &h.child.config}); err != nil {
		t.Fatal(err)
	}
}
func (h *childHarness) assertVMRunning(t *testing.T) {
	t.Helper()
	h.child.mu.Lock()
	state := h.child.state
	h.child.mu.Unlock()
	if h.cancelled.Load() != 0 || state != hostv1.VMState_VM_STATE_RUNNING {
		t.Fatal("controller fencing terminated VM")
	}
	select {
	case <-h.child.done:
		t.Fatal("VM completed on controller fencing")
	default:
	}
}
func waitRegisteredStreams(t *testing.T, s *child, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		n := len(s.streams)
		s.mu.Unlock()
		if n >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d streams registered, want %d", n, count)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClaimFencesPendingRequestsWithoutStoppingVM(t *testing.T) {
	h := serveChild(t)
	h.claim(t, "first")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 2)
	for _, op := range []string{"wait", "accept"} {
		go func() {
			conn, _, err := exchange(ctx, h.path, request{Operation: op, Session: "first", Port: netprotocol.EgressPort})
			if conn != nil {
				_ = conn.Close()
			}
			results <- err
		}()
	}
	waitRegisteredStreams(t, h.child, 2)
	h.claim(t, "replacement")
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("replaced controller retained pending request")
			}
		case <-time.After(time.Second):
			t.Fatal("claim did not wake old controller")
		}
	}
	h.assertVMRunning(t)
	for _, op := range []string{"status", "stop"} {
		if _, err := h.exchange(t, request{Operation: op, Session: "first"}); err == nil {
			t.Fatalf("stale %s accepted", op)
		}
	}
	if _, err := h.exchange(t, request{Operation: "status", Session: "replacement"}); err != nil {
		t.Fatal(err)
	}
	h.assertVMRunning(t)
}

func TestAdoptionRejectsDifferentIdentityAndPreservesOwner(t *testing.T) {
	h := serveChild(t)
	h.claim(t, "owner")
	for _, mutation := range []struct {
		name   string
		change func(*Config)
	}{
		{"state-disk", func(c *Config) { c.StatePath = "other-state" }},
		{"guest-content", func(c *Config) { c.GuestManifestDigest = "sha256:different" }},
		{"memory", func(c *Config) { c.MemoryBytes *= 2 }},
		{"cpu", func(c *Config) { c.CPUCount = 2 }},
		{"root-disk", func(c *Config) { c.RootFSPath = "other-root" }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			changed := h.child.config
			mutation.change(&changed)
			if _, err := h.exchange(t, request{Operation: "claim", Session: "intruder", Config: &changed}); err == nil {
				t.Fatal("different configuration adopted")
			}
			if _, err := h.exchange(t, request{Operation: "status", Session: "owner"}); err != nil {
				t.Fatalf("rejected claim revoked owner: %v", err)
			}
			h.assertVMRunning(t)
		})
	}
	config := h.child.config
	config.CommandLine = "console=ttyS0 boot_nonce=second"
	if _, err := h.exchange(t, request{Operation: "claim", Session: "recovered", Config: &config}); err != nil {
		t.Fatalf("restart command line prevented adoption: %v", err)
	}
	h.assertVMRunning(t)
}

func TestEgressAcceptRelayIsRevokedByClaim(t *testing.T) {
	h := serveChild(t)
	h.claim(t, "first")
	client := &Client{path: h.path, session: "first"}
	listener, err := client.Listen(netprotocol.EgressPort)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	failed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			failed <- err
		} else {
			accepted <- conn
		}
	}()
	waitRegisteredStreams(t, h.child, 1)
	peer, guest := unixPair(t)
	select {
	case h.child.incoming <- peer:
	case <-time.After(time.Second):
		t.Fatal("egress was not accepted")
	}
	var conn net.Conn
	select {
	case conn = <-accepted:
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("accept did not return")
	}
	defer conn.Close()
	if _, err := guest.Write([]byte("egress")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "egress" {
		t.Fatalf("egress relay: %q, %v", buf, err)
	}
	h.claim(t, "replacement")
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("old egress stream survived replacement")
	}
	_ = guest.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := guest.Read(buf); err != io.EOF {
		t.Fatalf("guest egress did not close: %v", err)
	}
	h.assertVMRunning(t)
}

// A Host that adopts a running VMM keeps its session after a failed start, so
// an explicit forced Stop still reaches the VM that holds the state disk.
func TestAdoptedVMStaysStoppableByItsController(t *testing.T) {
	h := serveChild(t)
	h.claim(t, "previous")
	config := h.child.config
	config.CommandLine = "console=ttyS0 boot_nonce=restarted-host"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Start(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if client.Spawned() {
		t.Fatal("adoption reported a newly spawned VM")
	}
	h.assertVMRunning(t)
	h.child.cancel = func() { h.cancelled.Add(1); h.child.state = hostv1.VMState_VM_STATE_STOPPED; close(h.child.done) }
	if err := client.Stop(); err != nil {
		t.Fatal(err)
	}
	if h.cancelled.Load() != 1 || client.State() != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("forced stop did not reach the adopted VM: cancels=%d state=%v", h.cancelled.Load(), client.State())
	}
}

func TestSpawnedMatchesOnlyItsOwnLaunch(t *testing.T) {
	for _, test := range []struct {
		name, requestedLaunch, actualLaunch string
		spawned                             bool
	}{
		{"matching-launch", "ours", "ours", true},
		{"competing-launch", "ours", "other", false},
		{"adopted-instance", "", "existing", false},
		{"empty-identity", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := serveChild(t)
			h.child.launch = test.actualLaunch
			client := &Client{path: h.path, session: "controller", launch: test.requestedLaunch, changes: make(chan hostv1.VMState, 8)}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := client.claim(ctx, h.child.config); err != nil {
				t.Fatal(err)
			}
			if client.Spawned() != test.spawned {
				t.Fatalf("spawned=%v want %v", client.Spawned(), test.spawned)
			}
			h.assertVMRunning(t)
		})
	}
}
