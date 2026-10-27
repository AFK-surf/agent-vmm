//go:build linux

package vsock

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLoopbackDeadlinesAndClose(t *testing.T) {
	listener, err := Listen(unix.VMADDR_PORT_ANY)
	if err != nil {
		t.Skipf("vsock loopback unavailable: %v", err)
	}
	defer listener.Close()
	raw, err := listener.(*Listener).file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var port uint32
	if err := raw.Control(func(fd uintptr) {
		address, socketErr := unix.Getsockname(int(fd))
		if socketErr == nil {
			port = address.(*unix.SockaddrVM).Port
		}
	}); err != nil || port == 0 {
		t.Fatalf("listener port=%d err=%v", port, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := Dial(ctx, unix.VMADDR_CID_LOCAL, port)
	if err != nil {
		t.Skipf("vsock loopback unavailable: %v", err)
	}
	defer client.Close()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Read(make([]byte, 1)); err == nil {
		t.Fatal("read deadline did not fire")
	} else if !os.IsTimeout(err) {
		t.Fatalf("deadline = %v", err)
	}
	if err := server.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() { _, err := server.Read(make([]byte, 1)); read <- err }()
	server.Close()
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("closed read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake Read")
	}
	accepted := make(chan error, 1)
	go func() { _, err := listener.Accept(); accepted <- err }()
	listener.Close()
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("closed accept succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake Accept")
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := Dial(canceled, unix.VMADDR_CID_LOCAL, port); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Dial = %v", err)
	}
}
