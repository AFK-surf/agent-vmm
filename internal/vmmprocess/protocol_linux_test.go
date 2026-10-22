//go:build linux

package vmmprocess

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"golang.org/x/sys/unix"
)

func unixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	var conns [2]*net.UnixConn
	for i, fd := range fds {
		file := os.NewFile(uintptr(fd), "test socket")
		conn, err := net.FileConn(file)
		_ = file.Close()
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = conn.(*net.UnixConn)
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}
	return conns[0], conns[1]
}

func TestRelayPreservesBufferedDataAndHalfCloses(t *testing.T) {
	client, left := unixPair(t)
	right, guest := unixPair(t)
	requestData := bytes.Repeat([]byte("request"), 1000)
	if _, err := client.Write(requestData); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(left)
	if _, err := reader.Peek(1); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { relay(&stream{left, reader}, right); close(done) }()
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(guest)
	if err != nil || !bytes.Equal(got, requestData) {
		t.Fatalf("request after half-close: %d bytes, %v", len(got), err)
	}
	response := []byte("response after request EOF")
	if _, err := guest.Write(response); err != nil {
		t.Fatal(err)
	}
	if err := guest.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(client)
	if err != nil || !bytes.Equal(got, response) {
		t.Fatalf("response after half-close: %q, %v", got, err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not finish both directions")
	}
}

func listenTestUnix(t *testing.T) *net.UnixListener {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vmm.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestExchangePreservesReplyReadAhead(t *testing.T) {
	listener := listenTestUnix(t)
	payload := []byte{0, 1, 0xff, 4, '\n', 7}
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		var req request
		if err := readMessage(bufio.NewReader(conn), &req); err != nil {
			serverDone <- err
			return
		}
		header, _ := json.Marshal(reply{State: hostv1.VMState_VM_STATE_RUNNING})
		header = append(header, '\n')
		_, err = conn.Write(append(header, payload...))
		serverDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, r, err := exchange(ctx, listener.Addr().String(), request{Operation: "dial", Session: "owner", Port: 4050})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if r.State != hostv1.VMState_VM_STATE_RUNNING {
		t.Fatalf("unexpected state: %v", r.State)
	}
	got, err := io.ReadAll(conn)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("buffered stream payload: %v, %v", got, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestExchangeCancellationClosesPendingSocket(t *testing.T) {
	listener := listenTestUnix(t)
	received := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			peerDone <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		var req request
		if err := readMessage(reader, &req); err != nil {
			peerDone <- err
			return
		}
		close(received)
		_, err = reader.ReadByte()
		peerDone <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := exchange(ctx, listener.Addr().String(), request{Operation: "accept", Session: "owner"})
		result <- err
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("server did not receive request")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("exchange cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exchange ignored cancellation")
	}
	select {
	case err := <-peerDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("canceled socket remained open: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not observe cancellation")
	}
}
