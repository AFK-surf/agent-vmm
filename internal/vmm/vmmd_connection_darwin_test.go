//go:build darwin

package vmm

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestVMMDConnectionClosesLateResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	client, peer := net.Pipe()
	defer peer.Close()
	done := make(chan error, 1)
	go func() {
		connection, err := awaitVMMDConnection(ctx, func() (net.Conn, error) {
			close(started)
			<-release
			return client, nil
		})
		if connection != nil {
			connection.Close()
		}
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("connect = %v", err)
	}
	close(release)
	peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("late connection remained open: %v", err)
	}
}

func TestVMMDConnectionOwnershipAfterHandoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, peer := net.Pipe()
	defer peer.Close()
	connection, err := awaitVMMDConnection(ctx, func() (net.Conn, error) { return client, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	cancel()
	done := make(chan error, 1)
	go func() { _, err := peer.Write([]byte("x")); done <- err }()
	connection.SetReadDeadline(time.Now().Add(time.Second))
	data := make([]byte, 1)
	if _, err := io.ReadFull(connection, data); err != nil {
		t.Fatalf("caller-owned connection was closed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
