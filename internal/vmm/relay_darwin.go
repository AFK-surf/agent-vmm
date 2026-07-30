//go:build darwin

package vmm

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"time"
)

func ServeRelay(ctx context.Context, socketPath string, manager *Manager) error {
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go relay(ctx, connection, manager)
	}
}

func relay(ctx context.Context, local net.Conn, manager *Manager) {
	defer local.Close()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	remote, err := manager.ConnectVMMD(connectCtx)
	cancel()
	if err != nil {
		return
	}
	defer remote.Close()

	finished := make(chan struct{}, 2)
	copyStream := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		if writer, ok := destination.(interface{ CloseWrite() error }); ok {
			_ = writer.CloseWrite()
		}
		finished <- struct{}{}
	}
	go copyStream(remote, local)
	go copyStream(local, remote)
	select {
	case <-ctx.Done():
	case <-finished:
		select {
		case <-finished:
		case <-ctx.Done():
		}
	}
}
