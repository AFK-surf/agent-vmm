//go:build darwin

package vmm

import (
	"context"
	"net"
)

// VZ Connect cannot be cancelled. A connection belongs either to the receiver
// or to this goroutine, which closes it when the receiver has left.
func awaitVMMDConnection(ctx context.Context, connect func() (net.Conn, error)) (net.Conn, error) {
	type result struct {
		connection net.Conn
		err        error
	}
	ready := make(chan result)
	go func() {
		connection, err := connect()
		select {
		case ready <- result{connection: connection, err: err}:
		case <-ctx.Done():
			if connection != nil {
				_ = connection.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ready:
		return result.connection, result.err
	}
}
