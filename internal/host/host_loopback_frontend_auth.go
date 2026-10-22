//go:build darwin || linux

package host

import (
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net"
	"time"
)

func (lease *hostLoopbackFrontendLease) authenticate(connection net.Conn) {
	capacity := lease.proto.GetEffectiveCapacity()
	handshakeTimeout := min(time.Duration(capacity.GetConnectionIdleSeconds())*time.Second, 5*time.Second)
	_ = connection.SetReadDeadline(time.Now().Add(handshakeTimeout))
	preface := make([]byte, hostFrontendPrefaceBytes)
	if _, err := io.ReadFull(connection, preface); err != nil || string(preface[:7]) != "AVFWD1 " || preface[71] != '\n' {
		rejectFrontendConnection(connection)
		return
	}
	decoded := make([]byte, 32)
	if !lowerHex(preface[7:71]) {
		rejectFrontendConnection(connection)
		return
	}
	if _, err := hex.Decode(decoded, preface[7:71]); err != nil || subtle.ConstantTimeCompare(decoded, lease.token[:]) != 1 {
		rejectFrontendConnection(connection)
		return
	}
	_ = connection.SetReadDeadline(time.Time{})
	deadline := minTime(lease.proto.GetExpiresAt().AsTime(), time.Now().Add(time.Duration(capacity.GetConnectionDeadlineSeconds())*time.Second))
	_ = connection.SetDeadline(minTime(deadline, time.Now().Add(time.Duration(capacity.GetConnectionIdleSeconds())*time.Second)))
	connectionID, ok := lease.manager.reserveConnection(lease, connection)
	if !ok {
		rejectFrontendConnection(connection)
		return
	}
	pending := &hostLoopbackPendingConnection{lease: lease, connectionID: connectionID, connection: connection, deadline: deadline}
	select {
	case lease.manager.queue(lease.proto.GetRegistrationId(), lease.proto.GetConnectionInstance(), lease.proto.GetConnectionEpoch()) <- pending:
	case <-lease.ctx.Done():
		lease.drop(connectionID)
	}
}

// A rejected capability must be observable by the forwarding helper even when
// the caller has already buffered its request. A TCP reset prevents a write to
// an unauthenticated connection from being mistaken for a successful forward.
func rejectFrontendConnection(connection net.Conn) {
	if tcp, ok := connection.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = connection.Close()
}
