package servicepreface

import (
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"
)

const (
	Magic              = "AVM-SVC-TUNNEL1\x00"
	MaxCapabilityBytes = 4096
	prefaceTimeout     = 3 * time.Second
)

// Listener consumes and validates a target-local install capability before
// gRPC sees the byte stream. The secret is added after Iroh termination and
// never crosses the device boundary.
type Listener struct {
	net.Listener
	capability []byte
}

func New(listener net.Listener, capability []byte) (*Listener, error) {
	if listener == nil || len(capability) < 32 || len(capability) > MaxCapabilityBytes {
		return nil, errors.New("listener and bounded 256-bit capability are required")
	}
	return &Listener{Listener: listener, capability: append([]byte(nil), capability...)}, nil
}

func (listener *Listener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if listener.authorize(connection) {
			return connection, nil
		}
		_ = connection.Close()
	}
}

func (listener *Listener) authorize(connection net.Conn) bool {
	_ = connection.SetReadDeadline(time.Now().Add(prefaceTimeout))
	header := make([]byte, len(Magic)+4)
	if _, err := io.ReadFull(connection, header); err != nil || string(header[:len(Magic)]) != Magic {
		return false
	}
	length := int(binary.BigEndian.Uint32(header[len(Magic):]))
	if length != len(listener.capability) || length < 32 || length > MaxCapabilityBytes {
		return false
	}
	actual := make([]byte, length)
	if _, err := io.ReadFull(connection, actual); err != nil || subtle.ConstantTimeCompare(actual, listener.capability) != 1 {
		return false
	}
	_ = connection.SetReadDeadline(time.Time{})
	return true
}
