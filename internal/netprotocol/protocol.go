package netprotocol

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
)

const (
	MTU        = 1500
	HeaderSize = 8
	MaxFrame   = HeaderSize + MTU
	EgressPort = 4051
	QueueSize  = 1024
)

const (
	TunName         = "avm-egress0"
	GuestAddress    = "100.127.255.2"
	HostAddress     = "100.127.255.1"
	VirtualDNS      = HostAddress + ":53"
	TunPrefix       = "100.127.255.0/30"
	ProtocolVersion = uint16(1)
)

var magic = [8]byte{'A', 'V', 'M', 'E', 'G', 'R', 'E', 'S'}

var ErrInvalidFrame = errors.New("invalid egress frame")

type Type uint8

const (
	Data Type = iota + 1
	Ping
	Pong
)

type Handshake struct {
	BootID string
}

func WriteHandshake(writer io.Writer, handshake Handshake) error {
	if len(handshake.BootID) == 0 || len(handshake.BootID) > 255 {
		return errors.New("boot ID must contain 1 to 255 bytes")
	}
	buffer := make([]byte, 13+len(handshake.BootID))
	copy(buffer, magic[:])
	binary.BigEndian.PutUint16(buffer[8:10], ProtocolVersion)
	binary.BigEndian.PutUint16(buffer[10:12], MTU)
	buffer[12] = byte(len(handshake.BootID))
	copy(buffer[13:], handshake.BootID)
	return writeFull(writer, buffer)
}

func ReadHandshake(reader io.Reader, expectedBootID string) (Handshake, error) {
	header := make([]byte, 13)
	if _, err := io.ReadFull(reader, header); err != nil {
		return Handshake{}, err
	}
	if string(header[:8]) != string(magic[:]) {
		return Handshake{}, errors.New("invalid egress handshake magic")
	}
	if binary.BigEndian.Uint16(header[8:10]) != ProtocolVersion {
		return Handshake{}, errors.New("unsupported egress protocol version")
	}
	if binary.BigEndian.Uint16(header[10:12]) != MTU {
		return Handshake{}, errors.New("egress MTU mismatch")
	}
	length := int(header[12])
	if length == 0 {
		return Handshake{}, errors.New("empty boot ID")
	}
	bootID := make([]byte, length)
	if _, err := io.ReadFull(reader, bootID); err != nil {
		return Handshake{}, err
	}
	result := Handshake{BootID: string(bootID)}
	if expectedBootID != "" && result.BootID != expectedBootID {
		return Handshake{}, errors.New("egress boot ID mismatch")
	}
	return result, nil
}

func WriteFrame(writer io.Writer, frameType Type, payload []byte) error {
	if err := validateFrame(frameType, payload); err != nil {
		return err
	}
	header := [HeaderSize]byte{byte(frameType)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
	if err := writeFull(writer, header[:]); err != nil {
		return err
	}
	return writeFull(writer, payload)
}

func ReadFrame(reader io.Reader) (Type, []byte, error) {
	header := [HeaderSize]byte{}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	if header[1] != 0 || header[2] != 0 || header[3] != 0 {
		return 0, nil, fmt.Errorf("%w: nonzero header flags", ErrInvalidFrame)
	}
	frameType := Type(header[0])
	length := binary.BigEndian.Uint32(header[4:])
	if length > MTU {
		return 0, nil, fmt.Errorf("%w: oversized payload", ErrInvalidFrame)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	if err := validateFrame(frameType, payload); err != nil {
		return 0, nil, err
	}
	return frameType, payload, nil
}

func ReadFrameContext(ctx context.Context, reader io.ReadCloser) (Type, []byte, error) {
	type result struct {
		type_   Type
		payload []byte
		err     error
	}
	ready := make(chan result, 1)
	go func() {
		frameType, payload, err := ReadFrame(reader)
		ready <- result{type_: frameType, payload: payload, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = reader.Close()
		<-ready
		return 0, nil, ctx.Err()
	case result := <-ready:
		return result.type_, result.payload, result.err
	}
}

func validateFrame(frameType Type, payload []byte) error {
	switch frameType {
	case Data:
		if len(payload) < 20 || len(payload) > MTU || payload[0]>>4 != 4 {
			return fmt.Errorf("%w: DATA must contain one IPv4 packet", ErrInvalidFrame)
		}
		headerLength := int(payload[0]&0xf) * 4
		if headerLength < 20 || headerLength > len(payload) {
			return fmt.Errorf("%w: invalid IPv4 header length", ErrInvalidFrame)
		}
		if int(binary.BigEndian.Uint16(payload[2:4])) != len(payload) {
			return fmt.Errorf("%w: IPv4 packet length mismatch", ErrInvalidFrame)
		}
	case Ping, Pong:
		if len(payload) != 8 {
			return fmt.Errorf("%w: keepalive nonce must be 8 bytes", ErrInvalidFrame)
		}
	default:
		return fmt.Errorf("%w: type %d", ErrInvalidFrame, frameType)
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		count, err := writer.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func Addresses() (guest, host netip.Addr, prefix netip.Prefix) {
	return netip.MustParseAddr(GuestAddress), netip.MustParseAddr(HostAddress), netip.MustParsePrefix(TunPrefix)
}
