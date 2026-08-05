package netprotocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type shortWriter struct{ bytes.Buffer }

func (writer *shortWriter) Write(data []byte) (int, error) {
	if len(data) > 2 {
		data = data[:2]
	}
	return writer.Buffer.Write(data)
}

func TestHandshakeRoundTripWithPartialIO(t *testing.T) {
	var wire shortWriter
	if err := WriteHandshake(&wire, Handshake{BootID: "boot-1"}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadHandshake(io.LimitReader(&wire, int64(wire.Len())), "boot-1")
	if err != nil || got.BootID != "boot-1" {
		t.Fatalf("ReadHandshake() = %#v, %v", got, err)
	}
}

func TestFrameRoundTripAndValidation(t *testing.T) {
	packet := make([]byte, 20)
	packet[0] = 0x45
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	var wire shortWriter
	if err := WriteFrame(&wire, Data, packet); err != nil {
		t.Fatal(err)
	}
	frameType, got, err := ReadFrame(&wire)
	if err != nil || frameType != Data || !bytes.Equal(got, packet) {
		t.Fatalf("ReadFrame() = %v, %x, %v", frameType, got, err)
	}
	for _, test := range []struct {
		name    string
		type_   Type
		payload []byte
	}{
		{"unknown", 9, nil},
		{"short IPv4", Data, make([]byte, 19)},
		{"bad nonce", Ping, make([]byte, 7)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := WriteFrame(io.Discard, test.type_, test.payload); err == nil {
				t.Fatal("WriteFrame accepted invalid frame")
			}
		})
	}
}

func TestReadFrameRejectsHeaderAndCancellation(t *testing.T) {
	header := make([]byte, HeaderSize)
	header[0] = byte(Data)
	binary.BigEndian.PutUint32(header[4:], MTU+1)
	if _, _, err := ReadFrame(bytes.NewReader(header)); err == nil {
		t.Fatal("ReadFrame accepted oversized payload")
	}
	reader, writer := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := ReadFrameContext(ctx, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadFrameContext error = %v", err)
	}
	_ = writer.Close()
	_ = reader.Close()
}

func TestHandshakeRejectsMismatch(t *testing.T) {
	var wire bytes.Buffer
	if err := WriteHandshake(&wire, Handshake{BootID: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadHandshake(&wire, "new"); err == nil {
		t.Fatal("ReadHandshake accepted a stale boot")
	}
}
