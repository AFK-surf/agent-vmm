//go:build linux

package egress

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
)

type memoryDevice struct {
	reader  *io.PipeReader
	writer  *io.PipeWriter
	mu      sync.Mutex
	written bytes.Buffer
}

func newMemoryDevice() (*memoryDevice, *io.PipeWriter) {
	reader, input := io.Pipe()
	return &memoryDevice{reader: reader, writer: input}, input
}

func (device *memoryDevice) Read(buffer []byte) (int, error) { return device.reader.Read(buffer) }
func (device *memoryDevice) Write(buffer []byte) (int, error) {
	device.mu.Lock()
	defer device.mu.Unlock()
	return device.written.Write(buffer)
}
func (device *memoryDevice) Close() error { return device.reader.Close() }

func TestPumpHandshakePacketAndCancel(t *testing.T) {
	device, input := newMemoryDevice()
	guest, host := net.Pipe()
	pump := NewWithTransport(device, func(context.Context) (net.Conn, error) { return guest, nil }, "boot")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	if _, err := netprotocol.ReadHandshake(host, "boot"); err != nil {
		t.Fatal(err)
	}
	if err := netprotocol.WriteHandshake(host, netprotocol.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 20)
	packet[0] = 0x45
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	if _, err := input.Write(packet); err != nil {
		t.Fatal(err)
	}
	frameType, got, err := netprotocol.ReadFrame(host)
	if err != nil || frameType != netprotocol.Data || !bytes.Equal(got, packet) {
		t.Fatalf("frame = %v %x %v", frameType, got, err)
	}
	if err := netprotocol.WriteFrame(host, netprotocol.Data, packet); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		device.mu.Lock()
		matched := bytes.Equal(device.written.Bytes(), packet)
		device.mu.Unlock()
		if matched {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	_ = host.Close()
	_ = pump.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pump did not stop")
	}
}

func TestTUNQueuePressureDropsPackets(t *testing.T) {
	device, input := newMemoryDevice()
	pump := NewWithTransport(device, func(context.Context) (net.Conn, error) { return nil, errors.New("unused") }, "boot")
	packets := make(chan []byte, 1)
	result := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go pump.readTUN(ctx, packets, result)
	packet := make([]byte, 20)
	packet[0] = 0x45
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	if _, err := input.Write(packet); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Write(packet); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for pump.drops.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pump.drops.Load() != 1 {
		t.Fatalf("drops = %d", pump.drops.Load())
	}
	cancel()
	_ = pump.Close()
}
