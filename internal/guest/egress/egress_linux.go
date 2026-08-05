//go:build linux

package egress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
)

type DialFunc func(context.Context) (net.Conn, error)

type Health struct {
	Configured     bool
	Connected      bool
	Reconnecting   bool
	LastError      string
	ReconnectCount uint64
	Drops          uint64
	LastRTT        time.Duration
}

type Pump struct {
	device io.ReadWriteCloser
	dial   DialFunc
	bootID string

	configured   atomic.Bool
	connected    atomic.Bool
	reconnecting atomic.Bool
	reconnects   atomic.Uint64
	drops        atomic.Uint64
	rttNanos     atomic.Int64
	lastError    atomic.Pointer[string]
	closeOnce    sync.Once
	writeMu      sync.Mutex
}

func New(bootID string) (*Pump, error) {
	device, err := createTUN()
	if err != nil {
		return nil, err
	}
	if err := configureTUN(); err != nil {
		_ = device.Close()
		return nil, err
	}
	pump := NewWithTransport(device, dialHost, bootID)
	pump.configured.Store(true)
	return pump, nil
}

func NewWithTransport(device io.ReadWriteCloser, dial DialFunc, bootID string) *Pump {
	return &Pump{device: device, dial: dial, bootID: bootID}
}

func (pump *Pump) Run(ctx context.Context) error {
	if pump.device == nil || pump.dial == nil || pump.bootID == "" {
		return errors.New("egress device, dialer, and boot ID are required")
	}
	packets := make(chan []byte, netprotocol.QueueSize)
	readErr := make(chan error, 1)
	go pump.readTUN(ctx, packets, readErr)
	backoff := 100 * time.Millisecond
	for {
		pump.reconnecting.Store(true)
		connection, err := pump.dial(ctx)
		if err == nil {
			err = pump.runSession(ctx, connection, packets)
			_ = connection.Close()
			if pump.connected.Load() {
				backoff = 100 * time.Millisecond
			}
		}
		pump.connected.Store(false)
		pump.reconnecting.Store(true)
		if err != nil && !errors.Is(err, context.Canceled) {
			message := err.Error()
			pump.lastError.Store(&message)
			pump.reconnects.Add(1)
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-readErr:
			if ctx.Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		case <-time.After(jitter(backoff)):
		}
		backoff *= 2
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

func (pump *Pump) runSession(ctx context.Context, connection net.Conn, packets <-chan []byte) error {
	if err := netprotocol.WriteHandshake(connection, netprotocol.Handshake{BootID: pump.bootID}); err != nil {
		return err
	}
	if _, err := netprotocol.ReadHandshake(connection, pump.bootID); err != nil {
		return err
	}
	pump.connected.Store(true)
	pump.reconnecting.Store(false)
	pump.lastError.Store(nil)
	readErr := make(chan error, 1)
	go func() { readErr <- pump.readFrames(ctx, connection) }()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			return err
		case packet := <-packets:
			if err := pump.writeFrame(connection, netprotocol.Data, packet); err != nil {
				return err
			}
		case sent := <-ticker.C:
			payload := make([]byte, 8)
			binaryPutUint64(payload, uint64(sent.UnixNano()))
			if err := pump.writeFrame(connection, netprotocol.Ping, payload); err != nil {
				return err
			}
		}
	}
}

func (pump *Pump) readFrames(ctx context.Context, connection net.Conn) error {
	for {
		_ = connection.SetReadDeadline(time.Now().Add(16 * time.Second))
		frameType, payload, err := netprotocol.ReadFrame(connection)
		if err != nil {
			return err
		}
		switch frameType {
		case netprotocol.Data:
			if _, err := pump.device.Write(payload); err != nil {
				return err
			}
		case netprotocol.Ping:
			if err := pump.writeFrame(connection, netprotocol.Pong, payload); err != nil {
				return err
			}
		case netprotocol.Pong:
			pump.rttNanos.Store(int64(time.Since(time.Unix(0, int64(binaryUint64(payload))))))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (pump *Pump) readTUN(ctx context.Context, packets chan<- []byte, result chan<- error) {
	buffer := make([]byte, netprotocol.MTU+1)
	for {
		count, err := pump.device.Read(buffer)
		if err != nil {
			result <- err
			return
		}
		if count > netprotocol.MTU {
			pump.drops.Add(1)
			continue
		}
		packet := append([]byte(nil), buffer[:count]...)
		select {
		case packets <- packet:
		default:
			pump.drops.Add(1)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (pump *Pump) writeFrame(connection net.Conn, frameType netprotocol.Type, payload []byte) error {
	pump.writeMu.Lock()
	defer pump.writeMu.Unlock()
	return netprotocol.WriteFrame(connection, frameType, payload)
}

func (pump *Pump) Close() error {
	var err error
	pump.closeOnce.Do(func() { err = pump.device.Close() })
	return err
}

func (pump *Pump) Health() Health {
	result := Health{Configured: pump.configured.Load(), Connected: pump.connected.Load(), Reconnecting: pump.reconnecting.Load(), ReconnectCount: pump.reconnects.Load(), Drops: pump.drops.Load(), LastRTT: time.Duration(pump.rttNanos.Load())}
	if value := pump.lastError.Load(); value != nil {
		result.LastError = *value
	}
	return result
}

func (pump *Pump) Ready() error {
	health := pump.Health()
	if !health.Configured {
		return errors.New("egress TUN is not configured")
	}
	if !health.Connected {
		if health.LastError != "" {
			return errors.New(health.LastError)
		}
		return errors.New("egress session is reconnecting")
	}
	return nil
}

func (pump *Pump) Status() string {
	health := pump.Health()
	return fmt.Sprintf("connected=%t reconnecting=%t reconnects=%d drops=%d rtt_ms=%d", health.Connected, health.Reconnecting, health.ReconnectCount, health.Drops, health.LastRTT.Milliseconds())
}

func jitter(base time.Duration) time.Duration {
	return time.Duration(float64(base) * (0.8 + rand.Float64()*0.4))
}

func binaryPutUint64(buffer []byte, value uint64) {
	for index := 7; index >= 0; index-- {
		buffer[index] = byte(value)
		value >>= 8
	}
}

func binaryUint64(buffer []byte) uint64 {
	var value uint64
	for _, item := range buffer {
		value = value<<8 | uint64(item)
	}
	return value
}
