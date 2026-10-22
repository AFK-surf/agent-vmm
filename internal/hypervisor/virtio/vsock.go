// Portions derived from tinyrange/cc, commit eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Licensed under the Apache License, Version 2.0; see ../LICENSE-cc.txt.
package virtio

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"syscall"
)

const (
	vsockBufferSize  = 128 << 10
	vsockMaxPayload  = 64 << 10
	vsockMaxPackets  = 256
	vsockDataPackets = 128
)

type socketKey struct{ host, guest uint32 }
type packet struct {
	header vsockHeader
	data   []byte
}

type VSock struct {
	*transport
	mu          sync.Mutex
	cid         uint64
	generation  uint64
	nextPort    uint32
	connections map[socketKey]*vsockConn
	listeners   map[uint32]*vsockListener
	pending     []packet
	changed     chan struct{}
	closed      bool
}

func NewVSock(mem GuestMemory, irq func(bool) error, cid uint64) *VSock {
	v := &VSock{cid: cid, nextPort: 1024, connections: make(map[socketKey]*vsockConn), listeners: make(map[uint32]*vsockListener), changed: make(chan struct{})}
	cfg := make([]byte, 8)
	binary.LittleEndian.PutUint64(cfg, cid)
	v.transport = newTransport(mem, irq, 19, 0, 3, cfg)
	v.process = v.processQueue
	v.reset = v.resetConnections
	v.start()
	return v
}

func (v *VSock) signal()          { close(v.changed); v.changed = make(chan struct{}) }
func (v *VSock) enqueue(p packet) { v.pending = append(v.pending, p); v.signal(); v.kick() }
func (v *VSock) header(c *vsockConn, op uint16) vsockHeader {
	return vsockHeader{SrcCID: VSockCIDHost, DstCID: v.cid, SrcPort: c.key.host, DstPort: c.key.guest, Type: vsockTypeStream, Op: op, BufAlloc: vsockBufferSize, FwdCnt: c.forwarded}
}
func (v *VSock) control(c *vsockConn, op uint16, flags uint32) {
	h := v.header(c, op)
	h.Flags = flags
	if op == vsockOpCreditUpdate {
		for i := len(v.pending) - 1; i >= 0; i-- {
			p := &v.pending[i]
			if p.header.SrcPort == c.key.host && p.header.DstPort == c.key.guest && p.header.Op == op {
				p.header = h
				v.kick()
				return
			}
		}
	}
	v.enqueue(packet{header: h})
}

func (v *VSock) flushControl() {
	for _, c := range v.connections {
		if len(v.pending) >= vsockMaxPackets {
			return
		}
		if c.shutdownPending != 0 {
			v.control(c, vsockOpShutdown, c.shutdownPending)
			c.shutdownPending = 0
			c.creditPending = false
		} else if c.creditPending {
			v.control(c, vsockOpCreditUpdate, 0)
			c.creditPending = false
		}
	}
}

func (v *VSock) resetConnections() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.generation++
	v.pending = nil
	for key, c := range v.connections {
		c.err = syscall.ECONNRESET
		delete(v.connections, key)
	}
	for _, l := range v.listeners {
		l.pending = nil
	}
	v.signal()
}

func (v *VSock) Close() error {
	v.mu.Lock()
	if !v.closed {
		v.closed = true
		for _, c := range v.connections {
			c.err = net.ErrClosed
			c.readClosed = true
			c.writeClosed = true
			c.received = nil
		}
		v.pending = nil
		v.signal()
	}
	v.mu.Unlock()
	return v.transport.Close()
}

func (v *VSock) DialContext(ctx context.Context, port uint32) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil, net.ErrClosed
	}
	for {
		v.nextPort++
		if v.nextPort < 1024 {
			v.nextPort = 1024
		}
		if v.connections[socketKey{v.nextPort, port}] == nil {
			break
		}
	}
	c := &vsockConn{device: v, key: socketKey{v.nextPort, port}}
	v.connections[c.key] = c
	for len(v.pending) >= vsockDataPackets {
		changed := v.changed
		v.mu.Unlock()
		err := waitSocket(ctx, changed, zeroTime)
		v.mu.Lock()
		if err != nil || v.closed {
			if err == nil {
				err = net.ErrClosed
			}
			v.remove(c, err, false)
			v.mu.Unlock()
			return nil, err
		}
	}
	if c.err != nil {
		err := c.err
		v.mu.Unlock()
		return nil, err
	}
	v.control(c, vsockOpRequest, 0)
	for !c.connected && c.err == nil {
		changed := v.changed
		v.mu.Unlock()
		err := waitSocket(ctx, changed, zeroTime)
		v.mu.Lock()
		if err != nil {
			v.remove(c, err, true)
			v.mu.Unlock()
			return nil, err
		}
	}
	err := c.err
	v.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (v *VSock) Listen(port uint32) (net.Listener, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, net.ErrClosed
	}
	if _, ok := v.listeners[port]; ok {
		return nil, fmt.Errorf("vsock port %d already listening", port)
	}
	l := &vsockListener{device: v, port: port}
	v.listeners[port] = l
	return l, nil
}

func (v *VSock) remove(c *vsockConn, err error, reset bool) {
	if c.err != nil {
		return
	}
	c.err = err
	delete(v.connections, c.key)
	out := v.pending[:0]
	for _, p := range v.pending {
		if p.header.SrcPort != c.key.host || p.header.DstPort != c.key.guest {
			out = append(out, p)
		}
	}
	clear(v.pending[len(out):])
	v.pending = out
	if reset && !v.closed && len(v.pending) < vsockMaxPackets {
		v.control(c, vsockOpRST, 0)
	}
	v.signal()
}

func (v *VSock) processQueue(index int, c *chain) (response, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.generation != c.generation {
		return response{}, false, nil
	}
	switch index {
	case 0:
		v.flushControl()
		if len(v.pending) == 0 {
			return response{}, false, nil
		}
		p := v.pending[0]
		capacity := c.writableBytes() - vsockHeaderSize
		if len(c.request) != 0 || capacity < 0 || (len(p.data) > 0 && capacity == 0) {
			return response{}, false, fmt.Errorf("vsock receive buffer is smaller than header")
		}
		n := min(len(p.data), capacity)
		h := p.header
		if conn := v.connections[socketKey{h.SrcPort, h.DstPort}]; conn != nil {
			h.FwdCnt = conn.forwarded
			conn.reported = conn.forwarded
		}
		h.Len = uint32(n)
		data := append(encodeVsockHeader(h), p.data[:n]...)
		if n < len(p.data) {
			v.pending[0].data = p.data[n:]
		} else {
			v.pending[0] = packet{}
			v.pending = v.pending[1:]
		}
		v.signal()
		v.kick()
		return response{data: data, used: uint32(len(data))}, true, nil
	case 1:
		if len(v.pending) >= vsockMaxPackets {
			return response{}, false, nil
		}
		h, err := parseVsockHeader(c.request)
		if err != nil {
			return response{}, false, err
		}
		if len(c.request)-vsockHeaderSize != int(h.Len) || h.Len > vsockMaxPayload || c.writableBytes() != 0 {
			return response{}, false, fmt.Errorf("invalid vsock packet length")
		}
		if h.SrcCID != v.cid || h.DstCID != VSockCIDHost || h.Type != vsockTypeStream {
			return response{}, true, nil
		}
		v.receive(h, c.request[vsockHeaderSize:])
		return response{}, true, nil
	case 2:
		// No transport-reset event is needed while this device instance is running.
		return response{}, false, nil
	}
	return response{}, false, nil
}

func (v *VSock) receive(h vsockHeader, data []byte) {
	key := socketKey{h.DstPort, h.SrcPort}
	c := v.connections[key]
	if c == nil {
		if h.Op == vsockOpRST {
			return
		}
		c = &vsockConn{device: v, key: key}
		l := v.listeners[key.host]
		if h.Op != vsockOpRequest || len(data) != 0 || l == nil || l.closed || len(l.pending) >= 64 || h.FwdCnt != 0 {
			v.control(c, vsockOpRST, 0)
			return
		}
		c.connected = true
		c.peerAlloc = h.BufAlloc
		v.connections[key] = c
		l.pending = append(l.pending, c)
		v.control(c, vsockOpResponse, 0)
		v.signal()
		return
	}
	if h.Op == vsockOpRST {
		v.remove(c, syscall.ECONNRESET, false)
		return
	}
	if h.FwdCnt-c.peerForwarded > c.sent-c.peerForwarded {
		v.remove(c, syscall.EPROTO, true)
		return
	}
	if h.FwdCnt != c.peerForwarded || h.BufAlloc != c.peerAlloc {
		c.creditRequested = false
	}
	c.peerForwarded = h.FwdCnt
	c.peerAlloc = h.BufAlloc
	if !c.connected && h.Op != vsockOpResponse {
		v.remove(c, syscall.EPROTO, true)
		return
	}
	if h.Op != vsockOpRW && len(data) != 0 {
		v.remove(c, syscall.EPROTO, true)
		return
	}
	switch h.Op {
	case vsockOpResponse:
		if c.connected {
			v.remove(c, syscall.EPROTO, true)
			return
		}
		c.connected = true
	case vsockOpRW:
		if c.peerSendClosed || len(data) > vsockBufferSize-len(c.received) {
			v.remove(c, syscall.EPROTO, true)
			return
		}
		if c.readClosed {
			c.forwarded += uint32(len(data))
		} else {
			c.received = append(c.received, data...)
		}
	case vsockOpShutdown:
		if h.Flags & ^uint32(vsockShutdownRecv|vsockShutdownSend) != 0 {
			v.remove(c, syscall.EPROTO, true)
			return
		}
		if h.Flags&vsockShutdownSend != 0 {
			c.peerSendClosed = true
		}
		if h.Flags&vsockShutdownRecv != 0 {
			c.peerRecvClosed = true
		}
	case vsockOpCreditRequest:
		v.control(c, vsockOpCreditUpdate, 0)
	case vsockOpCreditUpdate:
	default:
		v.remove(c, syscall.EPROTO, true)
		return
	}
	if c.closed && c.peerSendClosed && c.peerRecvClosed {
		v.remove(c, net.ErrClosed, true)
		return
	}
	v.signal()
}
