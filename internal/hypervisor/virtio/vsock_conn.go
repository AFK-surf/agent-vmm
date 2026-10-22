package virtio

import (
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

var zeroTime time.Time
var vsockCloseTimeout = 8 * time.Second

type vsockAddr struct {
	cid  uint64
	port uint32
}

func (a vsockAddr) Network() string { return "vsock" }
func (a vsockAddr) String() string {
	return strconv.FormatUint(a.cid, 10) + ":" + strconv.FormatUint(uint64(a.port), 10)
}

type vsockConn struct {
	closed                                                  bool
	creditRequested                                         bool
	device                                                  *VSock
	key                                                     socketKey
	connected                                               bool
	err                                                     error
	received                                                []byte
	forwarded, reported, sent, peerForwarded, peerAlloc     uint32
	readClosed, writeClosed, peerSendClosed, peerRecvClosed bool
	creditPending                                           bool
	shutdownPending                                         uint32
	readDeadline, writeDeadline                             time.Time
}

func waitSocket(ctx context.Context, changed <-chan struct{}, deadline time.Time) error {
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return os.ErrDeadlineExceeded
		}
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timeout:
		return os.ErrDeadlineExceeded
	}
}

func (c *vsockConn) Read(p []byte) (int, error) {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if c.readClosed {
			return 0, net.ErrClosed
		}
		if !c.readDeadline.IsZero() && !time.Now().Before(c.readDeadline) {
			return 0, os.ErrDeadlineExceeded
		}
		if len(c.received) > 0 {
			n := copy(p, c.received)
			copy(c.received, c.received[n:])
			c.received = c.received[:len(c.received)-n]
			c.forwarded += uint32(n)
			// Like Linux, report credit once half the buffer has been consumed unreported.
			if c.forwarded-c.reported >= vsockBufferSize/2 {
				c.creditPending = true
				v.flushControl()
			}
			return n, nil
		}
		if c.err != nil {
			return 0, c.err
		}
		if c.peerSendClosed {
			return 0, io.EOF
		}
		changed := v.changed
		deadline := c.readDeadline
		v.mu.Unlock()
		err := waitSocket(context.Background(), changed, deadline)
		v.mu.Lock()
		if err != nil {
			return 0, err
		}
	}
}

func (c *vsockConn) Write(p []byte) (int, error) {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	written := 0
	for len(p) > 0 {
		if c.closed {
			return written, net.ErrClosed
		}
		if c.err != nil {
			return written, c.err
		}
		if c.writeClosed || c.peerRecvClosed {
			return written, io.ErrClosedPipe
		}
		if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
			return written, os.ErrDeadlineExceeded
		}
		outstanding := c.sent - c.peerForwarded
		var credit uint32
		if outstanding <= c.peerAlloc {
			credit = c.peerAlloc - outstanding
		}
		if credit > 0 && len(v.pending) < vsockDataPackets {
			n := min(len(p), vsockMaxPayload, int(credit))
			h := v.header(c, vsockOpRW)
			h.Len = uint32(n)
			v.enqueue(packet{header: h, data: append([]byte(nil), p[:n]...)})
			c.sent += uint32(n)
			written += n
			p = p[n:]
			continue
		}
		if credit == 0 && !c.creditRequested && len(v.pending) < vsockDataPackets {
			c.creditRequested = true
			v.control(c, vsockOpCreditRequest, 0)
		}
		changed := v.changed
		deadline := c.writeDeadline
		v.mu.Unlock()
		err := waitSocket(context.Background(), changed, deadline)
		v.mu.Lock()
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (c *vsockConn) Close() error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.readClosed = true
	c.writeClosed = true
	c.forwarded += uint32(len(c.received))
	c.received = nil
	if c.err == nil {
		c.shutdownPending |= vsockShutdownRecv | vsockShutdownSend
		v.flushControl()
		if c.peerSendClosed && c.peerRecvClosed {
			v.remove(c, net.ErrClosed, true)
		} else {
			// Like Linux, reset a stream whose peer never completes the shutdown.
			time.AfterFunc(vsockCloseTimeout, func() {
				v.mu.Lock()
				defer v.mu.Unlock()
				v.remove(c, net.ErrClosed, true)
			})
		}
	}
	v.signal()
	return nil
}

// Abort discards queued output when the transport fails or its controller is fenced.
func (c *vsockConn) Abort() error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	c.closed = true
	c.readClosed = true
	c.writeClosed = true
	c.received = nil
	v.remove(c, net.ErrClosed, true)
	return nil
}

func (c *vsockConn) CloseRead() error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if c.readClosed {
		return nil
	}
	c.readClosed = true
	c.forwarded += uint32(len(c.received))
	c.received = nil
	c.shutdownPending |= vsockShutdownRecv
	v.flushControl()
	v.signal()
	return nil
}

func (c *vsockConn) CloseWrite() error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if c.writeClosed {
		return nil
	}
	c.writeClosed = true
	c.shutdownPending |= vsockShutdownSend
	v.flushControl()
	v.signal()
	return nil
}

func (c *vsockConn) LocalAddr() net.Addr  { return vsockAddr{VSockCIDHost, c.key.host} }
func (c *vsockConn) RemoteAddr() net.Addr { return vsockAddr{c.device.cid, c.key.guest} }
func (c *vsockConn) SetDeadline(t time.Time) error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.readDeadline = t
	c.writeDeadline = t
	v.signal()
	return nil
}
func (c *vsockConn) SetReadDeadline(t time.Time) error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.readDeadline = t
	v.signal()
	return nil
}
func (c *vsockConn) SetWriteDeadline(t time.Time) error {
	v := c.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.writeDeadline = t
	v.signal()
	return nil
}

type vsockListener struct {
	device  *VSock
	port    uint32
	closed  bool
	pending []*vsockConn
}

func (l *vsockListener) Accept() (net.Conn, error) {
	v := l.device
	v.mu.Lock()
	defer v.mu.Unlock()
	for {
		if l.closed || v.closed {
			return nil, net.ErrClosed
		}
		if len(l.pending) > 0 {
			c := l.pending[0]
			l.pending[0] = nil
			l.pending = l.pending[1:]
			if c.err != nil {
				continue
			}
			return c, nil
		}
		changed := v.changed
		v.mu.Unlock()
		<-changed
		v.mu.Lock()
	}
}
func (l *vsockListener) Close() error {
	v := l.device
	v.mu.Lock()
	defer v.mu.Unlock()
	if !l.closed {
		l.closed = true
		delete(v.listeners, l.port)
		for _, c := range l.pending {
			v.remove(c, net.ErrClosed, true)
		}
		l.pending = nil
		v.signal()
	}
	return nil
}
func (l *vsockListener) Addr() net.Addr { return vsockAddr{VSockCIDHost, l.port} }
