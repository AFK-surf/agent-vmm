package virtio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

type testVSockPeer struct {
	t      *testing.T
	v      *VSock
	mem    *testMemory
	rx, tx *testQueue
}

func newVSockPeer(t *testing.T) *testVSockPeer {
	t.Helper()
	mem := newTestMemory()
	v := NewVSock(mem, nil, 3)
	t.Cleanup(func() { _ = v.Close() })
	p := &testVSockPeer{t: t, v: v, mem: mem}
	p.rx = setupQueue(t, v.transport, mem, 0)
	p.tx = setupQueue(t, v.transport, mem, 1)
	negotiate(t, v.transport)
	return p
}
func (p *testVSockPeer) send(h vsockHeader, data []byte) {
	p.t.Helper()
	h.SrcCID = 3
	h.DstCID = VSockCIDHost
	h.Type = vsockTypeStream
	h.Len = uint32(len(data))
	raw := append(encodeVsockHeader(h), data...)
	_ = p.mem.WriteIPA(0x20000, raw)
	p.tx.descriptor(0, 0x20000, uint32(len(raw)), 0, 0)
	p.tx.submit()
	p.tx.wait()
}
func (p *testVSockPeer) receive(capacity int) (vsockHeader, []byte) {
	p.t.Helper()
	p.rx.descriptor(0, 0x30000, uint32(capacity), descFWrite, 0)
	p.rx.submit()
	p.rx.wait()
	elem, _ := p.mem.ReadIPA(p.rx.used+4+uint64((p.rx.completed-1)%8)*8, 8)
	size := binary.LittleEndian.Uint32(elem[4:])
	raw, _ := p.mem.ReadIPA(0x30000, int(size))
	h, err := parseVsockHeader(raw)
	if err != nil {
		p.t.Fatal(err)
	}
	if h.SrcCID != VSockCIDHost || h.DstCID != 3 || int(h.Len) != len(raw)-vsockHeaderSize {
		p.t.Fatalf("invalid host packet: %+v", h)
	}
	return h, raw[vsockHeaderSize:]
}
func (p *testVSockPeer) dial(port uint32, credit uint32) net.Conn {
	p.t.Helper()
	result := make(chan net.Conn, 1)
	fail := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		c, err := p.v.DialContext(ctx, port)
		if err != nil {
			fail <- err
		} else {
			result <- c
		}
	}()
	h, _ := p.receive(128)
	if h.Op != vsockOpRequest || h.DstPort != port {
		p.t.Fatalf("expected request, got %+v", h)
	}
	p.send(vsockHeader{SrcPort: port, DstPort: h.SrcPort, Op: vsockOpResponse, BufAlloc: credit}, nil)
	select {
	case c := <-result:
		return c
	case err := <-fail:
		p.t.Fatal(err)
	case <-ctx.Done():
		p.t.Fatal(ctx.Err())
	}
	return nil
}

func TestVSockHostDialCreditAndHalfClose(t *testing.T) {
	p := newVSockPeer(t)
	conn := p.dial(5000, 5)
	defer conn.Close()
	type writeResult struct {
		n   int
		err error
	}
	written := make(chan writeResult, 1)
	go func() { n, err := conn.Write([]byte("helloworld")); written <- writeResult{n, err} }()
	h, data := p.receive(4096)
	if h.Op != vsockOpRW || string(data) != "hello" {
		t.Fatalf("first credit window: %+v %q", h, data)
	}
	select {
	case result := <-written:
		t.Fatalf("write ignored credit: %+v", result)
	default:
	}
	request, _ := p.receive(128)
	if request.Op != vsockOpCreditRequest {
		t.Fatalf("missing credit request: %+v", request)
	}
	p.send(vsockHeader{SrcPort: 5000, DstPort: h.SrcPort, Op: vsockOpCreditUpdate, BufAlloc: 5, FwdCnt: 5}, nil)
	h, data = p.receive(4096)
	if h.Op != vsockOpRW || string(data) != "world" {
		t.Fatalf("second credit window: %+v %q", h, data)
	}
	select {
	case result := <-written:
		if result.n != 10 || result.err != nil {
			t.Fatalf("write: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("write stayed blocked")
	}
	p.send(vsockHeader{SrcPort: 5000, DstPort: h.SrcPort, Op: vsockOpRW, BufAlloc: 5, FwdCnt: 10}, []byte("reply"))
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "reply" {
		t.Fatalf("read: %q %v", buf, err)
	}
	p.v.mu.Lock()
	queued := len(p.v.pending)
	p.v.mu.Unlock()
	if queued != 0 {
		t.Fatalf("small read queued %d credit packets", queued)
	}
	bulk := make([]byte, vsockBufferSize/2-5)
	p.send(vsockHeader{SrcPort: 5000, DstPort: h.SrcPort, Op: vsockOpRW, BufAlloc: 5, FwdCnt: 10}, bulk)
	if _, err := io.ReadFull(conn, bulk); err != nil {
		t.Fatal(err)
	}
	credit, _ := p.receive(128)
	if credit.Op != vsockOpCreditUpdate || credit.FwdCnt != vsockBufferSize/2 {
		t.Fatalf("read did not return credit at threshold: %+v", credit)
	}
	p.send(vsockHeader{SrcPort: 5000, DstPort: h.SrcPort, Op: vsockOpShutdown, Flags: vsockShutdownSend, BufAlloc: 5, FwdCnt: 10}, nil)
	if _, err := conn.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("guest half-close read: %v", err)
	}
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	shutdown, _ := p.receive(128)
	if shutdown.Op != vsockOpShutdown || shutdown.Flags != vsockShutdownSend {
		t.Fatalf("host half-close: %+v", shutdown)
	}
}

func TestVSockGuestDialDeadlinesAndClose(t *testing.T) {
	p := newVSockPeer(t)
	listener, err := p.v.Listen(1024)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	p.send(vsockHeader{SrcPort: 4000, DstPort: 1024, Op: vsockOpRequest}, nil)
	h, _ := p.receive(128)
	if h.Op != vsockOpResponse {
		t.Fatalf("guest connection rejected: %+v", h)
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err := conn.Write([]byte("no credit")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write deadline: %v", err)
	}
	_ = conn.SetDeadline(time.Time{})
	readDone := make(chan error, 1)
	writeDone := make(chan error, 1)
	go func() { _, err := conn.Read(make([]byte, 1)); readDone <- err }()
	go func() { _, err := conn.Write([]byte("still no credit")); writeDone <- err }()
	if err := p.v.Close(); err != nil {
		t.Fatal(err)
	}
	for _, done := range []chan error{readDone, writeDone} {
		select {
		case err := <-done:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("close wakeup: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("close did not wake blocked I/O")
		}
	}
	if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("device close did not stop listener: %v", err)
	}
}

func TestVSockCancellationAndReset(t *testing.T) {
	p := newVSockPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := p.v.DialContext(ctx, 1024); result <- err }()
	h, _ := p.receive(128)
	if h.Op != vsockOpRequest {
		t.Fatal("missing dial request")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial cancellation blocked")
	}
	rst, _ := p.receive(128)
	if rst.Op != vsockOpRST {
		t.Fatalf("cancel did not reset stream: %+v", rst)
	}
	conn := p.dial(1024, 10)
	defer conn.Close()
	done := make(chan error, 1)
	go func() { _, err := conn.Read(make([]byte, 1)); done <- err }()
	writeReg(t, p.v.transport, regStatus, 0)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("reset did not fail connection")
		}
	case <-time.After(time.Second):
		t.Fatal("reset did not wake read")
	}
}

func TestVSockReceiveSplitsPacketsAndReturnsCredits(t *testing.T) {
	p := newVSockPeer(t)
	conn := p.dial(3000, 1<<20)
	defer conn.Close()
	data := bytes.Repeat([]byte{0x73}, 32<<10)
	if n, err := conn.Write(data); err != nil || n != len(data) {
		t.Fatalf("write: %d %v", n, err)
	}
	var got []byte
	for len(got) < len(data) {
		h, part := p.receive(128)
		if h.Op != vsockOpRW {
			t.Fatalf("expected data: %+v", h)
		}
		got = append(got, part...)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("fragmented stream corrupted")
	}
}

func TestVSockBackpressureDoesNotBlockOtherStreams(t *testing.T) {
	p := newVSockPeer(t)
	first := p.dial(5000, 0)
	defer first.Close()
	second := p.dial(5001, 1<<20)
	defer second.Close()
	blocked := make(chan error, 1)
	go func() { _, err := first.Write([]byte("blocked")); blocked <- err }()
	request, _ := p.receive(128)
	if request.Op != vsockOpCreditRequest || request.DstPort != 5000 {
		t.Fatalf("missing credit request: %+v", request)
	}
	if n, err := second.Write([]byte("alive")); n != 5 || err != nil {
		t.Fatalf("unrelated stream blocked: %d %v", n, err)
	}
	h, data := p.receive(128)
	if h.Op != vsockOpRW || h.DstPort != 5001 || string(data) != "alive" {
		t.Fatalf("unexpected packet: %+v %q", h, data)
	}
	_ = first.Close()
	select {
	case err := <-blocked:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("blocked write close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close could not interrupt backpressure")
	}
}

func TestVSockClosePreservesQueuedResponseUntilGuestDrains(t *testing.T) {
	p := newVSockPeer(t)
	conn := p.dial(5000, 1<<20)
	response := bytes.Repeat([]byte("finite response"), 1000)
	if n, err := conn.Write(response); err != nil || n != len(response) {
		t.Fatalf("write: %d %v", n, err)
	}
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("after close")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed write: %v", err)
	}
	var received []byte
	var last vsockHeader
	for len(received) < len(response) {
		h, data := p.receive(1024)
		if h.Op != vsockOpRW {
			t.Fatalf("response was replaced by operation %d after %d bytes", h.Op, len(received))
		}
		received = append(received, data...)
		last = h
	}
	if !bytes.Equal(received, response) {
		t.Fatal("queued response changed")
	}
	flags := uint32(0)
	for flags != vsockShutdownRecv|vsockShutdownSend {
		h, _ := p.receive(128)
		if h.Op != vsockOpShutdown {
			t.Fatalf("expected shutdown after response, got %+v", h)
		}
		flags |= h.Flags
	}
	p.send(vsockHeader{SrcPort: 5000, DstPort: last.SrcPort, Op: vsockOpShutdown, Flags: vsockShutdownRecv | vsockShutdownSend, BufAlloc: 1 << 20, FwdCnt: uint32(len(response))}, nil)
	rst, _ := p.receive(128)
	if rst.Op != vsockOpRST {
		t.Fatalf("peer completion did not retire stream: %+v", rst)
	}
	p.v.mu.Lock()
	remaining := len(p.v.connections)
	p.v.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("closed connection retained after peer completion: %d", remaining)
	}
}

func TestVSockAbortDiscardsQueuedControllerOutput(t *testing.T) {
	p := newVSockPeer(t)
	conn := p.dial(5000, 1<<20)
	if _, err := conn.Write([]byte("revoked command")); err != nil {
		t.Fatal(err)
	}
	if err := conn.(interface{ Abort() error }).Abort(); err != nil {
		t.Fatal(err)
	}
	h, data := p.receive(128)
	if h.Op != vsockOpRST || len(data) != 0 {
		t.Fatalf("aborted stream delivered queued output: %+v %q", h, data)
	}
}

func TestVSockCloseResetsUnansweredStream(t *testing.T) {
	timeout := vsockCloseTimeout
	vsockCloseTimeout = 10 * time.Millisecond
	t.Cleanup(func() { vsockCloseTimeout = timeout })
	p := newVSockPeer(t)
	conn := p.dial(5000, 1<<20)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if h, _ := p.receive(128); h.Op != vsockOpShutdown {
		t.Fatalf("expected shutdown: %+v", h)
	}
	if h, _ := p.receive(128); h.Op != vsockOpRST {
		t.Fatalf("unanswered close did not reset stream: %+v", h)
	}
	p.v.mu.Lock()
	remaining := len(p.v.connections)
	p.v.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("unanswered close retained %d connections", remaining)
	}
}
