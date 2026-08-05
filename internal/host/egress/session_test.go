package egress

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"golang.org/x/net/dns/dnsmessage"
)

func TestDuplicateSessionRejected(t *testing.T) {
	subsystem := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guest, host := net.Pipe()
	result := make(chan error, 1)
	go func() { result <- subsystem.Serve(ctx, host, "boot") }()
	if err := netprotocol.WriteHandshake(guest, netprotocol.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	if _, err := netprotocol.ReadHandshake(guest, "boot"); err != nil {
		t.Fatal(err)
	}
	duplicate, duplicateHost := net.Pipe()
	if err := subsystem.Serve(ctx, duplicateHost, "boot"); err == nil {
		t.Fatal("Serve accepted a duplicate session")
	}
	_ = duplicate.Close()
	subsystem.Close()
	_ = guest.Close()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("session did not stop")
	}
}

func TestVirtualDNSPacketRoundTrip(t *testing.T) {
	subsystem := New()
	lease, err := subsystem.Leases.Acquire(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	guest, host := net.Pipe()
	result := make(chan error, 1)
	go func() { result <- subsystem.Serve(context.Background(), host, "boot") }()
	if err := netprotocol.WriteHandshake(guest, netprotocol.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	if _, err := netprotocol.ReadHandshake(guest, "boot"); err != nil {
		t.Fatal(err)
	}
	name := dnsmessage.MustNewName("localhost.")
	query := dnsmessage.Message{Header: dnsmessage.Header{ID: 7, RecursionDesired: true}, Questions: []dnsmessage.Question{{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}
	payload, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	packet := udpPacket(netip.MustParseAddr(netprotocol.GuestAddress), netip.MustParseAddr(netprotocol.HostAddress), 32000, 53, payload)
	if err := netprotocol.WriteFrame(guest, netprotocol.Data, packet); err != nil {
		t.Fatal(err)
	}
	_ = guest.SetReadDeadline(time.Now().Add(3 * time.Second))
	frameType, response, err := netprotocol.ReadFrame(guest)
	if err != nil {
		t.Fatalf("DNS response: %v; metrics: %+v", err, subsystem.Metrics())
	}
	if frameType != netprotocol.Data || len(response) < 28 || netip.AddrFrom4([4]byte(response[12:16])) != netip.MustParseAddr(netprotocol.HostAddress) {
		t.Fatalf("DNS response frame = %v %x", frameType, response)
	}
	lease.Release()
	if err := netprotocol.WriteFrame(guest, netprotocol.Data, packet); err != nil {
		t.Fatal(err)
	}
	_, deniedResponse, err := netprotocol.ReadFrame(guest)
	if err != nil {
		t.Fatal(err)
	}
	var deniedMessage dnsmessage.Message
	if err := deniedMessage.Unpack(deniedResponse[28:]); err != nil || deniedMessage.RCode != dnsmessage.RCodeServerFailure {
		t.Fatalf("DNS without lease = %v, %v", deniedMessage.RCode, err)
	}
	secondLease, err := subsystem.Leases.Acquire(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer secondLease.Release()
	if err := netprotocol.WriteFrame(guest, netprotocol.Data, packet); err != nil {
		t.Fatal(err)
	}
	_, allowedResponse, err := netprotocol.ReadFrame(guest)
	if err != nil {
		t.Fatalf("DNS tuple was not reusable after lease release: %v", err)
	}
	var allowedMessage dnsmessage.Message
	if err := allowedMessage.Unpack(allowedResponse[28:]); err != nil || allowedMessage.RCode != dnsmessage.RCodeSuccess {
		t.Fatalf("DNS after lease reacquire = %v, %v", allowedMessage.RCode, err)
	}
	subsystem.Close()
	_ = guest.Close()
	<-result
}

type fixedResolver struct{}

func (fixedResolver) LookupCNAME(context.Context, string) (string, error) {
	return "canonical.example.", nil
}

func (fixedResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("192.168.1.10"), netip.MustParseAddr("1.1.1.1")}, nil
}

func TestVirtualDNSFiltersAnswersAndReturnsNoAAAA(t *testing.T) {
	subsystem := New()
	subsystem.resolver = fixedResolver{}
	name := dnsmessage.MustNewName("alias.example.")
	for _, queryType := range []dnsmessage.Type{dnsmessage.TypeA, dnsmessage.TypeCNAME, dnsmessage.TypeAAAA} {
		message := dnsmessage.Message{Header: dnsmessage.Header{ID: 9}, Questions: []dnsmessage.Question{{Name: name, Type: queryType, Class: dnsmessage.ClassINET}}}
		query, err := message.Pack()
		if err != nil {
			t.Fatal(err)
		}
		wire, err := subsystem.resolveDNS(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		var response dnsmessage.Message
		if err := response.Unpack(wire); err != nil {
			t.Fatal(err)
		}
		if queryType == dnsmessage.TypeAAAA {
			if len(response.Answers) != 0 {
				t.Fatalf("AAAA answers = %v", response.Answers)
			}
			continue
		}
		if queryType == dnsmessage.TypeCNAME {
			if len(response.Answers) != 1 {
				t.Fatalf("CNAME answers = %v", response.Answers)
			}
			canonical, ok := response.Answers[0].Body.(*dnsmessage.CNAMEResource)
			if !ok || canonical.CNAME.String() != "canonical.example." {
				t.Fatalf("CNAME answer = %v", response.Answers[0])
			}
			continue
		}
		if len(response.Answers) != 2 {
			t.Fatalf("A/CNAME answers = %v", response.Answers)
		}
		address, ok := response.Answers[1].Body.(*dnsmessage.AResource)
		if !ok || netip.AddrFrom4(address.A) != netip.MustParseAddr("1.1.1.1") {
			t.Fatalf("filtered A answer = %v", response.Answers[1])
		}
	}
}

func udpPacket(source, destination netip.Addr, sourcePort, destinationPort uint16, payload []byte) []byte {
	packet := make([]byte, 28+len(payload))
	packet[0] = 0x45
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	packet[8] = 64
	packet[9] = 17
	sourceBytes, destinationBytes := source.As4(), destination.As4()
	copy(packet[12:16], sourceBytes[:])
	copy(packet[16:20], destinationBytes[:])
	binary.BigEndian.PutUint16(packet[20:22], sourcePort)
	binary.BigEndian.PutUint16(packet[22:24], destinationPort)
	binary.BigEndian.PutUint16(packet[24:26], uint16(8+len(payload)))
	copy(packet[28:], payload)
	binary.BigEndian.PutUint16(packet[10:12], checksum(packet[:20]))
	return packet
}

func checksum(data []byte) uint16 {
	var sum uint32
	for index := 0; index < len(data); index += 2 {
		sum += uint32(binary.BigEndian.Uint16(data[index : index+2]))
	}
	for sum > 0xffff {
		sum = sum&0xffff + sum>>16
	}
	return ^uint16(sum)
}

func TestRepeatedSessionTeardown(t *testing.T) {
	subsystem := New()
	for range 1000 {
		guest, host := net.Pipe()
		result := make(chan error, 1)
		go func() { result <- subsystem.Serve(context.Background(), host, "boot") }()
		if err := netprotocol.WriteHandshake(guest, netprotocol.Handshake{BootID: "boot"}); err != nil {
			t.Fatal(err)
		}
		if _, err := netprotocol.ReadHandshake(guest, "boot"); err != nil {
			t.Fatal(err)
		}
		_ = guest.Close()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("session teardown timed out")
		}
	}
	if !subsystem.counters.empty() {
		t.Fatal("flow counters remained after reconnect loop")
	}
}

func TestInvalidFrameIsCounted(t *testing.T) {
	subsystem := New()
	guest, host := net.Pipe()
	result := make(chan error, 1)
	go func() { result <- subsystem.Serve(context.Background(), host, "boot") }()
	if err := netprotocol.WriteHandshake(guest, netprotocol.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	if _, err := netprotocol.ReadHandshake(guest, "boot"); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, netprotocol.HeaderSize)
	header[0] = 99
	if _, err := guest.Write(header); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, netprotocol.ErrInvalidFrame) {
		t.Fatalf("Serve() error = %v", err)
	}
	if subsystem.Metrics().InvalidFrames != 1 {
		t.Fatalf("invalid frame metric = %d", subsystem.Metrics().InvalidFrames)
	}
	_ = guest.Close()
}
