package egress

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"golang.org/x/net/dns/dnsmessage"
)

func TestDuplicateSessionRejected(t *testing.T) {
	subsystem := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guest, host := net.Pipe()
	result := make(chan error, 1)
	go func() { result <- subsystem.Serve(ctx, host, "boot") }()
	if err := egresslink.WriteHandshake(guest, egresslink.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	if _, err := egresslink.ReadHandshake(guest, "boot"); err != nil {
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
	if err := egresslink.WriteHandshake(guest, egresslink.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	if _, err := egresslink.ReadHandshake(guest, "boot"); err != nil {
		t.Fatal(err)
	}
	name := dnsmessage.MustNewName("localhost.")
	query := dnsmessage.Message{Header: dnsmessage.Header{ID: 7, RecursionDesired: true}, Questions: []dnsmessage.Question{{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}
	payload, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	packet := udpPacket(netip.MustParseAddr(egresslink.GuestAddress), netip.MustParseAddr(egresslink.HostAddress), 32000, 53, payload)
	if err := egresslink.WriteFrame(guest, egresslink.Data, packet); err != nil {
		t.Fatal(err)
	}
	_ = guest.SetReadDeadline(time.Now().Add(3 * time.Second))
	frameType, response, err := egresslink.ReadFrame(guest)
	if err != nil {
		t.Fatalf("DNS response: %v; metrics: %+v", err, subsystem.Metrics())
	}
	if frameType != egresslink.Data || len(response) < 28 || netip.AddrFrom4([4]byte(response[12:16])) != netip.MustParseAddr(egresslink.HostAddress) {
		t.Fatalf("DNS response frame = %v %x", frameType, response)
	}
	lease.Release()
	if err := egresslink.WriteFrame(guest, egresslink.Data, packet); err != nil {
		t.Fatal(err)
	}
	_, deniedResponse, err := egresslink.ReadFrame(guest)
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
	if err := egresslink.WriteFrame(guest, egresslink.Data, packet); err != nil {
		t.Fatal(err)
	}
	_, allowedResponse, err := egresslink.ReadFrame(guest)
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
	return []netip.Addr{netip.MustParseAddr("192.168.1.10"), netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("169.254.169.254"), netip.MustParseAddr("10.64.2.8")}, nil
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
		if err := egresslink.WriteHandshake(guest, egresslink.Handshake{BootID: "boot"}); err != nil {
			t.Fatal(err)
		}
		if _, err := egresslink.ReadHandshake(guest, "boot"); err != nil {
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
	if err := egresslink.WriteHandshake(guest, egresslink.Handshake{BootID: "boot"}); err != nil {
		t.Fatal(err)
	}
	if _, err := egresslink.ReadHandshake(guest, "boot"); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, egresslink.HeaderSize)
	header[0] = 99
	if _, err := guest.Write(header); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, egresslink.ErrInvalidFrame) {
		t.Fatalf("Serve() error = %v", err)
	}
	if subsystem.Metrics().InvalidFrames != 1 {
		t.Fatalf("invalid frame metric = %d", subsystem.Metrics().InvalidFrames)
	}
	_ = guest.Close()
}

func TestVirtualDNSUsesInstallationLANGrant(t *testing.T) {
	subsystem := New()
	subsystem.resolver = fixedResolver{}
	source := netip.MustParseAddr("10.64.1.8")
	if err := subsystem.Policies.Publish(Snapshot{BootID: "boot", Revision: 1, Sources: []Source{{EnvironmentID: "appliance:a", NamespaceID: "system.a", Subnet: netip.MustParsePrefix("10.64.1.0/24"), LANEgress: true}}}); err != nil {
		t.Fatal(err)
	}
	message := dnsmessage.Message{Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName("alias.example."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}
	query, err := message.Pack()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := subsystem.resolveSourceDNS(context.Background(), query, "boot", source)
	if err != nil {
		t.Fatal(err)
	}
	var response dnsmessage.Message
	if err := response.Unpack(wire); err != nil {
		t.Fatal(err)
	}
	var answers []netip.Addr
	for _, resource := range response.Answers {
		if a, ok := resource.Body.(*dnsmessage.AResource); ok {
			answers = append(answers, netip.AddrFrom4(a.A))
		}
	}
	if len(answers) != 1 || answers[0] != netip.MustParseAddr("192.168.1.10") {
		t.Fatalf("LAN-only DNS answers = %v", answers)
	}
	subsystem.Policies.RevokeEnvironment("appliance:a")
	wire, err = subsystem.resolveSourceDNS(context.Background(), query, "boot", source)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Unpack(wire); err != nil {
		t.Fatal(err)
	}
	for _, resource := range response.Answers {
		if _, ok := resource.Body.(*dnsmessage.AResource); ok {
			t.Fatal("DNS returned an address after LAN grant revocation")
		}
	}
}

func TestUDPRevocationClosesContinuousBidirectionalTraffic(t *testing.T) {
	policies := new(Policies)
	source := Source{EnvironmentID: "appliance:a", NamespaceID: "system.a", Subnet: netip.MustParsePrefix("10.64.1.0/24"), LANEgress: true}
	if err := policies.Publish(Snapshot{BootID: "boot", Revision: 1, Sources: []Source{source}}); err != nil {
		t.Fatal(err)
	}
	flow, cancel := policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), source)
	defer cancel()
	var connections [2]*net.UDPConn
	var peers [2]*net.UDPConn
	var received [2]chan struct{}
	var workers []chan struct{}
	for index := range connections {
		peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
		connection, err := net.DialUDP("udp4", nil, peer.LocalAddr().(*net.UDPAddr))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		peers[index] = peer
		connections[index] = connection
		received[index] = make(chan struct{}, 1)
		signal := received[index]
		writerDone := make(chan struct{})
		readerDone := make(chan struct{})
		workers = append(workers, writerDone, readerDone)
		go func() {
			defer close(writerDone)
			for {
				if _, err := peer.WriteToUDP([]byte("traffic"), connection.LocalAddr().(*net.UDPAddr)); err != nil {
					return
				}
			}
		}()
		go func() {
			defer close(readerDone)
			buffer := make([]byte, 64)
			for {
				if _, _, err := peer.ReadFromUDP(buffer); err != nil {
					return
				}
				select {
				case signal <- struct{}{}:
				default:
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { proxyUDP(flow, connections[0], connections[1]); close(done) }()
	for _, signal := range received {
		select {
		case <-signal:
		case <-time.After(time.Second):
			t.Fatal("bidirectional UDP traffic did not start")
		}
	}
	policies.RevokeEnvironment(source.EnvironmentID)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("continuous UDP traffic survived revocation")
	}
	for _, connection := range connections {
		if _, err := connection.Write([]byte("after-revoke")); err == nil {
			t.Fatal("revoked UDP endpoint remained open")
		}
	}
	t.Cleanup(func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
		for _, worker := range workers {
			select {
			case <-worker:
			case <-time.After(time.Second):
				t.Error("UDP peer traffic worker did not stop")
			}
		}
	})
}
