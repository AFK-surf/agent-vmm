//go:build linux

package network

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"golang.org/x/net/dns/dnsmessage"
)

type dnsServer struct {
	udp *net.UDPConn
	tcp net.Listener
}

func (server *dnsServer) close() {
	_ = server.udp.Close()
	_ = server.tcp.Close()
}

func (m *Manager) startDNSLocked(namespaceID string) error {
	if m.dns[namespaceID] != nil {
		return nil
	}
	address := m.namespaces[namespaceID].Gateway.String()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP(address), Port: 53})
	if err != nil {
		return err
	}
	tcp, err := net.Listen("tcp4", net.JoinHostPort(address, "53"))
	if err != nil {
		udp.Close()
		return err
	}
	server := &dnsServer{udp: udp, tcp: tcp}
	m.dns[namespaceID] = server
	go m.serveDNSUDP(namespaceID, server)
	go m.serveDNSTCP(namespaceID, server)
	return nil
}

func (m *Manager) serveDNSUDP(namespaceID string, server *dnsServer) {
	buffer := make([]byte, 64<<10)
	for {
		count, peer, err := server.udp.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		query := append([]byte(nil), buffer[:count]...)
		go func() {
			response, err := m.answerDNS(namespaceID, query)
			if err == nil {
				_, _ = server.udp.WriteToUDP(response, peer)
			}
		}()
	}
}

func (m *Manager) serveDNSTCP(namespaceID string, server *dnsServer) {
	for {
		connection, err := server.tcp.Accept()
		if err != nil {
			return
		}
		go func() {
			defer connection.Close()
			_ = connection.SetDeadline(time.Now().Add(30 * time.Second))
			var size [2]byte
			if _, err := io.ReadFull(connection, size[:]); err != nil {
				return
			}
			query := make([]byte, binary.BigEndian.Uint16(size[:]))
			if _, err := io.ReadFull(connection, query); err != nil {
				return
			}
			response, err := m.answerDNS(namespaceID, query)
			if err != nil || len(response) > 65535 {
				return
			}
			binary.BigEndian.PutUint16(size[:], uint16(len(response)))
			_, _ = connection.Write(append(size[:], response...))
		}()
	}
}

func (m *Manager) answerDNS(namespaceID string, query []byte) ([]byte, error) {
	var message dnsmessage.Message
	if err := message.Unpack(query); err != nil || len(message.Questions) != 1 {
		return nil, errors.New("invalid DNS query")
	}
	question := message.Questions[0]
	name := strings.ToLower(question.Name.String())
	if strings.HasSuffix(name, ".agent.") {
		addresses := m.localDNSAddresses(namespaceID, name)
		response := dnsmessage.Message{Header: dnsmessage.Header{ID: message.ID, Response: true, Authoritative: true, RecursionDesired: message.RecursionDesired}, Questions: message.Questions}
		if len(addresses) == 0 {
			response.RCode = dnsmessage.RCodeNameError
		}
		if question.Type == dnsmessage.TypeA {
			for _, address := range addresses {
				response.Answers = append(response.Answers, dnsmessage.Resource{
					Header: dnsmessage.ResourceHeader{Name: question.Name, Class: dnsmessage.ClassINET, TTL: 1},
					Body:   &dnsmessage.AResource{A: address.As4()},
				})
			}
		}
		return response.Pack()
	}
	m.mu.RLock()
	public := m.namespaces[namespaceID].EgressMode == vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
	m.mu.RUnlock()
	if !public {
		message.Response = true
		message.RCode = dnsmessage.RCodeRefused
		message.Answers = nil
		message.Authorities = nil
		message.Additionals = nil
		return message.Pack()
	}
	m.mu.RLock()
	source := m.namespaces[namespaceID].Gateway
	m.mu.RUnlock()
	dialer := net.Dialer{Timeout: 3 * time.Second, LocalAddr: &net.UDPAddr{IP: net.IP(source.AsSlice())}}
	upstream, err := dialer.Dial("udp4", publicResolver)
	if err != nil {
		return nil, err
	}
	defer upstream.Close()
	_ = upstream.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := upstream.Write(query); err != nil {
		return nil, err
	}
	buffer := make([]byte, 64<<10)
	count, err := upstream.Read(buffer)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), buffer[:count]...), nil
}

func (m *Manager) localDNSAddresses(namespaceID, name string) []netip.Addr {
	m.mu.RLock()
	defer m.mu.RUnlock()
	suffix := "." + namespaceID + ".agent."
	if !strings.HasSuffix(name, suffix) {
		return nil
	}
	label := strings.TrimSuffix(name, suffix)
	for id, endpoint := range m.endpoints[namespaceID] {
		if strings.EqualFold(id, label) {
			return []netip.Addr{endpoint.Address}
		}
	}
	for _, link := range m.links {
		if link.GetSourceNamespaceId() == namespaceID && link.GetDnsAlias() == label {
			endpoints := m.selectedEndpoints(link.GetDestinationNamespaceId(), link.GetDestination())
			addresses := make([]netip.Addr, 0, len(endpoints))
			for _, endpoint := range endpoints {
				addresses = append(addresses, endpoint.Address)
			}
			return addresses
		}
	}
	return nil
}
