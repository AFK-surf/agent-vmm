//go:build linux

package network

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"golang.org/x/net/dns/dnsmessage"
)

type dnsView struct{ state }

func (m *Manager) publishDNSLocked() { m.dnsView.Store(&dnsView{state: m.stateLocked()}) }

type dnsServer struct {
	mu          sync.Mutex
	closed      bool
	connections map[net.Conn]struct{}
	listeners   sync.WaitGroup
	queries     sync.WaitGroup
	logger      *slog.Logger
	udp         *net.UDPConn
	tcp         net.Listener
}

func (server *dnsServer) close(namespaceID string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.closed {
		return
	}
	server.closed = true
	for connection := range server.connections {
		connection.Close()
	}
	gateway := server.udp.LocalAddr()
	udpErr := server.udp.Close()
	tcpErr := server.tcp.Close()
	server.logger.Info("DNS listener closed", "namespace_id", namespaceID, "gateway", gateway.String(), "error", errors.Join(udpErr, tcpErr))
}

func (m *Manager) startDNSLocked(namespaceID string) error {
	if m.dns[namespaceID] != nil {
		return nil
	}
	address := m.namespaces[namespaceID].Gateway.String()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP(address), Port: 53})
	if err != nil {
		m.logger.Warn("DNS UDP listener failed", "namespace_id", namespaceID, "gateway", address, "error", err)
		return err
	}
	tcp, err := net.Listen("tcp4", net.JoinHostPort(address, "53"))
	if err != nil {
		m.logger.Warn("DNS TCP listener failed", "namespace_id", namespaceID, "gateway", address, "error", err)
		udp.Close()
		return err
	}
	server := &dnsServer{connections: make(map[net.Conn]struct{}), logger: m.logger, udp: udp, tcp: tcp}
	m.dns[namespaceID] = server
	server.listeners.Add(2)
	m.logger.Info("DNS listener started", "namespace_id", namespaceID, "gateway", address, "udp", udp.LocalAddr().String(), "tcp", tcp.Addr().String())
	go m.serveDNSUDP(namespaceID, server)
	go m.serveDNSTCP(namespaceID, server)
	return nil
}

func (m *Manager) serveDNSUDP(namespaceID string, server *dnsServer) {
	defer m.retireDNS(namespaceID, server)
	defer server.listeners.Done()
	buffer := make([]byte, 64<<10)
	for {
		count, peer, err := server.udp.ReadFromUDP(buffer)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				m.logger.Warn("DNS UDP read ended", "namespace_id", namespaceID, "gateway", server.udp.LocalAddr().String(), "peer", peer, "error", err)
			}
			return
		}
		query := append([]byte(nil), buffer[:count]...)
		server.queries.Add(1)
		go func() {
			defer server.queries.Done()
			response, err := m.answerDNS(namespaceID, query)
			if err != nil {
				m.logger.Warn("DNS UDP answer failed", "namespace_id", namespaceID, "gateway", server.udp.LocalAddr().String(), "peer", peer.String(), "error", err)
				return
			}
			if _, err := server.udp.WriteToUDP(response, peer); err != nil {
				m.logger.Warn("DNS UDP send failed", "namespace_id", namespaceID, "gateway", server.udp.LocalAddr().String(), "peer", peer.String(), "error", err)
			}
		}()
	}
}

func (m *Manager) serveDNSTCP(namespaceID string, server *dnsServer) {
	defer m.retireDNS(namespaceID, server)
	defer server.listeners.Done()
	for {
		connection, err := server.tcp.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				m.logger.Warn("DNS TCP accept ended", "namespace_id", namespaceID, "gateway", server.tcp.Addr().String(), "error", err)
			}
			return
		}
		server.mu.Lock()
		if server.closed {
			server.mu.Unlock()
			connection.Close()
			return
		}
		server.connections[connection] = struct{}{}
		server.queries.Add(1)
		server.mu.Unlock()
		go func() {
			defer server.queries.Done()
			defer func() {
				connection.Close()
				server.mu.Lock()
				delete(server.connections, connection)
				server.mu.Unlock()
			}()
			if err := connection.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
				m.logger.Warn("DNS TCP deadline failed", "namespace_id", namespaceID, "gateway", connection.LocalAddr().String(), "peer", connection.RemoteAddr().String(), "error", err)
			}
			var size [2]byte
			if _, err := io.ReadFull(connection, size[:]); err != nil {
				m.logger.Warn("DNS TCP header read failed", "namespace_id", namespaceID, "gateway", connection.LocalAddr().String(), "peer", connection.RemoteAddr().String(), "error", err)
				return
			}
			query := make([]byte, binary.BigEndian.Uint16(size[:]))
			if _, err := io.ReadFull(connection, query); err != nil {
				m.logger.Warn("DNS TCP query read failed", "namespace_id", namespaceID, "gateway", connection.LocalAddr().String(), "peer", connection.RemoteAddr().String(), "error", err)
				return
			}
			response, err := m.answerDNS(namespaceID, query)
			if err != nil {
				m.logger.Warn("DNS TCP answer failed", "namespace_id", namespaceID, "gateway", connection.LocalAddr().String(), "peer", connection.RemoteAddr().String(), "error", err)
				return
			}
			if len(response) > 65535 {
				m.logger.Warn("DNS TCP response exceeds frame size", "namespace_id", namespaceID, "gateway", connection.LocalAddr().String(), "peer", connection.RemoteAddr().String(), "response_bytes", len(response), "error", "DNS response exceeds TCP frame size")
				return
			}
			binary.BigEndian.PutUint16(size[:], uint16(len(response)))
			if _, err := connection.Write(append(size[:], response...)); err != nil {
				m.logger.Warn("DNS TCP send failed", "namespace_id", namespaceID, "gateway", connection.LocalAddr().String(), "peer", connection.RemoteAddr().String(), "error", err)
			}
		}()
	}
}

func (m *Manager) retireDNS(namespaceID string, server *dnsServer) {
	m.mu.Lock()
	if m.dns[namespaceID] != server {
		m.mu.Unlock()
		return
	}
	delete(m.dns, namespaceID)
	server.close(namespaceID)
	m.mu.Unlock()
	// Close can return before an in-flight socket operation releases its fd.
	server.listeners.Wait()
	server.queries.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed && m.dns[namespaceID] == nil && len(m.endpoints[namespaceID]) != 0 {
		if err := m.startDNSLocked(namespaceID); err != nil {
			m.lastError = err
		}
	}
}

func (m *Manager) answerDNS(namespaceID string, query []byte) ([]byte, error) {
	var message dnsmessage.Message
	if err := message.Unpack(query); err != nil || len(message.Questions) != 1 {
		return nil, errors.New("invalid DNS query")
	}
	view := m.dnsView.Load()
	question := message.Questions[0]
	name := strings.ToLower(question.Name.String())
	if strings.HasSuffix(name, ".agent.") {
		addresses := view.localDNSAddresses(namespaceID, name)
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
	public := view.namespaces[namespaceID].EgressMode == vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET || view.namespaces[namespaceID].LANEgress
	if !public {
		message.Response = true
		message.RCode = dnsmessage.RCodeRefused
		message.Answers = nil
		message.Authorities = nil
		message.Additionals = nil
		return message.Pack()
	}
	source := view.namespaces[namespaceID].Gateway
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

func (m *dnsView) localDNSAddresses(namespaceID, name string) []netip.Addr {
	suffix := "." + namespaceID + ".agent."
	serviceName := strings.HasSuffix(name, ".services.agent.")
	if serviceName {
		suffix = ".services.agent."
	} else if !strings.HasSuffix(name, suffix) {
		return nil
	}
	label := strings.TrimSuffix(name, suffix)
	if !serviceName {
		for id, endpoint := range m.endpoints[namespaceID] {
			if strings.EqualFold(id, label) {
				return []netip.Addr{endpoint.Address}
			}
		}
		for _, link := range m.links {
			if link.GetSourceNamespaceId() == namespaceID && link.GetDnsAlias() == label {
				endpoints := selectEndpoints(m.endpoints[link.GetDestinationNamespaceId()], link.GetDestination())
				addresses := make([]netip.Addr, 0, len(endpoints))
				for _, endpoint := range endpoints {
					addresses = append(addresses, endpoint.Address)
				}
				return addresses
			}
		}
	}
	for _, service := range m.services {
		if service.GetSourceNamespaceId() == namespaceID && service.GetDnsAlias() == label && service.GetExpiresUnixMillis() > time.Now().UnixMilli() {
			if service.GetRemote() {
				address, err := netip.ParseAddr(service.GetVirtualAddress())
				if err == nil && remoteServicePrefix.Contains(address) {
					return []netip.Addr{address}
				}
				return nil
			}
			endpoint := m.endpoints[service.GetDestinationNamespaceId()][service.GetDestinationContainerId()]
			if endpoint.InstanceID == service.GetDestinationContainerInstanceId() {
				return []netip.Addr{endpoint.Address}
			}
			return nil
		}
	}
	return nil
}
