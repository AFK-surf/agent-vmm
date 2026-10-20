//go:build linux

package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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

func (server *dnsServer) close(namespaceID string) {
	gateway := server.udp.LocalAddr()
	udpErr := server.udp.Close()
	tcpErr := server.tcp.Close()
	log.Printf("Guest DNS listener: phase=close namespace=%q gateway=%q error=%q", namespaceID, gateway.String(), fmt.Sprint(errors.Join(udpErr, tcpErr)))
}

func (m *Manager) startDNSLocked(namespaceID string) error {
	if m.dns[namespaceID] != nil {
		return nil
	}
	address := m.namespaces[namespaceID].Gateway.String()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP(address), Port: 53})
	if err != nil {
		log.Printf("Guest DNS listener: phase=udp_listen namespace=%q gateway=%q error=%q", namespaceID, address, err)
		return err
	}
	tcp, err := net.Listen("tcp4", net.JoinHostPort(address, "53"))
	if err != nil {
		log.Printf("Guest DNS listener: phase=tcp_listen namespace=%q gateway=%q error=%q", namespaceID, address, err)
		udp.Close()
		return err
	}
	server := &dnsServer{udp: udp, tcp: tcp}
	m.dns[namespaceID] = server
	log.Printf("Guest DNS listener: phase=start namespace=%q gateway=%q udp=%q tcp=%q", namespaceID, address, udp.LocalAddr().String(), tcp.Addr().String())
	go m.serveDNSUDP(namespaceID, server)
	go m.serveDNSTCP(namespaceID, server)
	return nil
}

func (m *Manager) serveDNSUDP(namespaceID string, server *dnsServer) {
	defer m.retireDNS(namespaceID, server)
	buffer := make([]byte, 64<<10)
	for {
		count, peer, err := server.udp.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Guest DNS listener: phase=udp_read namespace=%q gateway=%q peer=%v closed=%t error=%q", namespaceID, server.udp.LocalAddr().String(), peer, errors.Is(err, net.ErrClosed), err)
			return
		}
		query := append([]byte(nil), buffer[:count]...)
		go func() {
			response, err := m.answerDNS(namespaceID, query)
			if err != nil {
				log.Printf("Guest DNS request: phase=udp_answer namespace=%q gateway=%q peer=%q error=%q", namespaceID, server.udp.LocalAddr().String(), peer.String(), err)
				return
			}
			if _, err := server.udp.WriteToUDP(response, peer); err != nil {
				log.Printf("Guest DNS request: phase=udp_send namespace=%q gateway=%q peer=%q error=%q", namespaceID, server.udp.LocalAddr().String(), peer.String(), err)
			}
		}()
	}
}

func (m *Manager) serveDNSTCP(namespaceID string, server *dnsServer) {
	defer m.retireDNS(namespaceID, server)
	for {
		connection, err := server.tcp.Accept()
		if err != nil {
			log.Printf("Guest DNS listener: phase=tcp_accept namespace=%q gateway=%q closed=%t error=%q", namespaceID, server.tcp.Addr().String(), errors.Is(err, net.ErrClosed), err)
			return
		}
		go func() {
			defer connection.Close()
			if err := connection.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
				log.Printf("Guest DNS request: phase=tcp_deadline namespace=%q gateway=%q peer=%q error=%q", namespaceID, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
			}
			var size [2]byte
			if _, err := io.ReadFull(connection, size[:]); err != nil {
				log.Printf("Guest DNS request: phase=tcp_read_header namespace=%q gateway=%q peer=%q error=%q", namespaceID, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
				return
			}
			query := make([]byte, binary.BigEndian.Uint16(size[:]))
			if _, err := io.ReadFull(connection, query); err != nil {
				log.Printf("Guest DNS request: phase=tcp_read_query namespace=%q gateway=%q peer=%q error=%q", namespaceID, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
				return
			}
			response, err := m.answerDNS(namespaceID, query)
			if err != nil {
				log.Printf("Guest DNS request: phase=tcp_answer namespace=%q gateway=%q peer=%q error=%q", namespaceID, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
				return
			}
			if len(response) > 65535 {
				log.Printf("Guest DNS request: phase=tcp_response_size namespace=%q gateway=%q peer=%q response_bytes=%d error=%q", namespaceID, connection.LocalAddr().String(), connection.RemoteAddr().String(), len(response), "DNS response exceeds TCP frame size")
				return
			}
			binary.BigEndian.PutUint16(size[:], uint16(len(response)))
			if _, err := connection.Write(append(size[:], response...)); err != nil {
				log.Printf("Guest DNS request: phase=tcp_send namespace=%q gateway=%q peer=%q error=%q", namespaceID, connection.LocalAddr().String(), connection.RemoteAddr().String(), err)
			}
		}()
	}
}

func (m *Manager) retireDNS(namespaceID string, server *dnsServer) {
	m.mu.Lock()
	current := m.dns[namespaceID] == server
	if current {
		delete(m.dns, namespaceID)
		server.close(namespaceID)
	}
	m.mu.Unlock()
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
	public := m.namespaces[namespaceID].EgressMode == vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET || m.namespaces[namespaceID].LANEgress
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
				endpoints := m.selectedEndpoints(link.GetDestinationNamespaceId(), link.GetDestination())
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
