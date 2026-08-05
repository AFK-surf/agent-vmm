package egress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"golang.org/x/net/dns/dnsmessage"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

type dnsResolver interface {
	LookupCNAME(context.Context, string) (string, error)
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

func (subsystem *Subsystem) handleUDP(ctx context.Context, request *udp.ForwarderRequest, bootID string) {
	id := request.ID()
	destination, _ := addressFromTCPIP(id.LocalAddress)
	virtualDNS := destination.String() == netprotocol.HostAddress && id.LocalPort == 53
	environment, infrastructure, allowed := subsystem.authorize(bootID, id.RemoteAddress, id.LocalAddress, virtualDNS)
	if !allowed {
		subsystem.metrics.rejectedFlows.Add(1)
		discardUDP(request)
		return
	}
	kind := udpResource
	if virtualDNS {
		kind = dnsResource
	}
	if !subsystem.counters.acquire(kind, environment, infrastructure) {
		subsystem.metrics.rejectedFlows.Add(1)
		discardUDP(request)
		return
	}
	if kind == dnsResource {
		subsystem.metrics.activeDNS.Add(1)
	} else {
		subsystem.metrics.activeUDP.Add(1)
	}
	flowCtx := ctx
	flowCancel := func() {}
	if infrastructure && kind != dnsResource {
		flowCtx, flowCancel = subsystem.Leases.Context(ctx)
	}
	var queue waiter.Queue
	endpoint, err := request.CreateEndpoint(&queue)
	if err != nil {
		subsystem.counters.release(kind, environment, infrastructure)
		return
	}
	guest := gonet.NewUDPConn(&queue, endpoint)
	go func() {
		defer flowCancel()
		defer subsystem.counters.release(kind, environment, infrastructure)
		defer guest.Close()
		if kind == dnsResource {
			defer subsystem.metrics.activeDNS.Add(^uint64(0))
			subsystem.serveDNS(flowCtx, guest, infrastructure)
			return
		}
		defer subsystem.metrics.activeUDP.Add(^uint64(0))
		remote, dialErr := net.DialUDP("udp4", nil, net.UDPAddrFromAddrPort(netip.AddrPortFrom(destination, id.LocalPort)))
		if dialErr != nil {
			return
		}
		defer remote.Close()
		proxyUDP(flowCtx, guest, remote)
	}()
}

func discardUDP(request *udp.ForwarderRequest) {
	var queue waiter.Queue
	if endpoint, err := request.CreateEndpoint(&queue); err == nil {
		endpoint.Close()
	}
}

func proxyUDP(ctx context.Context, guest *gonet.UDPConn, remote *net.UDPConn) {
	associationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var activity atomic.Int64
	activity.Store(time.Now().UnixNano())
	done := make(chan struct{}, 2)
	copyDatagrams := func(destination net.Conn, source net.Conn) {
		defer func() { done <- struct{}{} }()
		buffer := make([]byte, 65535)
		for {
			_ = source.SetReadDeadline(time.Now().Add(time.Second))
			count, err := source.Read(buffer)
			if err != nil {
				if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() && time.Since(time.Unix(0, activity.Load())) < udpIdleTimeout {
					select {
					case <-associationCtx.Done():
						return
					default:
						continue
					}
				}
				return
			}
			if _, err := destination.Write(buffer[:count]); err != nil {
				return
			}
			activity.Store(time.Now().UnixNano())
		}
	}
	go copyDatagrams(remote, guest)
	go copyDatagrams(guest, remote)
	<-done
	cancel()
	_ = guest.SetDeadline(time.Now())
	_ = remote.SetDeadline(time.Now())
	<-done
}

func (subsystem *Subsystem) serveDNS(ctx context.Context, connection *gonet.UDPConn, infrastructure bool) {
	buffer := make([]byte, 4096)
	for {
		_ = connection.SetReadDeadline(time.Now().Add(dnsIdleTimeout))
		count, err := connection.Read(buffer)
		if err != nil {
			return
		}
		var response []byte
		if infrastructure && !subsystem.Leases.Active(time.Now()) {
			response, err = dnsFailureResponse(buffer[:count])
		} else {
			queryCtx, cancel := context.WithTimeout(ctx, dnsLookupTimeout)
			response, err = subsystem.resolveDNS(queryCtx, buffer[:count])
			cancel()
			if err != nil {
				response, err = dnsFailureResponse(buffer[:count])
			}
		}
		if err != nil {
			return
		}
		if _, err := connection.Write(response); err != nil {
			return
		}
	}
}

func dnsFailureResponse(query []byte) ([]byte, error) {
	var message dnsmessage.Message
	if err := message.Unpack(query); err != nil || message.Response || len(message.Questions) != 1 {
		return nil, errors.New("invalid DNS query")
	}
	message.Response = true
	message.RecursionAvailable = true
	message.RCode = dnsmessage.RCodeServerFailure
	message.Answers = nil
	message.Authorities = nil
	message.Additionals = nil
	return message.Pack()
}

func (subsystem *Subsystem) resolveDNS(ctx context.Context, query []byte) ([]byte, error) {
	var parser dnsmessage.Parser
	header, err := parser.Start(query)
	if err != nil || header.Response {
		return nil, errors.New("invalid DNS query")
	}
	question, err := parser.Question()
	if err != nil {
		return nil, err
	}
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: header.ID, Response: true, RecursionAvailable: true})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	if err := builder.Question(question); err != nil {
		return nil, err
	}
	if err := builder.StartAnswers(); err != nil {
		return nil, err
	}
	if question.Type == dnsmessage.TypeA {
		addresses, lookupErr := subsystem.resolver.LookupNetIP(ctx, "ip4", question.Name.String())
		if lookupErr != nil {
			return nil, lookupErr
		}
		canonical, canonicalErr := subsystem.resolver.LookupCNAME(ctx, question.Name.String())
		answerName := question.Name
		if canonicalErr == nil && canonical != question.Name.String() {
			canonicalName, nameErr := dnsmessage.NewName(canonical)
			if nameErr != nil {
				return nil, nameErr
			}
			if err := builder.CNAMEResource(dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeCNAME, Class: dnsmessage.ClassINET, TTL: 30}, dnsmessage.CNAMEResource{CNAME: canonicalName}); err != nil {
				return nil, err
			}
			answerName = canonicalName
		}
		for _, address := range addresses {
			address = address.Unmap()
			if !DestinationAllowed(address, nil, true) {
				continue
			}
			if err := builder.AResource(dnsmessage.ResourceHeader{Name: answerName, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 30}, dnsmessage.AResource{A: address.As4()}); err != nil {
				return nil, err
			}
		}
	} else if question.Type == dnsmessage.TypeCNAME {
		canonical, lookupErr := subsystem.resolver.LookupCNAME(ctx, question.Name.String())
		if lookupErr != nil {
			return nil, lookupErr
		}
		if canonical != question.Name.String() {
			canonicalName, nameErr := dnsmessage.NewName(canonical)
			if nameErr != nil {
				return nil, nameErr
			}
			if err := builder.CNAMEResource(dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeCNAME, Class: dnsmessage.ClassINET, TTL: 30}, dnsmessage.CNAMEResource{CNAME: canonicalName}); err != nil {
				return nil, err
			}
		}
	}
	return builder.Finish()
}
