package egress

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

func (subsystem *Subsystem) handleTCP(ctx context.Context, request *tcp.ForwarderRequest, bootID string) {
	id := request.ID()
	destination, _ := addressFromTCPIP(id.LocalAddress)
	virtualDNS := destination.String() == netprotocol.HostAddress && id.LocalPort == 53
	environment, infrastructure, allowed := subsystem.authorize(bootID, id.RemoteAddress, id.LocalAddress, virtualDNS)
	kind := tcpResource
	if virtualDNS {
		kind = dnsResource
	}
	if !allowed || !subsystem.counters.acquire(kind, environment, infrastructure) {
		subsystem.metrics.rejectedFlows.Add(1)
		request.Complete(true)
		return
	}
	if virtualDNS {
		subsystem.metrics.activeDNS.Add(1)
	} else {
		subsystem.metrics.activeTCP.Add(1)
	}
	flowCtx := ctx
	flowCancel := func() {}
	if infrastructure && kind != dnsResource {
		flowCtx, flowCancel = subsystem.Leases.Context(ctx)
	}
	go func() {
		defer flowCancel()
		defer subsystem.counters.release(kind, environment, infrastructure)
		if virtualDNS {
			defer subsystem.metrics.activeDNS.Add(^uint64(0))
			var queue waiter.Queue
			endpoint, tcpErr := request.CreateEndpoint(&queue)
			if tcpErr != nil {
				request.Complete(true)
				return
			}
			request.Complete(false)
			connection := gonet.NewTCPConn(&queue, endpoint)
			defer connection.Close()
			subsystem.serveDNSTCP(flowCtx, connection, infrastructure)
			return
		}
		defer subsystem.metrics.activeTCP.Add(^uint64(0))
		host, err := subsystem.dialer.DialContext(flowCtx, "tcp4", net.JoinHostPort(destination.String(), fmt.Sprint(id.LocalPort)))
		if err != nil {
			request.Complete(true)
			return
		}
		var queue waiter.Queue
		endpoint, tcpErr := request.CreateEndpoint(&queue)
		if tcpErr != nil {
			request.Complete(true)
			_ = host.Close()
			return
		}
		request.Complete(false)
		proxyTCP(flowCtx, gonet.NewTCPConn(&queue, endpoint), host)
	}()
}

func (subsystem *Subsystem) serveDNSTCP(ctx context.Context, connection *gonet.TCPConn, infrastructure bool) {
	var size [2]byte
	for {
		_ = connection.SetReadDeadline(time.Now().Add(dnsIdleTimeout))
		if _, err := io.ReadFull(connection, size[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint16(size[:])
		if length == 0 || length > 4096 {
			return
		}
		query := make([]byte, length)
		if _, err := io.ReadFull(connection, query); err != nil {
			return
		}
		var response []byte
		var err error
		if infrastructure && !subsystem.Leases.Active(time.Now()) {
			response, err = dnsFailureResponse(query)
		} else {
			queryCtx, cancel := context.WithTimeout(ctx, dnsLookupTimeout)
			response, err = subsystem.resolveDNS(queryCtx, query)
			cancel()
			if err != nil {
				response, err = dnsFailureResponse(query)
			}
		}
		if err != nil || len(response) > 65535 {
			return
		}
		binary.BigEndian.PutUint16(size[:], uint16(len(response)))
		if _, err := connection.Write(append(size[:], response...)); err != nil {
			return
		}
	}
}

func proxyTCP(ctx context.Context, guest *gonet.TCPConn, host net.Conn) {
	defer guest.Close()
	defer host.Close()
	doneContext := make(chan struct{})
	defer close(doneContext)
	go func() {
		select {
		case <-ctx.Done():
			_ = guest.SetDeadline(time.Now())
			_ = host.SetDeadline(time.Now())
		case <-doneContext:
		}
	}()
	activity := &idlePair{guest: guest, host: host, timeout: tcpIdleTimeout}
	activity.touch()
	done := make(chan struct{}, 2)
	go func() {
		copyWithActivity(host, guest, activity)
		if connection, ok := host.(*net.TCPConn); ok {
			_ = connection.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		copyWithActivity(guest, host, activity)
		_ = guest.CloseWrite()
		done <- struct{}{}
	}()
	<-done
	<-done
}

type idlePair struct {
	mu      sync.Mutex
	guest   net.Conn
	host    net.Conn
	timeout time.Duration
}

func (pair *idlePair) touch() {
	pair.mu.Lock()
	defer pair.mu.Unlock()
	deadline := time.Now().Add(pair.timeout)
	_ = pair.guest.SetDeadline(deadline)
	_ = pair.host.SetDeadline(deadline)
}

func copyWithActivity(destination, source net.Conn, activity *idlePair) {
	buffer := make([]byte, 32<<10)
	for {
		count, err := source.Read(buffer)
		if count > 0 {
			activity.touch()
			if _, writeErr := destination.Write(buffer[:count]); writeErr != nil {
				return
			}
			activity.touch()
		}
		if err != nil {
			return
		}
	}
}
