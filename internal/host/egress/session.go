package egress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

const (
	tcpDialTimeout   = 10 * time.Second
	tcpIdleTimeout   = 5 * time.Minute
	udpIdleTimeout   = 2 * time.Minute
	dnsIdleTimeout   = 10 * time.Second
	dnsLookupTimeout = 4 * time.Second
)

type Metrics struct {
	PacketsReceived uint64
	PacketsSent     uint64
	PacketDrops     uint64
	InvalidFrames   uint64
	RejectedFlows   uint64
	ActiveTCP       uint64
	ActiveUDP       uint64
	ActiveDNS       uint64
}

type metrics struct {
	packetsReceived atomic.Uint64
	packetsSent     atomic.Uint64
	packetDrops     atomic.Uint64
	invalidFrames   atomic.Uint64
	rejectedFlows   atomic.Uint64
	activeTCP       atomic.Uint64
	activeUDP       atomic.Uint64
	activeDNS       atomic.Uint64
}

type Subsystem struct {
	Policies Policies
	Leases   Leases

	mu         sync.Mutex
	session    context.CancelFunc
	connection net.Conn
	bootID     string
	counters   counters
	metrics    metrics
	dialer     net.Dialer
	resolver   dnsResolver
	services   serviceRoutes
}

func New() *Subsystem {
	return &Subsystem{counters: counters{environment: make(map[string][3]int)}, dialer: net.Dialer{Timeout: tcpDialTimeout}, resolver: net.DefaultResolver,
		services: serviceRoutes{values: make(map[serviceRouteKey]ServiceRoute)}}
}

func (subsystem *Subsystem) Serve(ctx context.Context, connection net.Conn, bootID string) error {
	if connection == nil || bootID == "" {
		return errors.New("connection and boot ID are required")
	}
	if err := ctx.Err(); err != nil {
		_ = connection.Close()
		return err
	}
	_ = connection.SetDeadline(time.Now().Add(tcpDialTimeout))
	subsystem.mu.Lock()
	if subsystem.session != nil {
		subsystem.mu.Unlock()
		_ = connection.Close()
		return errors.New("egress session already active")
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	subsystem.session = cancel
	subsystem.connection = connection
	subsystem.bootID = bootID
	subsystem.mu.Unlock()
	defer func() {
		cancel()
		_ = connection.Close()
		subsystem.mu.Lock()
		subsystem.session = nil
		subsystem.connection = nil
		subsystem.bootID = ""
		subsystem.mu.Unlock()
	}()
	if _, err := netprotocol.ReadHandshake(connection, bootID); err != nil {
		return err
	}
	if err := netprotocol.WriteHandshake(connection, netprotocol.Handshake{BootID: bootID}); err != nil {
		return err
	}
	_ = connection.SetDeadline(time.Time{})
	return subsystem.runStack(sessionCtx, connection, bootID)
}

func (subsystem *Subsystem) Close() {
	subsystem.mu.Lock()
	if subsystem.session != nil {
		subsystem.session()
	}
	if subsystem.connection != nil {
		_ = subsystem.connection.Close()
	}
	subsystem.mu.Unlock()
	subsystem.Policies.Clear()
	subsystem.Leases.Clear()
}

func (subsystem *Subsystem) Metrics() Metrics {
	return Metrics{
		PacketsReceived: subsystem.metrics.packetsReceived.Load(), PacketsSent: subsystem.metrics.packetsSent.Load(),
		PacketDrops: subsystem.metrics.packetDrops.Load(), InvalidFrames: subsystem.metrics.invalidFrames.Load(),
		RejectedFlows: subsystem.metrics.rejectedFlows.Load(), ActiveTCP: subsystem.metrics.activeTCP.Load(),
		ActiveUDP: subsystem.metrics.activeUDP.Load(), ActiveDNS: subsystem.metrics.activeDNS.Load(),
	}
}

func (subsystem *Subsystem) Status() (active bool, bootID string, policyRevision uint64, sourceCount int) {
	subsystem.mu.Lock()
	active, bootID = subsystem.session != nil, subsystem.bootID
	subsystem.mu.Unlock()
	if state := subsystem.Policies.state.Load(); state != nil && state.bootID == bootID {
		policyRevision = state.revision
		sourceCount = len(state.sources)
	}
	return
}

func (subsystem *Subsystem) runStack(ctx context.Context, connection net.Conn, bootID string) error {
	writer := &frameWriter{writer: connection}
	networkStack := stack.New(stack.Options{NetworkProtocols: []stack.NetworkProtocolFactory{ipv4.NewProtocol}, TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol}})
	defer networkStack.Destroy()
	endpoint := channel.New(netprotocol.QueueSize, netprotocol.MTU, "")
	defer endpoint.Close()
	if err := networkStack.CreateNIC(1, endpoint); err != nil {
		return fmt.Errorf("create egress NIC: %s", err)
	}
	networkStack.SetPromiscuousMode(1, true)
	networkStack.SetSpoofing(1, true)
	networkStack.SetRouteTable([]tcpip.Route{{Destination: headerIPv4EmptySubnet(), NIC: 1}})
	tcpForwarder := tcp.NewForwarder(networkStack, 65535, 1024, func(request *tcp.ForwarderRequest) { subsystem.handleTCP(ctx, request, bootID) })
	udpForwarder := udp.NewForwarder(networkStack, func(request *udp.ForwarderRequest) { subsystem.handleUDP(ctx, request, bootID) })
	networkStack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpForwarder.HandlePacket)
	networkStack.SetTransportProtocolHandler(udp.ProtocolNumber, udpForwarder.HandlePacket)

	writeErr := make(chan error, 1)
	go func() { writeErr <- writePackets(ctx, writer, endpoint, &subsystem.metrics) }()
	for {
		_ = connection.SetReadDeadline(time.Now().Add(16 * time.Second))
		frameType, payload, err := netprotocol.ReadFrame(connection)
		if err != nil {
			if errors.Is(err, netprotocol.ErrInvalidFrame) {
				subsystem.metrics.invalidFrames.Add(1)
			}
			return err
		}
		switch frameType {
		case netprotocol.Data:
			subsystem.metrics.packetsReceived.Add(1)
			packet := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(payload)})
			endpoint.InjectInbound(ipv4.ProtocolNumber, packet)
			packet.DecRef()
		case netprotocol.Ping:
			if err := writer.write(netprotocol.Pong, payload); err != nil {
				return err
			}
		case netprotocol.Pong:
		default:
			subsystem.metrics.invalidFrames.Add(1)
		}
		select {
		case err := <-writeErr:
			return err
		default:
		}
	}
}

type frameWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (writer *frameWriter) write(frameType netprotocol.Type, payload []byte) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return netprotocol.WriteFrame(writer.writer, frameType, payload)
}

func writePackets(ctx context.Context, writer *frameWriter, endpoint *channel.Endpoint, values *metrics) error {
	for {
		packet := endpoint.ReadContext(ctx)
		if packet == nil {
			return ctx.Err()
		}
		view := packet.ToView()
		data := append([]byte(nil), view.AsSlice()...)
		view.Release()
		packet.DecRef()
		if len(data) > netprotocol.MTU {
			values.packetDrops.Add(1)
			continue
		}
		if err := writer.write(netprotocol.Data, data); err != nil {
			return err
		}
		values.packetsSent.Add(1)
	}
}

func headerIPv4EmptySubnet() tcpip.Subnet {
	subnet, _ := tcpip.NewSubnet(tcpip.AddrFrom4([4]byte{}), tcpip.MaskFromBytes([]byte{0, 0, 0, 0}))
	return subnet
}

func (subsystem *Subsystem) authorize(bootID string, sourceAddress, destinationAddress tcpip.Address, virtualDNS bool) (string, bool, bool) {
	source, ok := addressFromTCPIP(sourceAddress)
	if !ok {
		return "", false, false
	}
	destination, ok := addressFromTCPIP(destinationAddress)
	if !ok {
		return "", false, false
	}
	guest, _, _ := netprotocol.Addresses()
	infrastructure := source == guest
	environment := ""
	var subnets []netip.Prefix
	if state := subsystem.Policies.state.Load(); state != nil && state.bootID == bootID {
		subnets = make([]netip.Prefix, 0, len(state.sources))
		for _, item := range state.sources {
			subnets = append(subnets, item.Subnet)
		}
	}
	if infrastructure {
		if !subsystem.Leases.Active(time.Now()) {
			return "", true, false
		}
	} else {
		resolved, allowed := subsystem.Policies.Resolve(bootID, source)
		if !allowed {
			return "", false, false
		}
		environment = resolved.EnvironmentID
	}
	_, host, _ := netprotocol.Addresses()
	if virtualDNS && destination == host {
		return environment, infrastructure, true
	}
	return environment, infrastructure, DestinationAllowed(destination, subnets, true)
}

func addressFromTCPIP(address tcpip.Address) (netip.Addr, bool) {
	if address.Len() != 4 {
		return netip.Addr{}, false
	}
	return netip.AddrFrom4(address.As4()), true
}
