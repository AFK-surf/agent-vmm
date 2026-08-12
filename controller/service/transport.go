package service

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
)

// Binding is the complete authorization fence carried independently of the
// selected LOCAL, DEVICE, COMPUTE, or LAN data transport. It is intentionally
// address-free: a caller cannot select a peer endpoint or workload namespace.
type Binding struct {
	RouteID                   string
	LeaseID                   string
	Class                     servicev1.RouteClass
	Generation                uint64
	SourceEnvironmentID       string
	SourceContainerInstanceID string
	DestinationExportID       string
	DestinationInstanceID     string
	ExpiresAt                 time.Time
	Capability                *trustv1.RouteCapability
}

func (binding Binding) Validate(header *servicev1.OpenServiceForwardHeader, now time.Time) error {
	if header == nil || binding.RouteID == "" || binding.LeaseID == "" || binding.Generation == 0 || binding.Capability == nil {
		return errors.New("complete service route binding is required")
	}
	if binding.Class != servicev1.RouteClass_ROUTE_CLASS_LOCAL && binding.Class != servicev1.RouteClass_ROUTE_CLASS_DEVICE &&
		binding.Class != servicev1.RouteClass_ROUTE_CLASS_COMPUTE && binding.Class != servicev1.RouteClass_ROUTE_CLASS_LAN {
		return errors.New("unsupported service route class")
	}
	if !now.Before(binding.ExpiresAt) || header.GetRouteId() != binding.RouteID || header.GetLeaseId() != binding.LeaseID ||
		header.GetRouteGeneration() != binding.Generation || header.GetSourceEnvironmentId() != binding.SourceEnvironmentID ||
		header.GetSourceContainerInstanceId() != binding.SourceContainerInstanceID || !proto.Equal(header.GetCapability(), binding.Capability) {
		return errors.New("service route binding is stale or mismatched")
	}
	return nil
}

// MemoryTransport is a bounded test/lab transport. It models transport
// selection and disconnect without becoming an authority or persisting facts.
type MemoryTransport struct {
	mu       sync.RWMutex
	routes   map[string]Binding
	handlers map[string]func(context.Context, net.Conn)
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{routes: make(map[string]Binding), handlers: make(map[string]func(context.Context, net.Conn))}
}

func (transport *MemoryTransport) Register(binding Binding, handler func(context.Context, net.Conn)) error {
	if binding.RouteID == "" || handler == nil {
		return errors.New("route and handler are required")
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if _, exists := transport.routes[binding.RouteID]; exists {
		return errors.New("route already registered")
	}
	transport.routes[binding.RouteID], transport.handlers[binding.RouteID] = binding, handler
	return nil
}

func (transport *MemoryTransport) Revoke(routeID string) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	delete(transport.routes, routeID)
	delete(transport.handlers, routeID)
}

func (transport *MemoryTransport) Open(ctx context.Context, header *servicev1.OpenServiceForwardHeader, now time.Time) (net.Conn, error) {
	transport.mu.RLock()
	binding, ok := transport.routes[header.GetRouteId()]
	handler := transport.handlers[header.GetRouteId()]
	transport.mu.RUnlock()
	if !ok || handler == nil {
		return nil, errors.New("service route is unavailable")
	}
	if err := binding.Validate(header, now); err != nil {
		return nil, err
	}
	client, destination := net.Pipe()
	go func() {
		defer destination.Close()
		handler(ctx, destination)
	}()
	return client, nil
}
