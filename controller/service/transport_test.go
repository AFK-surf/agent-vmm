package service

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
)

func TestMemoryTransportKeepsAuthorityIndependentOfRouteClass(t *testing.T) {
	for _, class := range []servicev1.RouteClass{servicev1.RouteClass_ROUTE_CLASS_LOCAL, servicev1.RouteClass_ROUTE_CLASS_DEVICE, servicev1.RouteClass_ROUTE_CLASS_COMPUTE, servicev1.RouteClass_ROUTE_CLASS_LAN} {
		t.Run(class.String(), func(t *testing.T) {
			now := time.Now()
			capability := &trustv1.RouteCapability{CapabilityId: "capability", RouteGeneration: 4}
			binding := Binding{RouteID: "route", LeaseID: "lease", Class: class, Generation: 4, SourceEnvironmentID: "source",
				SourceContainerInstanceID: "source-v1", DestinationExportID: "export", DestinationInstanceID: "destination-v1", ExpiresAt: now.Add(time.Minute), Capability: capability}
			transport := NewMemoryTransport()
			if err := transport.Register(binding, func(_ context.Context, connection net.Conn) {
				buffer := make([]byte, 4)
				_, _ = io.ReadFull(connection, buffer)
				_, _ = connection.Write(buffer)
			}); err != nil {
				t.Fatal(err)
			}
			header := &servicev1.OpenServiceForwardHeader{RouteId: "route", LeaseId: "lease", RouteGeneration: 4,
				SourceEnvironmentId: "source", SourceContainerInstanceId: "source-v1", Capability: capability}
			connection, err := transport.Open(context.Background(), header, now)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if _, err := connection.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			response := make([]byte, 4)
			if _, err := io.ReadFull(connection, response); err != nil || string(response) != "ping" {
				t.Fatalf("response=%q err=%v", response, err)
			}
		})
	}
}

func TestMemoryTransportRejectsForgedExpiredAndRevokedRoutes(t *testing.T) {
	now := time.Now()
	capability := &trustv1.RouteCapability{CapabilityId: "capability", RouteGeneration: 9}
	binding := Binding{RouteID: "route", LeaseID: "lease", Class: servicev1.RouteClass_ROUTE_CLASS_DEVICE, Generation: 9,
		SourceEnvironmentID: "source", SourceContainerInstanceID: "container-v1", ExpiresAt: now.Add(time.Minute), Capability: capability}
	transport := NewMemoryTransport()
	if err := transport.Register(binding, func(context.Context, net.Conn) {}); err != nil {
		t.Fatal(err)
	}
	base := &servicev1.OpenServiceForwardHeader{RouteId: "route", LeaseId: "lease", RouteGeneration: 9,
		SourceEnvironmentId: "source", SourceContainerInstanceId: "container-v1", Capability: capability}
	for name, mutate := range map[string]func(*servicev1.OpenServiceForwardHeader){
		"allocation route":      func(value *servicev1.OpenServiceForwardHeader) { value.RouteId = "forged" },
		"namespace":             func(value *servicev1.OpenServiceForwardHeader) { value.SourceEnvironmentId = "other" },
		"container replacement": func(value *servicev1.OpenServiceForwardHeader) { value.SourceContainerInstanceId = "container-v2" },
		"generation":            func(value *servicev1.OpenServiceForwardHeader) { value.RouteGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			forged := *base
			mutate(&forged)
			if connection, err := transport.Open(context.Background(), &forged, now); err == nil {
				connection.Close()
				t.Fatal("forged route opened")
			}
		})
	}
	if connection, err := transport.Open(context.Background(), base, now.Add(2*time.Minute)); err == nil {
		connection.Close()
		t.Fatal("expired route opened")
	}
	transport.Revoke("route")
	if connection, err := transport.Open(context.Background(), base, now); err == nil {
		connection.Close()
		t.Fatal("revoked route opened")
	}
}
