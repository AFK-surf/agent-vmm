//go:build darwin || linux

package session

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type unsettledUpdateSession struct {
	healthv1.UnimplementedHealthServer
	entered  chan struct{}
	released chan struct{}
}

func (server *unsettledUpdateSession) Check(context.Context, *healthv1.HealthCheckRequest) (*healthv1.HealthCheckResponse, error) {
	close(server.entered)
	<-server.released
	return &healthv1.HealthCheckResponse{Status: healthv1.HealthCheckResponse_SERVING}, nil
}

func TestUpdateSessionDrainRetainsUnsettledHandlersAcrossDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.WaitForHandlers(true))
	handler := &unsettledUpdateSession{entered: make(chan struct{}), released: make(chan struct{})}
	healthv1.RegisterHealthServer(grpcServer, handler)
	go grpcServer.Serve(listener)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go healthv1.NewHealthClient(connection).Check(ctx, &healthv1.HealthCheckRequest{})
	select {
	case <-handler.entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	_, sessionCancel := context.WithCancel(context.Background())
	owner := &Owner{servers: map[string]*server{"old": {timer: time.NewTimer(time.Hour), cancel: sessionCancel, server: grpcServer, listener: listener, stopDone: make(chan struct{})}}}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer drainCancel()
	if err := owner.stopSession(drainCtx, "old"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain deadline=%v", err)
	}
	owner.mu.Lock()
	retained := owner.servers["old"] != nil
	owner.mu.Unlock()
	if !retained {
		t.Fatal("unsettled handler lost its recovery reference")
	}
	close(handler.released)
	if err := owner.stopSession(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	retained = owner.servers["old"] != nil
	owner.mu.Unlock()
	if retained {
		t.Fatal("settled handler retained its session")
	}
}
