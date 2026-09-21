package mcpappliance

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestHTTPFrontendBindsSessionAndReleasesOnDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controller := NewController(&Identity{}, t.TempDir(), nil)
	controllerListener := bufconn.Listen(1 << 20)
	controllerServer := grpc.NewServer()
	remotev1.RegisterRemoteControllerServiceServer(controllerServer, controller)
	go func() { _ = controllerServer.Serve(controllerListener) }()
	t.Cleanup(controllerServer.Stop)
	controllerConnection, err := grpc.NewClient("passthrough:///controller", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return controllerListener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { controllerConnection.Close() })

	stream := &applianceControlStream{ctx: ctx, received: make(chan *remotev1.HostControlMessage, 32)}
	hello := applianceHello()
	stream.received <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}}
	session, err := controllerremote.AcceptControl(stream, "registration")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	liveCtx, stopLive := context.WithCancel(ctx)
	live := &controlConnection{session: session, requests: make(chan commandRequest), done: make(chan struct{}), hello: hello, close: stopLive}
	controller.install(live)
	go func() { _ = controller.runControl(liveCtx, live) }()

	stream.onCommand = func(command *remotev1.ControllerCommand) {
		open := command.GetOpenSession()
		if open == nil {
			return
		}
		go bridgeTestRuntime(ctx, t, remotev1.NewRemoteControllerServiceClient(controllerConnection), open)
	}
	httpListener := bufconn.Listen(1 << 20)
	httpDone := make(chan error, 1)
	httpContext, stopHTTP := context.WithCancel(ctx)
	go func() { httpDone <- controller.ServeHTTPFrontend(httpContext, httpListener) }()
	client := &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return httpListener.Dial() }}}
	token := strings.Repeat("0a", bindingTokenLength/2)
	otherToken := strings.Repeat("0b", bindingTokenLength/2)
	initialize := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`)
	for _, authorization := range []string{"", "Bearer invalid"} {
		unauthorized, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/mcp", bytes.NewReader(initialize))
		if err != nil {
			t.Fatal(err)
		}
		unauthorized.Header.Set("Authorization", authorization)
		unauthorized.Header.Set("Content-Type", "application/json")
		unauthorized.Header.Set("Accept", "application/json, text/event-stream")
		unauthorizedResponse, err := client.Do(unauthorized)
		if err != nil {
			t.Fatal(err)
		}
		unauthorizedResponse.Body.Close()
		if unauthorizedResponse.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthorized status = %d", unauthorizedResponse.StatusCode)
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/mcp", bytes.NewReader(initialize))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("initialize status=%d session=%q", response.StatusCode, response.Header.Get("Mcp-Session-Id"))
	}
	sessionID := response.Header.Get("Mcp-Session-Id")
	initialized, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)))
	if err != nil {
		t.Fatal(err)
	}
	initialized.Header.Set("Authorization", "Bearer "+token)
	initialized.Header.Set("Mcp-Session-Id", sessionID)
	initialized.Header.Set("Content-Type", "application/json")
	initialized.Header.Set("Accept", "application/json, text/event-stream")
	initializedResponse, err := client.Do(initialized)
	if err != nil {
		t.Fatal(err)
	}
	initializedResponse.Body.Close()
	if initializedResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("same-binding session status = %d", initializedResponse.StatusCode)
	}
	stolen, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	stolen.Header.Set("Authorization", "Bearer "+otherToken)
	stolen.Header.Set("Mcp-Session-Id", sessionID)
	stolenResponse, err := client.Do(stolen)
	if err != nil {
		t.Fatal(err)
	}
	stolenResponse.Body.Close()
	if stolenResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("stolen session status = %d", stolenResponse.StatusCode)
	}
	otherInitialize, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/mcp", bytes.NewReader(initialize))
	if err != nil {
		t.Fatal(err)
	}
	otherInitialize.Header.Set("Authorization", "Bearer "+otherToken)
	otherInitialize.Header.Set("Content-Type", "application/json")
	otherInitialize.Header.Set("Accept", "application/json, text/event-stream")
	otherResponse, err := client.Do(otherInitialize)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, otherResponse.Body)
	otherResponse.Body.Close()
	if otherResponse.StatusCode != http.StatusOK || otherResponse.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("other initialize status=%d session=%q", otherResponse.StatusCode, otherResponse.Header.Get("Mcp-Session-Id"))
	}
	deleteRequest, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest.Header.Set("Authorization", "Bearer "+token)
	deleteRequest.Header.Set("Mcp-Session-Id", sessionID)
	deleteResponse, err := client.Do(deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	deleteResponse.Body.Close()
	if deleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", deleteResponse.StatusCode)
	}
	stream.mu.Lock()
	allocations := make(map[string]struct{})
	openedSessions := 0
	for _, command := range stream.commands {
		if ensure := command.GetEnsureAllocation(); ensure != nil {
			allocations[ensure.GetAllocationId()] = struct{}{}
		}
		if command.GetOpenSession() != nil {
			openedSessions++
		}
	}
	stream.mu.Unlock()
	if len(allocations) != 2 || openedSessions != 2 {
		t.Fatalf("binding allocations = %v, opened sessions = %d", allocations, openedSessions)
	}
	stopHTTP()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stream.mu.Lock()
		var releasedAllocations int
		for _, command := range stream.commands {
			if command.GetReleaseAllocation() != nil {
				releasedAllocations++
			}
		}
		stream.mu.Unlock()
		if releasedAllocations == 2 {
			select {
			case err := <-httpDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP frontend did not stop")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("DELETE and HTTP stop did not release both allocations and runtimes")
}

func bridgeTestRuntime(ctx context.Context, t *testing.T, client remotev1.RemoteControllerServiceClient, open *remotev1.OpenSession) {
	t.Helper()
	runtimeListener := bufconn.Listen(1 << 20)
	runtimeServer := grpc.NewServer()
	hostv1.RegisterAgentRuntimeServiceServer(runtimeServer, &hostv1.UnimplementedAgentRuntimeServiceServer{})
	go func() { _ = runtimeServer.Serve(runtimeListener) }()
	defer runtimeServer.Stop()
	runtimeConnection, err := runtimeListener.Dial()
	if err != nil {
		return
	}
	defer runtimeConnection.Close()
	frontend, err := client.ServeSession(ctx)
	if err != nil {
		return
	}
	if frontend.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Header{Header: &remotev1.SessionHeader{RegistrationId: "registration", ConnectionEpoch: 7, AllocationId: open.GetAllocationId(), AllocationGeneration: open.GetAllocationGeneration()}}}) != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		buffer := make([]byte, 32<<10)
		for {
			count, err := runtimeConnection.Read(buffer)
			if count > 0 && frontend.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Data{Data: append([]byte(nil), buffer[:count]...)}}) != nil {
				return
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			frame, err := frontend.Recv()
			if err != nil {
				return
			}
			if len(frame.GetData()) > 0 {
				if _, err := runtimeConnection.Write(frame.GetData()); err != nil {
					return
				}
			}
			if frame.GetCloseWrite() {
				return
			}
		}
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
}
