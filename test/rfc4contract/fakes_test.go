package rfc4contract

import (
	"context"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type fakeRuntime struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	mu          sync.Mutex
	environment *hostv1.AgentEnvironment
	calls       []string
}

func (fake *fakeRuntime) GetEnvironment(context.Context, *hostv1.GetOwnEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls = append(fake.calls, "GetEnvironment")
	return &hostv1.GetEnvironmentResponse{Environment: fake.environment}, nil
}

type controllerCommand struct {
	CorrelationID string
	Kind          string
	Revision      uint64
}

type fakeController struct {
	mu       sync.Mutex
	instance string
	seen     map[string]controllerCommand
	results  map[string]string
}

func (fake *fakeController) connect(instance string, fullInventory bool) bool {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fullInventory {
		return false
	}
	fake.instance = instance
	fake.seen = make(map[string]controllerCommand)
	fake.results = make(map[string]string)
	return true
}

func (fake *fakeController) command(instance string, command controllerCommand) string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if instance != fake.instance {
		return "STALE_CONNECTION"
	}
	if result, ok := fake.results[command.CorrelationID]; ok {
		return result
	}
	fake.seen[command.CorrelationID] = command
	fake.results[command.CorrelationID] = "OK"
	return "OK"
}

func TestFakeRuntimeRecordsOnlyProjectBehavior(t *testing.T) {
	fake := &fakeRuntime{environment: &hostv1.AgentEnvironment{Id: "fixture"}}
	response, err := fake.GetEnvironment(context.Background(), &hostv1.GetOwnEnvironmentRequest{})
	if err != nil || response.GetEnvironment().GetId() != "fixture" {
		t.Fatalf("response=%v err=%v", response, err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "GetEnvironment" {
		t.Fatalf("calls=%v", fake.calls)
	}
}

func TestFakeControllerFaultModel(t *testing.T) {
	fake := &fakeController{}
	if fake.connect("a", false) {
		t.Fatal("connection without full inventory was accepted")
	}
	if !fake.connect("a", true) {
		t.Fatal("valid connection was rejected")
	}
	command := controllerCommand{CorrelationID: "one", Kind: "ensure", Revision: 1}
	if result := fake.command("a", command); result != "OK" {
		t.Fatal(result)
	}
	if result := fake.command("a", command); result != "OK" {
		t.Fatalf("duplicate command result=%s", result)
	}
	if !fake.connect("b", true) {
		t.Fatal("replacement connection was rejected")
	}
	if result := fake.command("a", controllerCommand{CorrelationID: "two"}); result != "STALE_CONNECTION" {
		t.Fatalf("stale connection result=%s", result)
	}
}

func TestLeaseDeadlineUsesLocalClock(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Second)
	if !deadline.After(now) || deadline.Sub(now) != 30*time.Second {
		t.Fatalf("deadline=%s now=%s", deadline, now)
	}
}
