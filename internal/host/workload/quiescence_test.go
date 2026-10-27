//go:build darwin || linux

package workload

import (
	"context"
	"errors"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

func TestRegistrationQuiescenceWaitsForLastExecution(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	controller.mu.Lock()
	run.registrationID = "registration"
	controller.mu.Unlock()
	identity := auth.Identity{EnvironmentID: "env-a", AllocationID: "allocation-a", ExecutionOwnerID: "runtime-instance-a:1"}
	tokens := make([]*ExecutionToken, 0, 2)
	for _, id := range []string{"first", "last"} {
		token, err := controller.AcquireStreamExecution(t.Context(), identity, "main", "instance-a", id, hostv1.ExecutionKind_EXECUTION_KIND_PROCESS, time.Now().Add(time.Minute).UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	done := make(chan error, 1)
	go func() { done <- controller.WaitRegistrationQuiesced(t.Context(), "registration") }()
	controller.ReleaseStreamExecution(tokens[0])
	select {
	case err := <-done:
		t.Fatalf("returned with an active execution: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	controller.ReleaseStreamExecution(tokens[1])
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("last execution did not wake drain")
	}
}

func TestRegistrationQuiescenceHonorsContextCancellation(t *testing.T) {
	controller, run, _ := executionTestFixture(t)
	controller.mu.Lock()
	run.registrationID = "registration"
	run.starting = true
	controller.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- controller.WaitRegistrationQuiesced(ctx, "registration") }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("drain error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled drain did not return")
	}
}
