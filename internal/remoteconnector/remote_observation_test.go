package remoteconnector

import (
	"context"
	"errors"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type observationClientStub struct {
	state            *connectorv1.GetRemoteStateResponse
	stateErr         error
	observationCalls int
	candidate        *connectorv1.WorkloadReclaimCandidate
}

func (client *observationClientStub) GetRemoteState(_ context.Context, request *connectorv1.GetRemoteStateRequest, _ ...grpc.CallOption) (*connectorv1.GetRemoteStateResponse, error) {
	if !request.GetSummaryOnly() {
		panic("periodic observation must not load stored allocations")
	}
	return client.state, client.stateErr
}

func (client *observationClientStub) GetRemoteObservation(context.Context, *connectorv1.GetRemoteObservationRequest, ...grpc.CallOption) (*connectorv1.GetRemoteObservationResponse, error) {
	client.observationCalls++
	return &connectorv1.GetRemoteObservationResponse{ObservedAt: timestamppb.Now(), ReclaimCandidate: client.candidate}, nil
}

func TestPublicObservationUsesFiniteHealthAndDropsNativeReasons(t *testing.T) {
	health := publicHealth(&hostv1.HealthStatus{
		State: hostv1.HealthState_HEALTH_STATE_DEGRADED,
		Components: []*hostv1.ComponentHealth{
			{Component: "network", State: hostv1.HealthState_HEALTH_STATE_DEGRADED, Reason: "/private/path: secret"},
			{Component: "unexpected-private-component", State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE, Reason: "native error"},
		},
	})
	if health.GetStatus() != "degraded" || health.GetIssue() != "host_degraded" || len(health.GetComponents()) != 1 {
		t.Fatalf("health = %+v", health)
	}
	if strings.Contains(health.String(), "private") || strings.Contains(health.String(), "secret") || strings.Contains(health.String(), "native error") {
		t.Fatalf("health leaked a native reason: %s", health.String())
	}

	usage := publicUsage(&hostv1.Usage{MemoryBytes: 42, Stale: true, UnavailableReason: "/tmp/private", SampledAt: timestamppb.New(time.Now())})
	if usage.GetMemoryBytes() != 42 || usage.GetIssue() != "usage_stale" || strings.Contains(usage.String(), "private") {
		t.Fatalf("usage = %+v", usage)
	}

}

func TestCurrentObservationDoesNotRefreshCachedInventoryWhenStateReadFails(t *testing.T) {
	client := &observationClientStub{
		state: &connectorv1.GetRemoteStateResponse{Registration: &hostv1.RemoteRegistration{
			Id: "registration", InventoryRevision: 41,
		}},
		stateErr: errors.New("state unavailable"),
	}

	observation, err := collectCurrentObservation(context.Background(), client, "registration", 2)
	if err == nil || observation != nil {
		t.Fatalf("collectCurrentObservation() = (%v, %v), want no observation and an error", observation, err)
	}
	if client.observationCalls != 0 {
		t.Fatalf("GetRemoteObservation calls = %d, want 0", client.observationCalls)
	}
}

func TestObservationReclaimCandidateDoesNotCrossRegistration(t *testing.T) {
	for _, owner := range []string{"registration-a", "registration-b"} {
		client := &observationClientStub{candidate: &connectorv1.WorkloadReclaimCandidate{RegistrationId: owner, AllocationId: "allocation", ContainerId: "worker", ContainerInstanceId: "instance", ExpiresUnixMillis: time.Now().Add(time.Second).UnixMilli()}}
		observation := collectObservation(context.Background(), client, &hostv1.RemoteRegistration{Id: "registration-a"}, 1)
		if owner == "registration-a" {
			if observation.GetReclaimCandidate().GetContainerInstanceId() != "instance" {
				t.Fatal("scoped candidate lost exact instance")
			}
		} else if observation.GetReclaimCandidate() != nil {
			t.Fatal("another registration received reclaim authority")
		}
	}
}
