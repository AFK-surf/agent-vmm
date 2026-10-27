package remoteconnector

import (
	"context"
	"errors"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type endpointObservationState struct {
	mu    sync.RWMutex
	value *trustv1.EndpointObservation
}

func newEndpointObservationState(value *trustv1.EndpointObservation) *endpointObservationState {
	state := new(endpointObservationState)
	state.Store(value)
	return state
}

func (state *endpointObservationState) Load() *trustv1.EndpointObservation {
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.value == nil {
		return nil
	}
	return proto.Clone(state.value).(*trustv1.EndpointObservation)
}

func (state *endpointObservationState) Store(value *trustv1.EndpointObservation) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if value == nil {
		state.value = nil
		return
	}
	state.value = proto.Clone(value).(*trustv1.EndpointObservation)
}

const observationInterval = 15 * time.Second

var publicHealthComponents = map[string]struct{}{
	"vmmd": {}, "state-disk": {}, "network": {}, "egress": {}, "quota": {},
	"containerd/runsc": {}, "host-egress": {},
}

type remoteObservationClient interface {
	GetRemoteState(context.Context, *connectorv1.GetRemoteStateRequest, ...grpc.CallOption) (*connectorv1.GetRemoteStateResponse, error)
	GetRemoteObservation(context.Context, *connectorv1.GetRemoteObservationRequest, ...grpc.CallOption) (*connectorv1.GetRemoteObservationResponse, error)
}

func collectObservation(ctx context.Context, client remoteObservationClient, registration *hostv1.RemoteRegistration, sequence uint64) *remotev1.RemoteObservationSnapshot {
	now := time.Now()
	result := observationBase(registration, sequence, now)
	response, err := client.GetRemoteObservation(ctx, &connectorv1.GetRemoteObservationRequest{RegistrationId: registration.GetId()})
	if err != nil || response.GetObservedAt() == nil || !response.GetObservedAt().IsValid() {
		result.Health = &remotev1.RemoteObservationHealth{Status: "unavailable", Issue: "observation_unavailable", Message: "Host observation is unavailable."}
		result.Usage = &remotev1.RemoteObservationUsage{Stale: true, Issue: "observation_unavailable"}
		return result
	}
	observed := response.GetObservedAt().AsTime()
	if observed.Before(now.Add(-2*time.Minute)) || observed.After(now.Add(30*time.Second)) {
		result.Health = &remotev1.RemoteObservationHealth{Status: "unavailable", Issue: "observation_time_invalid", Message: "Host observation time is invalid."}
		result.Usage = &remotev1.RemoteObservationUsage{Stale: true, Issue: "observation_time_invalid"}
		return result
	}
	result.ObservedUnixMillis = observed.UnixMilli()
	result.InventoryObservedUnixMillis = observed.UnixMilli()
	result.Health = publicHealth(response.GetHealth())
	result.Usage = publicUsage(response.GetUsage())
	if candidate := response.GetReclaimCandidate(); candidate != nil && candidate.GetRegistrationId() == registration.GetId() {
		result.ReclaimCandidate = &remotev1.WorkloadReclaimCandidate{RegistrationId: candidate.GetRegistrationId(), AllocationId: candidate.GetAllocationId(), ContainerId: candidate.GetContainerId(), ContainerInstanceId: candidate.GetContainerInstanceId(), ExpiresUnixMillis: candidate.GetExpiresUnixMillis()}
	}
	return result
}

func collectCurrentObservation(ctx context.Context, client remoteObservationClient, registrationID string, sequence uint64) (*remotev1.RemoteObservationSnapshot, error) {
	state, err := client.GetRemoteState(ctx, &connectorv1.GetRemoteStateRequest{RegistrationId: registrationID, SummaryOnly: true})
	if err != nil || state.GetRegistration() == nil {
		return nil, errors.New("current remote registration observation is unavailable")
	}
	return collectObservation(ctx, client, state.GetRegistration(), sequence), nil
}

func observationBase(registration *hostv1.RemoteRegistration, sequence uint64, observed time.Time) *remotev1.RemoteObservationSnapshot {
	pool := registration.GetPool()
	return &remotev1.RemoteObservationSnapshot{
		Sequence: sequence, ObservedUnixMillis: observed.UnixMilli(),
		Capacity:                    &remotev1.Capacity{PerEnvironmentLimits: limitsToRemote(pool.GetPerEnvironmentLimits()), MaxEgressMode: remotev1.EgressMode(pool.GetMaxEgressMode())},
		InventoryWatermark:          registration.GetInventoryRevision(),
		InventoryObservedUnixMillis: observed.UnixMilli(),
	}
}

func publicHealth(value *hostv1.HealthStatus) *remotev1.RemoteObservationHealth {
	result := &remotev1.RemoteObservationHealth{Status: healthState(value.GetState())}
	if result.Status != "healthy" {
		result.Issue = "host_" + result.Status
		result.Message = "Host health is " + result.Status + "."
	}
	for _, component := range value.GetComponents() {
		if len(result.Components) >= 16 {
			break
		}
		if _, ok := publicHealthComponents[component.GetComponent()]; !ok {
			continue
		}
		result.Components = append(result.Components, &remotev1.RemoteObservationComponent{Component: component.GetComponent(), Status: healthState(component.GetState())})
	}
	return result
}

func healthState(value hostv1.HealthState) string {
	switch value {
	case hostv1.HealthState_HEALTH_STATE_HEALTHY:
		return "healthy"
	case hostv1.HealthState_HEALTH_STATE_DEGRADED:
		return "degraded"
	default:
		return "unavailable"
	}
}

func publicUsage(value *hostv1.Usage) *remotev1.RemoteObservationUsage {
	if value == nil {
		return &remotev1.RemoteObservationUsage{Stale: true, Issue: "usage_unavailable"}
	}
	result := &remotev1.RemoteObservationUsage{CpuNanos: value.GetCpuNanos(), MemoryBytes: value.GetMemoryBytes(), Pids: value.GetPids(), ReadBytes: value.GetReadBytes(), WriteBytes: value.GetWriteBytes(), DiskBytes: value.GetDiskBytes(), NetworkRxBytes: value.GetNetworkRxBytes(), NetworkTxBytes: value.GetNetworkTxBytes(), Stale: value.GetStale()}
	if value.GetSampledAt() != nil && value.GetSampledAt().IsValid() {
		result.SampledUnixMillis = value.GetSampledAt().AsTime().UnixMilli()
	}
	if result.Stale {
		result.Issue = "usage_stale"
	}
	return result
}

func renewRemoteObservations(ctx context.Context, client connectorv1.RemoteComputeServiceClient, registration *hostv1.RemoteRegistration, epoch uint64, initialSequence uint64, writer *controlWriter) error {
	ticker := time.NewTicker(observationInterval)
	defer ticker.Stop()
	sequence := initialSequence
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			observation, err := collectCurrentObservation(ctx, client, registration.GetId(), sequence+1)
			if err != nil {
				return err
			}
			if err := writer.Send(ctx, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_ObservationRenewal{ObservationRenewal: &remotev1.RemoteObservationRenewal{ConnectionEpoch: epoch, Observation: observation}}}); err != nil {
				return err
			}
			sequence++
		}
	}
}
