package remoteconnector

import (
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func hello(registration *hostv1.RemoteRegistration, instance string, epoch uint64, endpointObservation *trustv1.EndpointObservation, observation *remotev1.RemoteObservationSnapshot) *remotev1.HostHello {
	pool := registration.GetPool()
	message := &remotev1.HostHello{
		ProtocolVersion: ProtocolVersion, HostApiVersion: HostAPIVersion, ConnectorRelease: ConnectorRelease,
		RegistrationId: registration.GetId(), ConnectionInstance: instance, PoolRevision: registration.GetPoolPolicyRevision(),
		ConnectionEpoch: epoch, SupportedFeatures: append([]string(nil), protocolFeatures...),
		RequiredFeatures:     append([]string(nil), requiredProtocolFeatures...),
		InventoryWatermark:   registration.GetInventoryRevision(),
		Capacity:             &remotev1.Capacity{MaxEnvironments: pool.GetMaxEnvironments(), AggregateLimits: pool.GetAggregateLimits(), PerEnvironmentLimits: pool.GetPerEnvironmentLimits(), MaxEgressMode: pool.GetMaxEgressMode(), MaxLeaseSeconds: seconds(pool.GetMaxLeaseDuration()), DisconnectGraceSeconds: seconds(pool.GetDisconnectGrace())},
		HostLoopbackFrontend: pool.GetHostLoopbackFrontend(),
		EndpointObservation:  endpointObservation,
		Observation:          observation,
	}
	for _, allocation := range registration.GetAllocations() {
		message.Inventory = append(message.Inventory, allocationToRemote(allocation))
	}
	return message
}

func seconds(duration *durationpb.Duration) uint32 {
	if duration == nil || duration.AsDuration() <= 0 {
		return 0
	}
	return uint32(duration.AsDuration() / time.Second)
}

func allocationToRemote(allocation *hostv1.RemoteAllocation) *remotev1.Allocation {
	if allocation == nil {
		return nil
	}
	return &remotev1.Allocation{AllocationId: allocation.GetAllocationId(), DisplayLabel: boundedLabel(allocation.GetDisplayLabel()), Limits: allocation.GetLimits(), EgressMode: allocation.GetEgressMode(), Revision: allocation.GetRevision(), State: allocation.GetState(), Lease: leaseToRemote(allocation.GetLease(), allocation.GetRevision())}
}

func leaseToRemote(lease *hostv1.RemoteLease, allocationRevision uint64) *remotev1.Lease {
	if lease == nil {
		return nil
	}
	var expiry int64
	if lease.GetExpiresAt() != nil {
		expiry = lease.GetExpiresAt().AsTime().UnixMilli()
	}
	return &remotev1.Lease{LeaseId: lease.GetLeaseId(), AllocationId: lease.GetAllocationId(), WorkloadRef: boundedLabel(lease.GetWorkloadRef()), Generation: lease.GetGeneration(), ExpiresUnixMillis: expiry, State: lease.GetState(), AllocationRevision: allocationRevision}
}
