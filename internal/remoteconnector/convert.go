package remoteconnector

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
)

func hello(registration *hostv1.RemoteRegistration, instance string, epoch uint64, endpointObservation *trustv1.EndpointObservation, observation *remotev1.RemoteObservationSnapshot) *remotev1.HostHello {
	pool := registration.GetPool()
	message := &remotev1.HostHello{
		ProtocolVersion: ProtocolVersion, ConnectorRelease: ConnectorRelease,
		RegistrationId: registration.GetId(), ConnectionInstance: instance, PoolRevision: registration.GetPoolPolicyRevision(),
		ConnectionEpoch: epoch, SupportedFeatures: append([]string(nil), protocolFeatures...),
		InventoryWatermark:  registration.GetInventoryRevision(),
		Capacity:            &remotev1.Capacity{PerEnvironmentLimits: limitsToRemote(pool.GetPerEnvironmentLimits()), MaxEgressMode: remotev1.EgressMode(pool.GetMaxEgressMode())},
		EndpointObservation: endpointObservation,
		Observation:         observation,
	}
	for index, allocation := range registration.GetAllocations() {
		if index == MaxActiveInventoryItems {
			break
		}
		message.Inventory = append(message.Inventory, allocationToRemote(allocation))
	}
	return message
}

func allocationToRemote(allocation *hostv1.RemoteAllocation) *remotev1.Allocation {
	if allocation == nil {
		return nil
	}
	return &remotev1.Allocation{AllocationId: allocation.GetAllocationId(), DisplayLabel: boundedLabel(allocation.GetDisplayLabel()), EgressMode: remotev1.EgressMode(allocation.GetEgressMode()), Revision: allocation.GetRevision(), State: remotev1.AllocationState(allocation.GetState()), Generation: allocation.GetGeneration(), Limits: limitsToRemote(allocation.GetLimits())}
}

func limitsToRemote(value *hostv1.ResourceLimits) *remotev1.ResourceLimits {
	if value == nil {
		return nil
	}
	return &remotev1.ResourceLimits{Pids: value.Pids, DiskBytes: value.DiskBytes}
}
func limitsToHost(value *remotev1.ResourceLimits) *hostv1.ResourceLimits {
	if value == nil {
		return nil
	}
	return &hostv1.ResourceLimits{Pids: value.Pids, DiskBytes: value.DiskBytes}
}
