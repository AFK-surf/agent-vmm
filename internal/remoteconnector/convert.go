package remoteconnector

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
)

func hello(registration *hostv1.RemoteRegistration, instance string, epoch uint64, endpointObservation *trustv1.EndpointObservation, observation *remotev1.RemoteObservationSnapshot) *remotev1.HostHello {
	pool := registration.GetPool()
	message := &remotev1.HostHello{
		ProtocolVersion: ProtocolVersion, HostApiVersion: HostAPIVersion, ConnectorRelease: ConnectorRelease,
		RegistrationId: registration.GetId(), ConnectionInstance: instance, PoolRevision: registration.GetPoolPolicyRevision(),
		ConnectionEpoch: epoch, SupportedFeatures: append([]string(nil), protocolFeatures...),
		RequiredFeatures:     append([]string(nil), requiredProtocolFeatures...),
		InventoryWatermark:   registration.GetInventoryRevision(),
		Capacity:             &remotev1.Capacity{PerEnvironmentLimits: pool.GetPerEnvironmentLimits(), MaxEgressMode: pool.GetMaxEgressMode()},
		HostLoopbackFrontend: pool.GetHostLoopbackFrontend(),
		EndpointObservation:  endpointObservation,
		Observation:          observation,
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
	return &remotev1.Allocation{AllocationId: allocation.GetAllocationId(), DisplayLabel: boundedLabel(allocation.GetDisplayLabel()), EgressMode: allocation.GetEgressMode(), Revision: allocation.GetRevision(), State: allocation.GetState(), Generation: allocation.GetGeneration(), ResourcesV2: allocation.GetResourcesV2()}
}
