//go:build darwin || linux

package appliance

import (
	"context"
	"net/netip"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
)

func (controller *Owner) EgressSources(ctx context.Context, client vmmdv1.VMMDServiceClient, bootID string) ([]egress.Source, error) {
	sources := []egress.Source{}
	installations, err := controller.store.ListApplianceInstallations(ctx)
	if err != nil {
		return nil, err
	}
	cursor := ""
	for {
		response, err := client.ListSystemAppliances(ctx, &vmmdv1.ListSystemAppliancesRequest{AfterInstallationId: cursor, Limit: 64})
		if err != nil {
			return nil, err
		}
		for _, appliance := range response.GetAppliances() {
			identity := appliance.GetIdentity()
			subnet, err := netip.ParsePrefix(appliance.GetNetworkSubnet())
			if err != nil {
				continue
			}
			source := egress.Source{EnvironmentID: "appliance:" + identity.GetInstallationId(), NamespaceID: identity.GetNamespaceId(), Subnet: subnet}
			for _, installation := range installations {
				if installation.ID != identity.GetInstallationId() || !installation.DesiredEnabled || installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) || appliance.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || identity.GetBootId() != bootID || installation.CurrentBootID != bootID || identity.GetContainerInstanceId() == "" || identity.GetNamespaceId() != systemNamespaceID(installation.ID) || identity.GetContainerInstanceId() != installation.CurrentContainerInstance {
					continue
				}
				grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
				if err != nil {
					return nil, err
				}
				if grant.EgressMode == int32(hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET) {
					source.Mode = egress.PublicInternet
				}
				source.LANEgress = grant.LANEgress
				source.HostTCPEndpoints = append([]netip.AddrPort(nil), grant.HostTCPEndpoints...)
				source.InstanceID = identity.GetContainerInstanceId()
				source.GrantRevision = grant.Revision
			}
			sources = append(sources, source)
		}
		if response.GetNextCursor() == "" {
			break
		}
		cursor = response.GetNextCursor()
	}
	return sources, nil
}

func (controller *Owner) HostTCPInfrastructureEndpoints() []netip.AddrPort {
	var endpoints []netip.AddrPort
	controller.applianceFrontendMu.Lock()
	for _, frontend := range controller.applianceFrontends {
		if frontend.listener != nil {
			if endpoint, err := netip.ParseAddrPort(frontend.listener.Addr().String()); err == nil {
				endpoints = append(endpoints, endpoint)
			}
		}
	}
	controller.applianceFrontendMu.Unlock()
	return endpoints
}
