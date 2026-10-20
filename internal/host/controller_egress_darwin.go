//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
)

func (controller *Controller) startEgress() error {
	controller.mu.Lock()
	if controller.egressCancel != nil {
		controller.mu.Unlock()
		return nil
	}
	listener, bootID, err := controller.manager.ListenEgress()
	if err != nil {
		controller.mu.Unlock()
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	controller.egressCancel = cancel
	controller.egressAcceptWG.Add(1)
	controller.mu.Unlock()
	go func() {
		defer controller.egressAcceptWG.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			if ctx.Err() != nil {
				_ = connection.Close()
				return
			}
			controller.egressSessionWG.Add(1)
			go func(connection net.Conn) {
				defer controller.egressSessionWG.Done()
				_ = controller.egress.Serve(ctx, connection, bootID)
			}(connection)
		}
	}()
	return nil
}

func (controller *Controller) stopEgress() {
	controller.mu.Lock()
	cancel := controller.egressCancel
	controller.egressCancel = nil
	controller.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	controller.manager.CloseEgress()
	controller.egressAcceptWG.Wait()
	controller.egress.Close()
	controller.egressSessionWG.Wait()
}

func (controller *Controller) publishSourcePoliciesLocked(ctx context.Context, excludedEnvironmentID string) error {
	vm := controller.manager.Snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING || vm.GetBootId() == "" {
		controller.egress.Policies.Clear()
		return errors.New("VM is not running")
	}
	environments, err := controller.store.ListEnvironments(ctx)
	if err != nil {
		controller.egress.Policies.Clear()
		return err
	}
	if excludedEnvironmentID != "" {
		filtered := environments[:0]
		for _, environment := range environments {
			if environment.ID != excludedEnvironmentID {
				filtered = append(filtered, environment)
			}
		}
		environments = filtered
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		controller.egress.Policies.Clear()
		return err
	}
	defer connection.Close()
	response, err := client.ListNamespaces(ctx, &vmmdv1.ListNamespacesRequest{})
	if err != nil {
		controller.egress.Policies.Clear()
		return err
	}
	namespaces := make(map[string]*vmmdv1.Namespace, len(response.GetNamespaces()))
	for _, namespace := range response.GetNamespaces() {
		if namespace.GetObservedStatus() != "ready" || namespaces[namespace.GetId()] != nil {
			controller.egress.Policies.Clear()
			return errors.New("guest namespace projection is not uniquely ready")
		}
		namespaces[namespace.GetId()] = namespace
	}
	hostNamespaces := make(map[string]struct{}, len(environments))
	for _, environment := range environments {
		hostNamespaces[environment.NamespaceID] = struct{}{}
	}
	for namespaceID := range namespaces {
		if _, ok := hostNamespaces[namespaceID]; !ok {
			controller.egress.Policies.Clear()
			return fmt.Errorf("guest namespace %q has no authoritative host owner", namespaceID)
		}
	}
	sources := make([]egress.Source, 0, len(environments))
	for _, environment := range environments {
		namespace := namespaces[environment.NamespaceID]
		if namespace == nil {
			continue
		}
		subnet, parseErr := netip.ParsePrefix(namespace.GetSubnet())
		if parseErr != nil {
			controller.egress.Policies.Clear()
			return parseErr
		}
		mode := egress.DenyAll
		if environment.EgressMode == int32(hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET) {
			mode = egress.PublicInternet
		}
		sources = append(sources, egress.Source{EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID, Subnet: subnet, Mode: mode})
	}

	applianceSources, err := controller.applianceEgressSources(ctx, client, vm.GetBootId())
	if err != nil {
		controller.egress.Policies.Clear()
		return err
	}
	sources = append(sources, applianceSources...)
	return controller.egress.Policies.Publish(egress.Snapshot{BootID: vm.GetBootId(), Revision: controller.policyRevision.Add(1), Sources: sources})
}

func (controller *Controller) applianceEgressSources(ctx context.Context, client vmmdv1.VMMDServiceClient, bootID string) ([]egress.Source, error) {
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

func (controller *Controller) hostTCPInfrastructureEndpoints() []netip.AddrPort {
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
	if controller.hostFrontends != nil {
		controller.hostFrontends.mu.Lock()
		for _, lease := range controller.hostFrontends.leases {
			if lease.listener != nil {
				if endpoint, err := netip.ParseAddrPort(lease.listener.Addr().String()); err == nil {
					endpoints = append(endpoints, endpoint)
				}
			}
		}
		controller.hostFrontends.mu.Unlock()
	}
	controller.serviceListenerMu.Lock()
	for _, service := range controller.serviceListeners {
		if service.listener != nil {
			if endpoint, err := netip.ParseAddrPort(service.listener.Addr().String()); err == nil {
				endpoints = append(endpoints, endpoint)
			}
		}
	}
	controller.serviceListenerMu.Unlock()

	return endpoints
}

func (controller *Controller) hostTCPInfrastructure(target netip.AddrPort) bool {
	for _, endpoint := range controller.hostTCPInfrastructureEndpoints() {
		if endpoint.Port() == target.Port() && (endpoint.Addr().IsUnspecified() || endpoint.Addr().Unmap() == target.Addr()) {
			return true
		}
	}
	return false
}
