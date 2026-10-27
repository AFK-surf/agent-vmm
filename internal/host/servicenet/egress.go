//go:build darwin || linux

package servicenet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
)

func (controller *Owner) StartEgress() error {
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

func (controller *Owner) StopEgress() {
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

func (controller *Owner) PublishSourcePoliciesLocked(ctx context.Context, excludedEnvironmentID string) error {
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
	client, err := controller.guestConnection.Client()
	if err != nil {
		controller.egress.Policies.Clear()
		return err
	}

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

	applianceSources, err := controller.sources.EgressSources(ctx, client, vm.GetBootId())
	if err != nil {
		controller.egress.Policies.Clear()
		return err
	}
	sources = append(sources, applianceSources...)
	return controller.egress.Policies.Publish(egress.Snapshot{BootID: vm.GetBootId(), Revision: controller.policyRevision.Add(1), Sources: sources})
}

func (controller *Owner) hostTCPInfrastructureEndpoints() []netip.AddrPort {
	var endpoints []netip.AddrPort
	endpoints = append(endpoints, controller.sources.HostTCPInfrastructureEndpoints()...)
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

func (controller *Owner) HostTCPInfrastructure(target netip.AddrPort) bool {
	for _, endpoint := range controller.hostTCPInfrastructureEndpoints() {
		if endpoint.Port() == target.Port() && (endpoint.Addr().IsUnspecified() || endpoint.Addr().Unmap() == target.Addr()) {
			return true
		}
	}
	return false
}
