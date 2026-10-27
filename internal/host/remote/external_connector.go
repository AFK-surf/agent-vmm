//go:build darwin || linux

package remote

import (
	"context"
	"crypto/x509"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

type externalConnectorRoute struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// StartExternalConnectors starts one supervised connector for each external registration.
func (controller *Owner) StartExternalConnectors(ctx context.Context) error {
	controller.externalConnectorMu.Lock()
	if controller.externalConnectorContext == nil || controller.externalConnectorContext.Err() != nil {
		controller.externalConnectorContext = ctx
	}
	controller.externalConnectorMu.Unlock()
	return controller.ReconcileExternalConnectors(ctx)
}

// ReconcileExternalConnectors restarts every durable desired connector after
// the VM has been stopped and started without replacing the Host lifetime
// context with the bounded Operator RPC context.
func (controller *Owner) ReconcileExternalConnectors(ctx context.Context) error {
	for cursor := ""; ; {
		registrations, err := controller.store.ListRemoteRegistrationPage(ctx, cursor, 32)
		if err != nil {
			return err
		}
		if len(registrations) == 0 {
			return nil
		}
		for _, registration := range registrations {
			cursor = registration.ID
			if externalRegistrationDesired(registration) {
				controller.ensureExternalConnector(registration.ID)
			}
		}
	}
}

func externalRegistrationDesired(registration store.RemoteRegistration) bool {
	return registration.OwnerKind == store.RemoteRegistrationExternal &&
		(registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING)) &&
		registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY)
}

func (controller *Owner) ensureExternalConnector(registrationID string) {
	controller.externalConnectorMu.Lock()
	defer controller.externalConnectorMu.Unlock()
	if controller.externalConnectorContext == nil || controller.externalConnectorContext.Err() != nil || controller.externalConnectors[registrationID] != nil {
		return
	}
	routeContext, cancel := context.WithCancel(controller.externalConnectorContext)
	route := &externalConnectorRoute{cancel: cancel, done: make(chan struct{})}
	controller.externalConnectors[registrationID] = route
	go controller.runExternalConnector(routeContext, registrationID, route)
}

func (controller *Owner) runExternalConnector(ctx context.Context, registrationID string, route *externalConnectorRoute) {
	defer func() {
		controller.externalConnectorMu.Lock()
		if controller.externalConnectors[registrationID] == route {
			delete(controller.externalConnectors, registrationID)
		}
		controller.externalConnectorMu.Unlock()
		close(route.done)
	}()
	registration, err := controller.store.GetRemoteRegistration(ctx, registrationID)
	if err != nil || !externalRegistrationDesired(registration) {
		return
	}
	config, err := controller.externalConnectorConfig(registration)
	if err != nil {
		_, _ = controller.store.ReportConnectorStatus(ctx, registrationID, "",
			int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR), "", time.Now(), time.Time{}, 0,
			"CONFIG_INVALID", "Managed connector configuration is invalid.")
		return
	}
	runner := controller.externalConnectorRunner
	// Run returns only when ctx ends. The supervisor cancels ctx when the registration is no longer desired.
	_ = runner(ctx, config)
}

func (controller *Owner) externalConnectorConfig(registration store.RemoteRegistration) (remoteconnector.Config, error) {
	roots := x509.NewCertPool()
	if len(registration.TrustBundle) == 0 || !roots.AppendCertsFromPEM(registration.TrustBundle) {
		return remoteconnector.Config{}, errors.New("managed controller trust bundle is invalid")
	}
	return remoteconnector.Config{
		RegistrationID:      registration.ID,
		RemoteComputeSocket: controller.remoteComputeSocket, Transport: remoteconnector.ExternalTransport{Endpoint: registration.Endpoint, Credentials: controller.remoteCredentials, Roots: roots, Trust: &remoteconnector.TrustBoundary{Socket: controller.trustSocket}, Worker: &remoteconnector.WorkerSockets{Socket: controller.dataWorkerSocket, ServiceTunnelSocket: controller.serviceTunnelSocket, MeshControlSocket: controller.meshControlSocket}},
	}, nil
}

func (controller *Owner) StopExternalConnector(registrationID string) {
	controller.externalConnectorMu.Lock()
	route := controller.externalConnectors[registrationID]
	controller.externalConnectorMu.Unlock()
	if route != nil {
		route.cancel()
		<-route.done
	}
}

func (controller *Owner) StopAllExternalConnectors() {
	controller.externalConnectorMu.Lock()
	routes := make([]*externalConnectorRoute, 0, len(controller.externalConnectors))
	for _, route := range controller.externalConnectors {
		routes = append(routes, route)
	}
	controller.externalConnectorMu.Unlock()
	for _, route := range routes {
		route.cancel()
	}
	for _, route := range routes {
		<-route.done
	}
}
