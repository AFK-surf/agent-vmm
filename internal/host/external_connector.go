//go:build darwin || linux

package host

import (
	"context"
	"crypto/x509"
	"errors"
	"net/url"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

type externalConnectorRoute struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// StartExternalConnectors reconciles every durable external registration into
// one independently supervised connector. The Host remains the composition
// root; callers never need to launch one process per controller. It first
// converts registrations stored by an older build to the running protocol
// version, so a Host update keeps the enrollment.
func (controller *Controller) StartExternalConnectors(ctx context.Context) error {
	if err := controller.store.ConvertRemoteProtocolVersion(ctx, remoteconnector.ProtocolVersion); err != nil {
		return err
	}
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
func (controller *Controller) ReconcileExternalConnectors(ctx context.Context) error {
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
		registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) &&
		registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY)
}

func (controller *Controller) ensureExternalConnector(registrationID string) {
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

func (controller *Controller) runExternalConnector(ctx context.Context, registrationID string, route *externalConnectorRoute) {
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
	if runner == nil {
		runner = remoteconnector.Run
	}
	// remoteconnector.Run owns reconnect with capped backoff and reports each
	// failure phase. It stops only on explicit credential revocation. Do not
	// wrap it in a second retry loop.
	_ = runner(ctx, config)
}

func (controller *Controller) externalConnectorConfig(registration store.RemoteRegistration) (remoteconnector.Config, error) {
	parsed, err := url.Parse("https://" + registration.Endpoint)
	if err != nil || parsed.Hostname() == "" {
		return remoteconnector.Config{}, errors.New("managed controller endpoint is invalid")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return remoteconnector.Config{}, err
	}
	if len(registration.TrustBundle) == 0 || !roots.AppendCertsFromPEM(registration.TrustBundle) {
		return remoteconnector.Config{}, errors.New("managed controller trust bundle is invalid")
	}
	return remoteconnector.Config{
		Endpoint: registration.Endpoint, EndpointIdentity: parsed.Hostname(), RegistrationID: registration.ID,
		RemoteComputeSocket: controller.remoteComputeSocket, TrustSocket: controller.trustSocket,
		TrustCapabilityFile: controller.trustCapabilityFile, CredentialStore: controller.remoteCredentials,
		DataWorkerSocket: controller.dataWorkerSocket, ServiceTunnelSocket: controller.serviceTunnelSocket,
		MeshControlSocket: controller.meshControlSocket, UseExistingDataWorker: true, TLSRoots: roots,
	}, nil
}

func (controller *Controller) stopExternalConnector(registrationID string) {
	controller.externalConnectorMu.Lock()
	route := controller.externalConnectors[registrationID]
	controller.externalConnectorMu.Unlock()
	if route != nil {
		route.cancel()
		<-route.done
	}
}

func (controller *Controller) stopAllExternalConnectors() {
	controller.externalConnectorMu.Lock()
	routes := make([]*externalConnectorRoute, 0, len(controller.externalConnectors))
	for _, route := range controller.externalConnectors {
		routes = append(routes, route)
	}
	controller.externalConnectorMu.Unlock()
	for _, route := range routes {
		route.cancel()
	}
	var wait sync.WaitGroup
	for _, route := range routes {
		wait.Add(1)
		go func(route *externalConnectorRoute) { defer wait.Done(); <-route.done }(route)
	}
	wait.Wait()
}
