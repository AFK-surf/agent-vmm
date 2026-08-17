//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type privateControllerRoute struct {
	identity  string
	cancel    context.CancelFunc
	ready     chan struct{}
	done      chan error
	attempts  chan error
	dialSlot  chan struct{}
	readyOnce sync.Once
	admitOnce sync.Once
	admitted  chan struct{}
}

func (controller *Controller) revokePrivateControllerDomain(ctx context.Context, installationID string) error {
	controller.remoteAdmissionMu.Lock()
	installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		controller.remoteAdmissionMu.Unlock()
		return err
	}
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		controller.remoteAdmissionMu.Unlock()
		return err
	}
	if registration.OwnerKind != store.RemoteRegistrationPrivate || registration.InstallationID != installation.ID {
		controller.remoteAdmissionMu.Unlock()
		return errors.New("private registration does not belong to the appliance installation")
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		if registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
			registration, err = controller.store.BeginRemoteRevocation(ctx, registration.ID, registration.Revision)
		} else if registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING) {
			// A registration that never completed enrollment has no admitted
			// controller domain to revoke. The disabled installation fence still
			// prevents future activation until a fresh lifecycle mutation.
			controller.remoteAdmissionMu.Unlock()
			return nil
		}
	}
	controller.remoteAdmissionMu.Unlock()
	if err != nil {
		return err
	}

	controller.stopApplianceFrontends(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	if err := controller.revokeRemoteLeases(registration.ID); err != nil {
		return err
	}
	if registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		return nil
	}

	controller.remoteAdmissionMu.Lock()
	defer controller.remoteAdmissionMu.Unlock()
	current, err := controller.store.GetRemoteRegistration(ctx, registration.ID)
	if err != nil {
		return err
	}
	if current.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		return nil
	}
	_, err = controller.store.CompleteRemoteRevocation(ctx, current.ID, current.Revision)
	return err
}

func (controller *Controller) preparePrivateRegistration(ctx context.Context, installation store.ApplianceInstallation) error {
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		return err
	}
	if registration.OwnerKind != store.RemoteRegistrationPrivate || registration.InstallationID != installation.ID {
		return errors.New("private registration does not belong to the appliance installation")
	}
	if registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL) {
		registration, err = controller.store.SetRemoteEnrollment(ctx, registration.ID, registration.Revision,
			int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL), int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", "")
		if err != nil {
			return err
		}
	}
	if registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return errors.New("private registration enrollment is not ready")
	}
	if registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) {
		_, err = controller.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING))
		return err
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return errors.New("private registration is revoked")
	}
	return nil
}

func (controller *Controller) activatePrivateRegistration(ctx context.Context, installation store.ApplianceInstallation) error {
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		return err
	}
	if registration.OwnerKind != store.RemoteRegistrationPrivate || registration.InstallationID != installation.ID || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return errors.New("private registration activation fence failed")
	}
	if registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) {
		return nil
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return errors.New("private registration is revoked")
	}
	_, err = controller.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED))
	return err
}

func (controller *Controller) startPrivateControllerRoute(ctx context.Context, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity) error {
	if controller.remoteComputeSocket == "" {
		return errors.New("remote compute socket is unavailable")
	}
	if err := controller.preparePrivateRegistration(ctx, installation); err != nil {
		return err
	}
	controller.stopPrivateControllerRoute(installation.ID)
	routeContext, cancel := context.WithCancel(context.Background())
	route := &privateControllerRoute{identity: privateControllerIdentity(identity), cancel: cancel, ready: make(chan struct{}), admitted: make(chan struct{}), done: make(chan error, 1), attempts: make(chan error, 1), dialSlot: make(chan struct{}, 1)}
	controller.privateControllerMu.Lock()
	controller.privateControllers[installation.ID] = route
	controller.privateControllerMu.Unlock()
	config := remoteconnector.Config{
		RegistrationID:      installation.RegistrationID,
		RemoteComputeSocket: controller.remoteComputeSocket,
		PrivateControllerDialer: func(dialContext context.Context) (*grpc.ClientConn, error) {
			return controller.dialPrivateApplianceController(dialContext, identity, route.dialSlot)
		},
		OnReady:          func() { route.readyOnce.Do(func() { close(route.ready) }) },
		CommandAdmission: route.admitted,
		OnAttemptError: func(err error) {
			select {
			case <-route.attempts:
			default:
			}
			select {
			case route.attempts <- err:
			default:
			}
		},
	}
	go func() { route.done <- remoteconnector.Run(routeContext, config) }()
	startup := time.NewTimer(30 * time.Second)
	defer startup.Stop()
	select {
	case <-route.ready:
		return nil
	case err := <-route.done:
		controller.removePrivateControllerRoute(installation.ID, route)
		if err == nil {
			err = errors.New("private controller route stopped before ready")
		}
		return err
	case <-startup.C:
		var attemptErr error
		select {
		case attemptErr = <-route.attempts:
		default:
		}
		controller.stopPrivateControllerRoute(installation.ID)
		if attemptErr == nil {
			return errors.New("private controller ready deadline elapsed without a completed connection attempt")
		}
		return fmt.Errorf("private controller ready deadline elapsed: %w", attemptErr)
	case <-ctx.Done():
		controller.stopPrivateControllerRoute(installation.ID)
		return ctx.Err()
	}
}

func (controller *Controller) admitPrivateControllerRoute(installationID string) error {
	controller.privateControllerMu.Lock()
	route := controller.privateControllers[installationID]
	controller.privateControllerMu.Unlock()
	if route == nil {
		return errors.New("private controller route is unavailable")
	}
	route.admitOnce.Do(func() { close(route.admitted) })
	return nil
}

func (controller *Controller) dialPrivateApplianceController(ctx context.Context, identity *vmmdv1.SystemApplianceIdentity, dialSlot chan struct{}) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///private-appliance-controller",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(remoteconnector.MaxControlBytes), grpc.MaxCallSendMsgSize(remoteconnector.MaxControlBytes)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Minute, Timeout: 10 * time.Second}),
		grpc.WithContextDialer(func(dialContext context.Context, _ string) (net.Conn, error) {
			select {
			case dialSlot <- struct{}{}:
			case <-dialContext.Done():
				return nil, dialContext.Err()
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			releaseSlot := func() { <-dialSlot }
			streamContext, cancelStream := context.WithCancel(ctx)
			vmmd, owner, err := controller.guestClient(streamContext)
			if err != nil {
				cancelStream()
				releaseSlot()
				return nil, err
			}
			nonce := make([]byte, 32)
			if _, err := rand.Read(nonce); err != nil {
				cancelStream()
				owner.Close()
				releaseSlot()
				return nil, err
			}
			connection, err := openSystemStreamConn(streamContext, vmmd, &vmmdv1.SystemStreamHeader{Identity: identity,
				Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER, Nonce: nonce, MaxFrameBytes: 1 << 20,
				MaxTotalBytes: 1 << 30, IdleSeconds: 300, MaxConcurrentStreams: 1, MaxUseCount: 1024})
			if err != nil {
				cancelStream()
				owner.Close()
				releaseSlot()
				return nil, err
			}
			return &ownedSystemStreamConn{Conn: connection, owner: owner, cancel: cancelStream, release: releaseSlot}, nil
		}))
}

type ownedSystemStreamConn struct {
	net.Conn
	owner   interface{ Close() error }
	cancel  context.CancelFunc
	release func()
	once    sync.Once
}

func (connection *ownedSystemStreamConn) Close() error {
	var result error
	connection.once.Do(func() {
		connection.cancel()
		result = errors.Join(connection.Conn.Close(), connection.owner.Close())
		connection.release()
	})
	return result
}

func privateControllerIdentity(identity *vmmdv1.SystemApplianceIdentity) string {
	if identity == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", identity.GetInstallationId(), identity.GetBootId(), identity.GetContainerInstanceId())
}

func (controller *Controller) removePrivateControllerRoute(installationID string, route *privateControllerRoute) {
	controller.privateControllerMu.Lock()
	if controller.privateControllers[installationID] == route {
		delete(controller.privateControllers, installationID)
	}
	controller.privateControllerMu.Unlock()
}

func (controller *Controller) stopPrivateControllerRoute(installationID string) {
	controller.privateControllerMu.Lock()
	route := controller.privateControllers[installationID]
	delete(controller.privateControllers, installationID)
	controller.privateControllerMu.Unlock()
	if route == nil {
		return
	}
	route.cancel()
	select {
	case <-route.done:
	case <-time.After(3 * time.Second):
	}
}

func (controller *Controller) stopAllPrivateControllerRoutes() {
	controller.privateControllerMu.Lock()
	routes := controller.privateControllers
	controller.privateControllers = make(map[string]*privateControllerRoute)
	controller.privateControllerMu.Unlock()
	for _, route := range routes {
		route.cancel()
	}
	for _, route := range routes {
		select {
		case <-route.done:
		case <-time.After(3 * time.Second):
		}
	}
}
