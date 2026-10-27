//go:build darwin || linux

package workload

import (
	"context"
	"database/sql"
	"errors"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	operation "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ValidatePolicy(limits *hostv1.ResourceLimits, egress hostv1.EgressMode) error {
	if limits == nil || limits.Pids == nil || limits.DiskBytes == nil {
		return hostv1.InvalidField("limits", "pids and disk limits must be present")
	}
	if limits.GetPids() == 0 || limits.GetPids() > 1<<20 || limits.GetDiskBytes() == 0 || limits.GetDiskBytes() > 1<<50 {
		return hostv1.InvalidField("limits", "policy limits are outside supported bounds")
	}
	if egress != hostv1.EgressMode_EGRESS_MODE_DENY_ALL && egress != hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET {
		return hostv1.InvalidField("egress_mode", "egress mode must be deny-all or public-internet")
	}
	return nil
}
func (controller *Owner) PrepareLocalEnvironment(ctx context.Context, environmentID, displayName string, limits *hostv1.ResourceLimits) (store.Environment, error) {
	environment, err := controller.store.GetEnvironment(ctx, environmentID)
	if err == nil {
		if environment.OwnerKind != store.OwnerLocal {
			return store.Environment{}, status.Error(codes.FailedPrecondition, "environment is owned by a remote registration")
		}
		return environment, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.Environment{}, err
	}
	if err := ValidatePolicy(limits, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		return store.Environment{}, err
	}
	namespaceID, err := operation.NewID()
	if err != nil {
		return store.Environment{}, err
	}
	return controller.store.CreateEnvironment(ctx, store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: displayName,
		PIDs: limits.GetPids(), DiskBytes: limits.GetDiskBytes(),
		EgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL), OwnerKind: store.OwnerLocal,
	})
}
func (controller *Owner) ProjectManagedContainerIntent(ctx context.Context, environment string, container *hostv1.Container) error {
	intent, err := controller.store.GetManagedContainer(ctx, environment, container.GetId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	container.DesiredRunning = &intent.DesiredRunning
	return nil
}
