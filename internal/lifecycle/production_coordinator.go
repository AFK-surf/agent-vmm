//go:build darwin || linux

package lifecycle

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

const lifecycleOperationHandoffMaxBytes = 8 << 20

type productionLifecycleAdapter struct {
	value      paths
	descriptor *operationHandoffDescriptor
	material   *operationHandoffPlan
}

func newProductionLifecycleAdapter(value paths) LifecycleAdapter {
	return lifecycleAdapterFor(&productionLifecycleAdapter{value: value})
}

func lifecycleAdapterFor(adapter *productionLifecycleAdapter) LifecycleAdapter {
	return newHelperLifecycleAdapter(adapter.value, lifecycleAdapterHooks{
		Register: adapter.register,
		Observe:  adapter.observe,
	})
}

func decodeOperationField(name, encoded string, maxBytes int) ([]byte, error) {
	if encoded == "" {
		return nil, fmt.Errorf("operation handoff %s is required", name)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode operation handoff %s: %w", name, err)
	}
	if len(decoded) == 0 || len(decoded) > maxBytes {
		return nil, fmt.Errorf("operation handoff %s has invalid size", name)
	}
	return decoded, nil
}

func (adapter *productionLifecycleAdapter) register(ctx context.Context, request LifecycleRequest) error {
	if err := adapter.ensureInstallPlan(ctx); err != nil {
		return err
	}
	return adapter.applyManagedEnrollment(ctx, request)
}

func (adapter *productionLifecycleAdapter) observe(ctx context.Context, request LifecycleRequest) (LifecycleFacts, error) {
	if adapter.material == nil {
		return LifecycleFacts{}, errManagedInstallPlanUnavailable
	}
	connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
	if err != nil {
		return LifecycleFacts{}, err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	snapshot, err := client.GetSystemHealthSummary(ctx, &hostv1.GetSystemHealthSummaryRequest{})
	if err != nil {
		return LifecycleFacts{}, err
	}
	remote, err := client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: adapter.material.RegistrationID})
	if err != nil {
		return LifecycleFacts{}, err
	}
	healthy := snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING && snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY
	registration := remote.GetRegistration()
	return LifecycleFacts{HostHealthy: healthy, RemoteAttached: healthy && registration.GetEnrollmentState() == hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY && registration.GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED}, nil
}
