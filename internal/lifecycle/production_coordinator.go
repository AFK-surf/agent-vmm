//go:build darwin || linux

package lifecycle

import (
	"context"
)

const registrationPlanMaxBytes = 8 << 20

type productionLifecycleAdapter struct {
	value      paths
	requestID  string
	plan       *registrationClaimPlan
	recovery   *registrationRecoveryDescriptor
	descriptor *registrationClaimDescriptor
	material   *registrationConfiguration
}

func newProductionLifecycleAdapter(value paths) LifecycleAdapter {
	return lifecycleAdapterFor(&productionLifecycleAdapter{value: value})
}

func lifecycleAdapterFor(adapter *productionLifecycleAdapter) LifecycleAdapter {
	return newHelperLifecycleAdapter(adapter.value, lifecycleAdapterHooks{
		Register: adapter.register,
	})
}

func (adapter *productionLifecycleAdapter) register(ctx context.Context, request LifecycleRequest) error {
	adapter.requestID = request.RequestID
	if err := adapter.ensureRegistrationPlan(ctx); err != nil {
		return err
	}
	return adapter.applyRegistration(ctx, request)
}

func runRegistrationLifecycle(adapter LifecycleAdapter, kind string, request LifecycleRequest) error {
	coordinator, err := NewLifecycleCoordinator(adapter, LifecycleCoordinatorConfig{})
	if err != nil {
		return err
	}
	action := LifecycleInstall
	if kind == "registration-recover" {
		action = LifecycleRegister
	}
	return coordinator.Run(context.Background(), action, request)
}
