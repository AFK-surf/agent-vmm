package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultCoordinatorOperationTimeout = 5 * time.Minute
	defaultCoordinatorStepTimeout      = 2 * time.Minute
)

var (
	ErrCoordinatorAdapterUnavailable = errors.New("lifecycle adapter step is unavailable")
	ErrCoordinatorNotReady           = errors.New("Salix readiness was not observed")
)

// These actions are the local coordinator contract. The coordinator delegates
// provider-specific work to LifecycleAdapter and owns no durable state,
// retry loop, or competing lifecycle owner.
type LifecycleAction string

const (
	LifecycleInstall          LifecycleAction = "install"
	LifecycleInit             LifecycleAction = "init"
	LifecycleApplianceInstall LifecycleAction = "appliance-install"
	LifecycleBootstrap        LifecycleAction = "bootstrap"
	LifecycleRegister         LifecycleAction = "register"
	LifecycleReconnect        LifecycleAction = "reconnect"
	LifecycleStop             LifecycleAction = "stop"
	LifecycleRemove           LifecycleAction = "remove"
)

type LifecycleRequest struct {
	RequestID                  string
	InstallationID             string
	ExpectedRevision           uint64
	Purge                      bool
	DisablePersonalMeshPairing bool
}

// LifecycleFacts are observations from the local operator/appliance boundary.
// RemoteAttached means the exact managed registration is enrolled and its
// connector is connected. It is not Salix admission or product Node readiness.
type LifecycleFacts struct {
	HostHealthy       bool
	ApplianceHealthy  bool
	ControllerActive  bool
	InventoryComplete bool
	RemoteAttached    bool
}

// LifecycleAdapter is the provider-neutral seam. Implementations must honor
// ctx and must return only after the requested local operation has settled or
// failed. The real adapter can map these methods to local operator RPCs;
// tests can inject a deterministic mock without importing a provider SDK.
type LifecycleAdapter interface {
	Install(context.Context, LifecycleRequest) error
	Init(context.Context, LifecycleRequest) error
	ApplianceInstall(context.Context, LifecycleRequest) error
	Bootstrap(context.Context, LifecycleRequest) error
	Register(context.Context, LifecycleRequest) error
	Reconnect(context.Context, LifecycleRequest) error
	Stop(context.Context, LifecycleRequest) error
	Remove(context.Context, LifecycleRequest) error
	Observe(context.Context, LifecycleRequest) (LifecycleFacts, error)
}

type LifecycleCoordinatorConfig struct {
	OperationTimeout time.Duration
	StepTimeout      time.Duration
}

type LifecycleCoordinator struct {
	adapter LifecycleAdapter
	config  LifecycleCoordinatorConfig
}

func NewLifecycleCoordinator(adapter LifecycleAdapter, config LifecycleCoordinatorConfig) (*LifecycleCoordinator, error) {
	if adapter == nil {
		return nil, errors.New("lifecycle adapter is required")
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = defaultCoordinatorOperationTimeout
	}
	if config.StepTimeout <= 0 {
		config.StepTimeout = defaultCoordinatorStepTimeout
	}
	return &LifecycleCoordinator{adapter: adapter, config: config}, nil
}

// Run performs one bounded contract action. Install and remove are the only
// composite actions: installation moves forward through every admission
// boundary, while removal stops before deleting local product state.
func (coordinator *LifecycleCoordinator) Run(ctx context.Context, action LifecycleAction, request LifecycleRequest) error {
	if ctx == nil {
		return errors.New("lifecycle context is required")
	}
	steps, requireReady, err := coordinator.steps(action)
	if err != nil {
		return err
	}
	operationContext, cancel := context.WithTimeout(ctx, coordinator.config.OperationTimeout)
	defer cancel()
	for _, step := range steps {
		if err := coordinator.runStep(operationContext, step.name, step.call, request); err != nil {
			return err
		}
	}
	if !requireReady {
		return nil
	}
	facts, err := coordinator.observe(operationContext, request)
	if err != nil {
		return err
	}
	if !facts.RemoteAttached {
		return fmt.Errorf("%w: hostHealthy=%t applianceHealthy=%t controllerActive=%t inventoryComplete=%t",
			ErrCoordinatorNotReady, facts.HostHealthy, facts.ApplianceHealthy, facts.ControllerActive, facts.InventoryComplete)
	}
	return nil
}

type coordinatorStep struct {
	name LifecycleAction
	call func(LifecycleAdapter, context.Context, LifecycleRequest) error
}

func (coordinator *LifecycleCoordinator) steps(action LifecycleAction) ([]coordinatorStep, bool, error) {
	step := func(name LifecycleAction, method func(LifecycleAdapter, context.Context, LifecycleRequest) error) coordinatorStep {
		return coordinatorStep{name: name, call: method}
	}
	switch action {
	case LifecycleInstall:
		return []coordinatorStep{
			step(LifecycleInstall, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Install(ctx, request)
			}),
			step(LifecycleInit, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Init(ctx, request)
			}),
			step(LifecycleApplianceInstall, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.ApplianceInstall(ctx, request)
			}),
			step(LifecycleBootstrap, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Bootstrap(ctx, request)
			}),
			step(LifecycleRegister, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Register(ctx, request)
			}),
			step(LifecycleReconnect, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Reconnect(ctx, request)
			}),
		}, true, nil
	case LifecycleInit:
		return []coordinatorStep{step(LifecycleInit, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Init(ctx, request)
		})}, false, nil
	case LifecycleApplianceInstall:
		return []coordinatorStep{step(LifecycleApplianceInstall, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.ApplianceInstall(ctx, request)
		})}, false, nil
	case LifecycleBootstrap:
		return []coordinatorStep{step(LifecycleBootstrap, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Bootstrap(ctx, request)
		})}, false, nil
	case LifecycleRegister:
		return []coordinatorStep{step(LifecycleRegister, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Register(ctx, request)
		})}, false, nil
	case LifecycleReconnect:
		return []coordinatorStep{step(LifecycleReconnect, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Reconnect(ctx, request)
		})}, false, nil
	case LifecycleStop:
		return []coordinatorStep{step(LifecycleStop, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Stop(ctx, request)
		})}, false, nil
	case LifecycleRemove:
		return []coordinatorStep{
			step(LifecycleStop, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Stop(ctx, request)
			}),
			step(LifecycleRemove, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Remove(ctx, request)
			}),
		}, false, nil
	default:
		return nil, false, fmt.Errorf("unsupported lifecycle action %q", action)
	}
}

func (coordinator *LifecycleCoordinator) runStep(ctx context.Context, name LifecycleAction,
	call func(LifecycleAdapter, context.Context, LifecycleRequest) error, request LifecycleRequest) error {
	stepContext, cancel := context.WithTimeout(ctx, coordinator.config.StepTimeout)
	defer cancel()
	if err := call(coordinator.adapter, stepContext, request); err != nil {
		return fmt.Errorf("lifecycle %s: %w", name, err)
	}
	if err := stepContext.Err(); err != nil {
		return fmt.Errorf("lifecycle %s: %w", name, err)
	}
	return nil
}

func (coordinator *LifecycleCoordinator) observe(ctx context.Context, request LifecycleRequest) (LifecycleFacts, error) {
	observeContext, cancel := context.WithTimeout(ctx, coordinator.config.StepTimeout)
	defer cancel()
	facts, err := coordinator.adapter.Observe(observeContext, request)
	if err != nil {
		return LifecycleFacts{}, fmt.Errorf("lifecycle observe: %w", err)
	}
	if err := observeContext.Err(); err != nil {
		return LifecycleFacts{}, fmt.Errorf("lifecycle observe: %w", err)
	}
	return facts, nil
}

type lifecycleStepFunc func(context.Context, LifecycleRequest) error
type lifecycleObserveFunc func(context.Context, LifecycleRequest) (LifecycleFacts, error)

// lifecycleAdapterHooks lets the local helper reuse the already-owned host
// install/VM/drain/remove operations while a product supplies only the
// provider-specific appliance and controller calls. A nil appliance hook is a
// deliberate fail-closed result, never an implicit success.
type lifecycleAdapterHooks struct {
	ApplianceInstall lifecycleStepFunc
	Bootstrap        lifecycleStepFunc
	Register         lifecycleStepFunc
	Reconnect        lifecycleStepFunc
	Observe          lifecycleObserveFunc
}

type helperLifecycleAdapter struct {
	value     paths
	hooks     lifecycleAdapterHooks
	launchctl func(context.Context, ...string) error
	status    func(context.Context, paths) serviceStatus
}

func newHelperLifecycleAdapter(value paths, hooks lifecycleAdapterHooks) LifecycleAdapter {
	return &helperLifecycleAdapter{value: value, hooks: hooks, launchctl: launchctlContext, status: readStatusContext}
}

func (adapter *helperLifecycleAdapter) Install(ctx context.Context, request LifecycleRequest) error {
	status := adapter.status(ctx, adapter.value)
	if status.HostInstalled && status.HostLoaded && status.HostReadable && status.HostHealthy {
		return nil
	}
	return repairWithOptionsContext(ctx, adapter.value, adapter.launchctl, request.DisablePersonalMeshPairing)
}

func (adapter *helperLifecycleAdapter) Init(ctx context.Context, request LifecycleRequest) error {
	return ensureVMStartedContext(ctx, adapter.value, request.RequestID)
}

func (adapter *helperLifecycleAdapter) ApplianceInstall(ctx context.Context, request LifecycleRequest) error {
	return adapter.runHook(ctx, LifecycleApplianceInstall, adapter.hooks.ApplianceInstall, request)
}

func (adapter *helperLifecycleAdapter) Bootstrap(ctx context.Context, request LifecycleRequest) error {
	return adapter.runHook(ctx, LifecycleBootstrap, adapter.hooks.Bootstrap, request)
}

func (adapter *helperLifecycleAdapter) Register(ctx context.Context, request LifecycleRequest) error {
	return adapter.runHook(ctx, LifecycleRegister, adapter.hooks.Register, request)
}

func (adapter *helperLifecycleAdapter) Reconnect(ctx context.Context, request LifecycleRequest) error {
	return adapter.runHook(ctx, LifecycleReconnect, adapter.hooks.Reconnect, request)
}

func (adapter *helperLifecycleAdapter) Stop(ctx context.Context, _ LifecycleRequest) error {
	return drainWithContext(ctx, adapter.value, adapter.launchctl)
}

func (adapter *helperLifecycleAdapter) Remove(ctx context.Context, request LifecycleRequest) error {
	return uninstallWithContext(ctx, adapter.value, request.Purge, adapter.launchctl)
}

func (adapter *helperLifecycleAdapter) Observe(ctx context.Context, request LifecycleRequest) (LifecycleFacts, error) {
	if adapter.hooks.Observe != nil {
		return adapter.hooks.Observe(ctx, request)
	}
	if err := ctx.Err(); err != nil {
		return LifecycleFacts{}, err
	}
	status := readStatusContext(ctx, adapter.value)
	if err := ctx.Err(); err != nil {
		return LifecycleFacts{}, err
	}
	return LifecycleFacts{
		HostHealthy:       status.HostHealthy,
		ApplianceHealthy:  status.ApplianceHealthy,
		ControllerActive:  status.ControllerActive,
		InventoryComplete: status.InventoryComplete,
		RemoteAttached:    false,
	}, nil
}

func (adapter *helperLifecycleAdapter) runHook(ctx context.Context, action LifecycleAction, hook lifecycleStepFunc, request LifecycleRequest) error {
	if hook == nil {
		return fmt.Errorf("%w: %s", ErrCoordinatorAdapterUnavailable, action)
	}
	return hook(ctx, request)
}
