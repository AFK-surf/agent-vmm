//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultCoordinatorOperationTimeout = 15 * time.Minute
	defaultCoordinatorStepTimeout      = 2 * time.Minute
	defaultCoordinatorInitTimeout      = 10 * time.Minute
)

var ErrCoordinatorAdapterUnavailable = errors.New("lifecycle adapter step is unavailable")

// These actions are the local coordinator contract. The coordinator delegates
// provider-specific work to LifecycleAdapter and owns no durable state,
// retry loop, or competing lifecycle owner.
type LifecycleAction string

const (
	LifecycleInstall  LifecycleAction = "install"
	LifecycleInit     LifecycleAction = "init"
	LifecycleRegister LifecycleAction = "register"
	LifecycleStop     LifecycleAction = "stop"
	LifecycleRemove   LifecycleAction = "remove"
)

type LifecycleRequest struct {
	RequestID                  string
	Purge                      bool
	DisablePersonalMeshPairing bool
}

// LifecycleAdapter is the provider-neutral seam. Implementations must honor
// ctx and must return only after the requested local operation has settled or
// failed. The real adapter can map these methods to local operator RPCs;
// tests can inject a deterministic mock without importing a provider SDK.
type LifecycleAdapter interface {
	Install(context.Context, LifecycleRequest) error
	Init(context.Context, LifecycleRequest) error
	Register(context.Context, LifecycleRequest) error
	Stop(context.Context, LifecycleRequest) error
	Remove(context.Context, LifecycleRequest) error
}

type LifecycleCoordinatorConfig struct {
	OperationTimeout time.Duration
	StepTimeout      time.Duration
	InitTimeout      time.Duration
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
	if config.InitTimeout <= 0 {
		config.InitTimeout = defaultCoordinatorInitTimeout
	}
	return &LifecycleCoordinator{adapter: adapter, config: config}, nil
}

// Run performs one bounded contract action. Install and remove are the only
// composite actions. Install prepares the Host and VM, then applies the
// Registration. Remove stops before deleting local product state.
func (coordinator *LifecycleCoordinator) Run(ctx context.Context, action LifecycleAction, request LifecycleRequest) error {
	if ctx == nil {
		return errors.New("lifecycle context is required")
	}
	steps, err := coordinator.steps(action)
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
	return nil
}

type coordinatorStep struct {
	name LifecycleAction
	call func(LifecycleAdapter, context.Context, LifecycleRequest) error
}

func (coordinator *LifecycleCoordinator) steps(action LifecycleAction) ([]coordinatorStep, error) {
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
			step(LifecycleRegister, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Register(ctx, request)
			}),
		}, nil
	case LifecycleInit:
		return []coordinatorStep{step(LifecycleInit, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Init(ctx, request)
		})}, nil
	case LifecycleRegister:
		return []coordinatorStep{step(LifecycleRegister, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Register(ctx, request)
		})}, nil
	case LifecycleStop:
		return []coordinatorStep{step(LifecycleStop, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
			return adapter.Stop(ctx, request)
		})}, nil
	case LifecycleRemove:
		return []coordinatorStep{
			step(LifecycleStop, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Stop(ctx, request)
			}),
			step(LifecycleRemove, func(adapter LifecycleAdapter, ctx context.Context, request LifecycleRequest) error {
				return adapter.Remove(ctx, request)
			}),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported lifecycle action %q", action)
	}
}

func (coordinator *LifecycleCoordinator) runStep(ctx context.Context, name LifecycleAction,
	call func(LifecycleAdapter, context.Context, LifecycleRequest) error, request LifecycleRequest) error {
	stepTimeout := coordinator.config.StepTimeout
	if name == LifecycleInit {
		stepTimeout = coordinator.config.InitTimeout
	}
	stepContext, cancel := context.WithTimeout(ctx, stepTimeout)
	defer cancel()
	if err := call(coordinator.adapter, stepContext, request); err != nil {
		return fmt.Errorf("lifecycle %s: %w", name, err)
	}
	if err := stepContext.Err(); err != nil {
		return fmt.Errorf("lifecycle %s: %w", name, err)
	}
	return nil
}

type lifecycleStepFunc func(context.Context, LifecycleRequest) error

// lifecycleAdapterHooks lets the local helper reuse the already-owned host
// install/VM/drain/remove operations while a product supplies only the
// remote Registration call. A nil Registration hook is a
// deliberate fail-closed result, never an implicit success.
type lifecycleAdapterHooks struct {
	Register lifecycleStepFunc
}

type helperLifecycleAdapter struct {
	value     paths
	hooks     lifecycleAdapterHooks
	reconcile func(context.Context, paths, bool) error
	status    func(context.Context, paths) serviceStatus
}

func newHelperLifecycleAdapter(value paths, hooks lifecycleAdapterHooks) LifecycleAdapter {
	return &helperLifecycleAdapter{value: value, hooks: hooks, reconcile: reconcileService, status: func(ctx context.Context, value paths) serviceStatus { return readStatusContext(ctx, value) }}
}

func (adapter *helperLifecycleAdapter) Install(ctx context.Context, request LifecycleRequest) error {
	status := adapter.status(ctx, adapter.value)
	if status.HostInstalled && status.HostLoaded && status.HostReadable && status.HostHealthy {
		return nil
	}
	return adapter.reconcile(ctx, adapter.value, request.DisablePersonalMeshPairing)
}

func (adapter *helperLifecycleAdapter) Init(ctx context.Context, request LifecycleRequest) error {
	return ensureVMStartedContext(ctx, adapter.value, request.RequestID)
}

func (adapter *helperLifecycleAdapter) Register(ctx context.Context, request LifecycleRequest) error {
	return adapter.runHook(ctx, LifecycleRegister, adapter.hooks.Register, request)
}

func (adapter *helperLifecycleAdapter) Stop(ctx context.Context, _ LifecycleRequest) error {
	return drainService(ctx, adapter.value)
}

func (adapter *helperLifecycleAdapter) Remove(ctx context.Context, request LifecycleRequest) error {
	return removeService(ctx, adapter.value, request.Purge)
}

func (adapter *helperLifecycleAdapter) runHook(ctx context.Context, action LifecycleAction, hook lifecycleStepFunc, request LifecycleRequest) error {
	if hook == nil {
		return fmt.Errorf("%w: %s", ErrCoordinatorAdapterUnavailable, action)
	}
	return hook(ctx, request)
}
