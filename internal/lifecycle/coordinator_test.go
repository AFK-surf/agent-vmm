//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type coordinatorMock struct {
	calls       []string
	requests    []LifecycleRequest
	facts       LifecycleFacts
	observeErr  error
	failAction  LifecycleAction
	blockAction LifecycleAction
}

func (mock *coordinatorMock) step(ctx context.Context, action LifecycleAction, request LifecycleRequest) error {
	mock.calls = append(mock.calls, string(action))
	mock.requests = append(mock.requests, request)
	if mock.blockAction == action {
		<-ctx.Done()
		return ctx.Err()
	}
	if mock.failAction == action {
		return errors.New("mock step failed")
	}
	return nil
}

func (mock *coordinatorMock) Install(ctx context.Context, request LifecycleRequest) error {
	return mock.step(ctx, LifecycleInstall, request)
}

func (mock *coordinatorMock) Init(ctx context.Context, request LifecycleRequest) error {
	return mock.step(ctx, LifecycleInit, request)
}

func (mock *coordinatorMock) Register(ctx context.Context, request LifecycleRequest) error {
	return mock.step(ctx, LifecycleRegister, request)
}

func (mock *coordinatorMock) Stop(ctx context.Context, request LifecycleRequest) error {
	return mock.step(ctx, LifecycleStop, request)
}

func (mock *coordinatorMock) Remove(ctx context.Context, request LifecycleRequest) error {
	return mock.step(ctx, LifecycleRemove, request)
}

func (mock *coordinatorMock) Observe(_ context.Context, request LifecycleRequest) (LifecycleFacts, error) {
	mock.calls = append(mock.calls, "observe")
	mock.requests = append(mock.requests, request)
	return mock.facts, mock.observeErr
}

func newTestCoordinator(t *testing.T, mock *coordinatorMock, operationTimeout, stepTimeout time.Duration) *LifecycleCoordinator {
	t.Helper()
	coordinator, err := NewLifecycleCoordinator(mock, LifecycleCoordinatorConfig{
		OperationTimeout: operationTimeout,
		StepTimeout:      stepTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func TestLifecycleCoordinatorInstallRunsBoundedForwardContract(t *testing.T) {
	mock := &coordinatorMock{facts: LifecycleFacts{RemoteAttached: true}}
	coordinator := newTestCoordinator(t, mock, time.Second, time.Second)
	request := LifecycleRequest{RequestID: "request-1"}

	if err := coordinator.Run(context.Background(), LifecycleInstall, request); err != nil {
		t.Fatal(err)
	}
	if want := []string{"install", "init", "register", "observe"}; !reflect.DeepEqual(mock.calls, want) {
		t.Fatalf("calls=%v want=%v", mock.calls, want)
	}
	for index, observed := range mock.requests {
		if observed != request {
			t.Fatalf("request[%d]=%+v want=%+v", index, observed, request)
		}
	}
}

func TestLifecycleCoordinatorDoesNotDeriveControllerReadinessFromHealth(t *testing.T) {
	mock := &coordinatorMock{facts: LifecycleFacts{
		HostHealthy:    true,
		RemoteAttached: false,
	}}
	coordinator := newTestCoordinator(t, mock, time.Second, time.Second)

	err := coordinator.Run(context.Background(), LifecycleInstall, LifecycleRequest{RequestID: "request-2"})
	if !errors.Is(err, ErrCoordinatorNotReady) {
		t.Fatalf("error=%v want ErrCoordinatorNotReady", err)
	}
	if got, want := mock.calls[len(mock.calls)-1], "observe"; got != want {
		t.Fatalf("last call=%q want=%q", got, want)
	}
}

func TestLifecycleCoordinatorRemoveStopsBeforeRemoving(t *testing.T) {
	mock := &coordinatorMock{}
	coordinator := newTestCoordinator(t, mock, time.Second, time.Second)
	request := LifecycleRequest{RequestID: "request-3", Purge: true}

	if err := coordinator.Run(context.Background(), LifecycleRemove, request); err != nil {
		t.Fatal(err)
	}
	if want := []string{"stop", "remove"}; !reflect.DeepEqual(mock.calls, want) {
		t.Fatalf("calls=%v want=%v", mock.calls, want)
	}
}

func TestLifecycleCoordinatorExposesEverySingleStepAction(t *testing.T) {
	for _, action := range []LifecycleAction{
		LifecycleInit, LifecycleRegister, LifecycleStop,
	} {
		t.Run(string(action), func(t *testing.T) {
			mock := &coordinatorMock{}
			coordinator := newTestCoordinator(t, mock, time.Second, time.Second)
			if err := coordinator.Run(context.Background(), action, LifecycleRequest{RequestID: "request-single"}); err != nil {
				t.Fatal(err)
			}
			if want := []string{string(action)}; !reflect.DeepEqual(mock.calls, want) {
				t.Fatalf("calls=%v want=%v", mock.calls, want)
			}
		})
	}
}

func TestLifecycleCoordinatorBoundsAStuckStep(t *testing.T) {
	mock := &coordinatorMock{blockAction: LifecycleRegister}
	coordinator := newTestCoordinator(t, mock, 200*time.Millisecond, 30*time.Millisecond)

	started := time.Now()
	err := coordinator.Run(context.Background(), LifecycleInstall, LifecycleRequest{RequestID: "request-4"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stuck step exceeded bounded test window: %s", elapsed)
	}
	if want := []string{"install", "init", "register"}; !reflect.DeepEqual(mock.calls, want) {
		t.Fatalf("calls=%v want=%v", mock.calls, want)
	}
}

func TestLifecycleCoordinatorUsesIndependentInitTimeout(t *testing.T) {
	mock := &coordinatorMock{blockAction: LifecycleInit}
	coordinator, err := NewLifecycleCoordinator(mock, LifecycleCoordinatorConfig{
		OperationTimeout: time.Second,
		StepTimeout:      500 * time.Millisecond,
		InitTimeout:      20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = coordinator.Run(context.Background(), LifecycleInit, LifecycleRequest{RequestID: "request-init-timeout"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
		t.Fatalf("init used the general step timeout: %s", elapsed)
	}
}

func TestLifecycleCoordinatorRejectsMissingAdapter(t *testing.T) {
	if _, err := NewLifecycleCoordinator(nil, LifecycleCoordinatorConfig{}); err == nil {
		t.Fatal("expected missing adapter error")
	}
	adapter := newHelperLifecycleAdapter(testPaths(t.TempDir()), lifecycleAdapterHooks{})
	if err := adapter.Register(context.Background(), LifecycleRequest{}); !errors.Is(err, ErrCoordinatorAdapterUnavailable) {
		t.Fatalf("error=%v want ErrCoordinatorAdapterUnavailable", err)
	}
}

func TestManagedLifecycleAdapterDoesNotReinstallHealthyHost(t *testing.T) {
	adapter := newHelperLifecycleAdapter(testPaths(t.TempDir()), lifecycleAdapterHooks{}).(*helperLifecycleAdapter)
	adapter.status = func(context.Context, paths) serviceStatus {
		return serviceStatus{HostInstalled: true, HostLoaded: true, HostReadable: true, HostHealthy: true}
	}
	adapter.reconcile = func(context.Context, paths, bool) error {
		t.Fatal("healthy Host must not mutate launchd")
		return nil
	}
	if err := adapter.Install(context.Background(), LifecycleRequest{DisablePersonalMeshPairing: true}); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
}

func TestLifecycleCoordinatorRejectsUnknownAction(t *testing.T) {
	coordinator := newTestCoordinator(t, &coordinatorMock{}, time.Second, time.Second)
	if err := coordinator.Run(context.Background(), LifecycleAction("unknown"), LifecycleRequest{}); err == nil {
		t.Fatal("expected unknown action error")
	}
}
