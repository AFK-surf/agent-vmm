//go:build darwin

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestStageArtifactReplaysExactSucceededRequestAfterDeadline(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server := &applianceAdminServer{controller: &Controller{store: database}}
	request := &appliancev1.StageArtifactRequest{RequestId: "stage-replay", DeadlineUnixMillis: time.Now().Add(50 * time.Millisecond).UnixMilli(), Manifest: verifierManifest()}

	first, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.StageArtifact(ctx, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(time.UnixMilli(request.GetDeadlineUnixMillis())) + time.Millisecond)
	replayed, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.StageArtifact(ctx, request)
	})
	if err != nil || replayed.GetArtifactId() != first.GetArtifactId() || replayed.GetOperation().GetOperationId() != first.GetOperation().GetOperationId() {
		t.Fatalf("expired exact replay = %+v, err=%v; first=%+v", replayed, err, first)
	}
	changed := proto.Clone(request).(*appliancev1.StageArtifactRequest)
	changed.DeadlineUnixMillis = time.Now().Add(time.Minute).UnixMilli()
	if _, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.StageArtifact(ctx, changed)
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed replay error = %v", err)
	}
}

func TestInstallReplaysExactSucceededRequestAfterDeadline(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	deadline := time.Now().Add(-time.Minute).UnixMilli()
	operation, err := database.CreateApplianceOperation(context.Background(), store.ApplianceOperation{
		ID: "install-operation", InstallationID: "installation-1", RequestID: "install-replay",
		Kind:               int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_INSTALL),
		State:              int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase:              int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason:     int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED),
		DeadlineUnixMillis: deadline, Stage: "installed",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &applianceAdminServer{controller: &Controller{store: database}}
	request := &appliancev1.InstallRequest{InstallationId: "installation-1", RequestId: "install-replay", ArtifactId: "artifact-1", DeadlineUnixMillis: deadline}

	replayed, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.Install(ctx, request)
	})
	if err != nil || replayed.GetOperation().GetOperationId() != operation.ID {
		t.Fatalf("expired exact install replay = %+v, err=%v", replayed, err)
	}
	changed := proto.Clone(request).(*appliancev1.InstallRequest)
	changed.DeadlineUnixMillis = time.Now().Add(time.Minute).UnixMilli()
	if _, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.Install(ctx, changed)
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed install replay error = %v", err)
	}
}

func callAsOperator[T any](t *testing.T, call func(context.Context) (T, error)) (T, error) {
	t.Helper()
	var zero T
	value, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return call(ctx)
	})
	if err != nil {
		return zero, err
	}
	return value.(T), nil
}
