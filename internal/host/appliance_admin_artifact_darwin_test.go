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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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
	manifest := verifierManifest()
	encoded, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.controller.stageBundledApplianceArtifactManifest(context.Background(), encoded, manifest.ImageManifestDigest); err != nil {
		t.Fatal(err)
	}
	request := &appliancev1.StageArtifactRequest{RequestId: "stage-replay", DeadlineUnixMillis: time.Now().Add(50 * time.Millisecond).UnixMilli(), Manifest: manifest}

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

func TestStageArtifactRequiresContentOrVerifiedBundledArtifact(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server := &applianceAdminServer{controller: &Controller{store: database}}
	manifest := verifierManifest()
	stage := func(id string) (*appliancev1.ApplianceOperationResponse, error) {
		return callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
			return server.StageArtifact(ctx, &appliancev1.StageArtifactRequest{RequestId: id, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Manifest: manifest})
		})
	}
	if _, err := stage("unverified-bundled-reference"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unverified local reference admitted: %v", err)
	}
	encoded, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.controller.stageBundledApplianceArtifactManifest(context.Background(), encoded, manifest.ImageManifestDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := stage("verified-bundled-reference"); err != nil {
		t.Fatal(err)
	}
	manifest.Release = "unverified"
	if _, err := stage("altered-bundled-manifest"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("altered bundle admitted: %v", err)
	}
	manifest.ContentRefs = []*appliancev1.ApplianceContentReference{{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, ImageReference: manifest.ImageReference, ImageManifestDigest: manifest.ImageManifestDigest, Platform: manifest.Platform, ArchiveSize: 1, ArchiveSha256: make([]byte, 32)}}
	if _, err := stage("explicit-controller-content"); err != nil {
		t.Fatal(err)
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

func TestExpiredApplianceMutationProvesNonAdmissionAfterReceiptLookup(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{store: database}
	server := &applianceAdminServer{controller: controller}
	deadline := time.Now().Add(-time.Minute).UnixMilli()
	for name, invoke := range map[string]func(context.Context) (*appliancev1.ApplianceOperationResponse, error){
		"stop": func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
			return server.Stop(ctx, &appliancev1.StopApplianceRequest{InstallationId: "installation", RequestId: "expired-stop", ExpectedRevision: 1, DeadlineUnixMillis: deadline})
		},
		"install": func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
			return server.Install(ctx, &appliancev1.InstallRequest{InstallationId: "installation", RequestId: "expired-install", ArtifactId: "artifact", DeadlineUnixMillis: deadline})
		},
		"update": func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
			return server.Update(ctx, &appliancev1.UpdateApplianceRequest{InstallationId: "installation", RequestId: "expired-update", ExpectedRevision: 1, ArtifactId: "artifact", DeadlineUnixMillis: deadline})
		},
		"stage": func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
			return server.StageArtifact(ctx, &appliancev1.StageArtifactRequest{RequestId: "expired-stage", DeadlineUnixMillis: deadline, Manifest: verifierManifest()})
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := callAsOperator(t, invoke)
			typed := false
			for _, detail := range status.Convert(err).Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "APPLIANCE_OPERATION_NOT_ADMITTED" && info.Metadata["admission"] == "not_accepted" {
					typed = true
				}
			}
			if status.Code(err) != codes.DeadlineExceeded || !typed {
				t.Fatalf("expired mutation did not prove non-admission: %v", err)
			}
		})
	}
	if _, err := database.CreateApplianceOperation(context.Background(), store.ApplianceOperation{ID: "accepted", InstallationID: "installation", RequestID: "expired-stop", Kind: 4, State: 2, Phase: 5, ExpectedRevision: 1, DeadlineUnixMillis: deadline}); err != nil {
		t.Fatal(err)
	}
	response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.Stop(ctx, &appliancev1.StopApplianceRequest{InstallationId: "installation", RequestId: "expired-stop", ExpectedRevision: 1, DeadlineUnixMillis: deadline})
	})
	if err != nil || response.GetOperation().GetOperationId() != "accepted" {
		t.Fatal("expired accepted intent was not replayed", response, err)
	}
	lock := controller.integrationLock("appliance-lifecycle:installation")
	lock.Lock()
	go func() { time.Sleep(25 * time.Millisecond); lock.Unlock() }()
	_, err = callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		bounded, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		return server.Stop(bounded, &appliancev1.StopApplianceRequest{InstallationId: "installation", RequestId: "busy-stop", ExpectedRevision: 1, DeadlineUnixMillis: deadline})
	})
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Metadata["admission"] == "not_accepted" {
			t.Fatal("busy admission invented non-admission", err)
		}
	}
}
