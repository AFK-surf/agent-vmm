//go:build darwin || linux

package host

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestSystemRetentionUsesDurableUnfinishedUpdateAndCancellation(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	_, _, err := controller.store.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "target", CanonicalHash: []byte("target"), ProductID: "product", ImageDigest: "target-digest"}, store.ApplianceOperation{ID: "target-stage", RequestID: "target-stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := proto.Marshal(&appliancev1.ApplianceUpdateProgress{Request: &appliancev1.UpdateApplianceRequest{ArtifactId: "target"}, SourceArtifactId: installation.ArtifactID})
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "unfinished", InstallationID: installation.ID, RequestID: "unfinished", Kind: 9, State: 3, Phase: 6, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := controller.applianceImageRetention(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(projection.ManifestDigests, []string{"digest", "target-digest"}) {
		t.Fatalf("failed unfinished target lost: %v", projection)
	}
	_, err = controller.store.BeginApplianceStateMutation(ctx, installation, installation.Revision, store.ApplianceOperation{ID: "stop", RequestID: "stop", InstallationID: installation.ID, Kind: 4, State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	projection, err = controller.applianceImageRetention(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(projection.ManifestDigests, []string{"digest"}) || projection.InstallationRevision <= installation.Revision {
		t.Fatalf("cancellation retained target or lost fence: %v", projection)
	}
}

type retentionFailureGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	failed   string
	deferred bool
	observed []string
}

func (guest *retentionFailureGuest) ReconcileSystemImages(_ context.Context, request *vmmdv1.ReconcileSystemImagesRequest) (*vmmdv1.ReconcileSystemImagesResponse, error) {
	if request.InstallationId == guest.failed {
		if guest.deferred {
			value, _ := status.New(codes.Unavailable, "GC deferred").WithDetails(&errdetails.ErrorInfo{Domain: "agent-vmm", Reason: "SYSTEM_IMAGE_COLLECTION_DEFERRED", Metadata: map[string]string{"projection_applied": "true"}})
			return nil, value.Err()
		}
		return nil, status.Error(codes.Aborted, "late projection")
	}
	return &vmmdv1.ReconcileSystemImagesResponse{}, nil
}
func (guest *retentionFailureGuest) GetSystemAppliance(_ context.Context, request *vmmdv1.GetSystemApplianceRequest) (*vmmdv1.SystemApplianceResponse, error) {
	guest.observed = append(guest.observed, request.InstallationId)
	return nil, status.Error(codes.NotFound, "no guest record")
}

func TestRetentionFailureDoesNotSkipNeighborObservation(t *testing.T) {
	controller, first := updateDrainFixture(t)
	ctx := context.Background()
	grant, err := controller.store.GetApplianceGrant(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := controller.store.GetRemoteRegistration(ctx, first.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "neighbor"
	second.RegistrationID = "neighbor-registration"
	second.DataVolumeID = "neighbor-volume"
	registration.ID = second.RegistrationID
	registration.InstallationID = second.ID
	grant.InstallationID = second.ID
	second, _, _, err = controller.store.InstallApplianceDomain(ctx, second, grant, registration, nil, store.ApplianceOperation{ID: "neighbor-install", RequestID: "neighbor-install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	for _, installation := range []*store.ApplianceInstallation{&first, &second} {
		installation.RegistrationID = ""
		installation.DesiredEnabled = true
		updated, err := controller.store.UpdateApplianceInstallation(ctx, *installation, installation.Revision)
		if err != nil {
			t.Fatal(err)
		}
		*installation = updated
	}
	controller.applianceSecrets = &fileApplianceSecrets{directory: t.TempDir()}
	guest := &retentionFailureGuest{failed: first.ID}
	socketRoot, err := os.MkdirTemp("/tmp", "vmm-retention-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketRoot)
	listener, err := net.Listen("unix", filepath.Join(socketRoot, "guest.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("unix://"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	guest.deferred = true
	if err := controller.reconcileObservedApplianceCandidates(ctx, vmmdv1.NewVMMDServiceClient(connection), []store.ApplianceInstallation{first, second}, "", false); err != nil {
		t.Fatalf("applied projection with deferred GC blocked lifecycle: %v", err)
	}
	if !slices.Equal(guest.observed, []string{first.ID, second.ID}) {
		t.Fatalf("deferred GC blocked own/neighbor observation: %v", guest.observed)
	}
	guest.deferred = false
	guest.observed = nil
	err = controller.reconcileObservedApplianceCandidates(ctx, vmmdv1.NewVMMDServiceClient(connection), []store.ApplianceInstallation{first, second}, "", false)
	if err == nil || !strings.Contains(err.Error(), "late projection") {
		t.Fatalf("projection failure disappeared: %v", err)
	}
	if !slices.Equal(guest.observed, []string{first.ID, second.ID}) {
		t.Fatalf("failed projection skipped neighbor observation: %v", guest.observed)
	}
}
