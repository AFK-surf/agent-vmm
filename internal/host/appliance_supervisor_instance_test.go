//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type failingApplianceImportClient struct {
	vmmdv1.VMMDServiceClient
	imports int
}

func (client *failingApplianceImportClient) ImportImage(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[vmmdv1.ImportImageRequest, vmmdv1.Image], error) {
	client.imports++
	return nil, errors.New("guest import unavailable")
}

func TestBundledApplianceArtifactStagesWithoutChangingInstallations(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	oldManifest := verifierManifest()
	oldManifest.ProductId = "agent-vmm-mcp"
	oldManifest.ImageReference = "agent-vmm.local/mcp@" + oldManifest.GetImageManifestDigest()
	oldManifest.RequestedCapacity.MaxEgressMode = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
	oldCanonical, oldHash, err := (applianceArtifactVerifier{}).verify(oldManifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldArtifact, _, err := database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "old-artifact", CanonicalHash: oldHash,
		ImageDigest: oldManifest.GetImageManifestDigest(), ImageReference: oldManifest.GetImageReference(), Platform: oldManifest.GetPlatform(),
		ProductID: oldManifest.GetProductId(), Release: oldManifest.GetRelease(), Manifest: oldCanonical},
		store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	newManifest := proto.Clone(oldManifest).(*appliancev1.ControllerApplianceManifest)
	newManifest.Release = "2"
	newManifest.ImageManifestDigest = "sha256:" + strings.Repeat("f", 64)
	newManifest.ImageReference = "agent-vmm.local/mcp@" + newManifest.GetImageManifestDigest()
	newManifest.RequestedFrontends = nil
	encoded, hash, err := (applianceArtifactVerifier{}).verify(newManifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	if err := controller.stageBundledApplianceArtifactManifest(ctx, encoded, "sha256:"+strings.Repeat("e", 64)); err == nil {
		t.Fatal("manifest with a different Guest image lock was accepted")
	}
	if _, err := database.GetApplianceArtifactByCanonicalHash(ctx, hash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected bundle was staged: %v", err)
	}
	if err := controller.stageBundledApplianceArtifactManifest(ctx, encoded, newManifest.GetImageManifestDigest()); err != nil {
		t.Fatal(err)
	}
	staged, err := database.GetApplianceArtifactByCanonicalHash(ctx, hash)
	if err != nil || staged.ImageDigest != newManifest.GetImageManifestDigest() {
		t.Fatalf("staged bundle=%+v err=%v", staged, err)
	}
	approved := verifierGrant()
	approved.Capacity.MaxEgressMode = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
	before := make([]store.ApplianceInstallation, 0, 2)
	grants := make(map[string]store.ApplianceGrant)
	frontendsBefore := make(map[string][]store.ApplianceFrontend)
	for index, state := range []appliancev1.ApplianceState{appliancev1.ApplianceState_APPLIANCE_STATE_FAILED, appliancev1.ApplianceState_APPLIANCE_STATE_SEALED} {
		id := fmt.Sprintf("installation-%d", index)
		installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: id, ProductID: oldArtifact.ProductID,
			ArtifactID: oldArtifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
			DataVolumeID: "volume-" + id, DataVolumeBytes: 1 << 30}, grantFromProto(id, 3, approved),
			store.RemoteRegistration{ID: "registration-" + id, DisplayName: "private", ProtocolVersion: "1",
				PerEnvPIDs: 2, PerEnvDiskBytes: 2, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
				EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: store.RemoteRegistrationPrivate, InstallationID: id},
			frontendsFromProto(approved.GetFrontends()), store.ApplianceOperation{ID: "install-" + id, RequestID: "install-" + id, State: 2, Phase: 5})
		if err != nil {
			t.Fatal(err)
		}
		installation.DesiredEnabled = index == 0
		installation.State = int32(state)
		installation.CurrentBootID = "old-boot"
		installation.CurrentContainerInstance = "old-instance"
		installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
		if err != nil {
			t.Fatal(err)
		}
		before = append(before, installation)
		grants[id], err = database.GetApplianceGrant(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		frontendsBefore[id], err = database.ListApplianceFrontends(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := controller.stageBundledApplianceArtifactManifest(ctx, encoded, newManifest.GetImageManifestDigest()); err != nil {
		t.Fatal(err)
	}
	repeated, err := database.GetApplianceArtifactByCanonicalHash(ctx, hash)
	if err != nil || !reflect.DeepEqual(repeated, staged) {
		t.Fatalf("bundle staging was not idempotent: before=%+v after=%+v err=%v", staged, repeated, err)
	}
	after, err := database.ListApplianceInstallations(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("bundle staging changed installations: before=%+v after=%+v err=%v", before, after, err)
	}
	for _, installation := range before {
		grant, err := database.GetApplianceGrant(ctx, installation.ID)
		if err != nil || !reflect.DeepEqual(grant, grants[installation.ID]) {
			t.Fatalf("bundle staging changed grant: %+v %v", grant, err)
		}
		frontends, err := database.ListApplianceFrontends(ctx, installation.ID)
		if err != nil || !reflect.DeepEqual(frontends, frontendsBefore[installation.ID]) {
			t.Fatalf("bundle staging changed frontends: %+v %v", frontends, err)
		}
	}
}

func TestBootstrapReceiptUsesExactSingleUseIdentity(t *testing.T) {
	receipt := &appliancev1.BootstrapReceipt{InstallationId: "installation", BootstrapId: "bootstrap", Nonce: []byte("nonce")}
	if !validateBootstrapReceipt(receipt, "installation", "bootstrap", []byte("nonce")) {
		t.Fatal("exact bootstrap receipt was rejected")
	}
	for _, changed := range []*appliancev1.BootstrapReceipt{
		{InstallationId: "peer", BootstrapId: "bootstrap", Nonce: []byte("nonce")},
		{InstallationId: "installation", BootstrapId: "stale", Nonce: []byte("nonce")},
		{InstallationId: "installation", BootstrapId: "bootstrap", Nonce: []byte("other")},
	} {
		if validateBootstrapReceipt(changed, "installation", "bootstrap", []byte("nonce")) {
			t.Fatalf("stale receipt was accepted: %+v", changed)
		}
	}
}

func TestBootstrapFailureReasonIsBoundedAndStable(t *testing.T) {
	for _, test := range []struct{ message, want string }{
		{"rpc error: launch claim lifetime exceeds the fixed bound", "BOOTSTRAP_CLAIM_LIFETIME_INVALID"},
		{"rpc error: bootstrap envelope identity is invalid", "BOOTSTRAP_ENVELOPE_IDENTITY_INVALID"},
		{"transport failed with an implementation detail", "BOOTSTRAP_FAILED"},
	} {
		if got := bootstrapFailureReason(errors.New(test.message)); got != test.want {
			t.Fatalf("bootstrapFailureReason(%q) = %q, want %q", test.message, got, test.want)
		}
	}
}

func TestApplianceReconcileRejectsStaleObservedInstance(t *testing.T) {
	manifest, err := proto.Marshal(verifierManifest())
	if err != nil {
		t.Fatal(err)
	}
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, store.ApplianceArtifact{
		ID: "artifact", CanonicalHash: []byte("canonical"), ImageDigest: "sha256:digest", ImageReference: "registry.example/appliance@sha256:digest",
		Platform: "linux/arm64", ProductID: "product", Release: "1",
		Manifest: manifest,
	}, store.ApplianceOperation{ID: "stage-operation", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := database.InstallApplianceDomain(ctx,
		store.ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID, ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1 << 20},
		store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1},
		store.RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "installation"},
		nil, store.ApplianceOperation{ID: "install-operation", RequestID: "install", State: 2, Phase: 5}); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	stale := store.ApplianceInstallation{ID: "installation", State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING), Revision: 1, DesiredEnabled: true}
	runtime := &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "installation", BootId: "new-boot", ContainerInstanceId: "new-instance"}, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}
	if err := controller.adoptObservedAppliance(ctx, nil, stale, runtime); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetApplianceInstallation(ctx, "installation")
	if err != nil {
		t.Fatal(err)
	}
	if current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || current.CurrentContainerInstance != "" {
		t.Fatalf("stale observation mutated installation: %+v", current)
	}
}

func TestPrepareMissingApplianceRecoveryAdvancesFenceOnce(t *testing.T) {
	manifest, err := proto.Marshal(recoveryContentManifest())
	if err != nil {
		t.Fatal(err)
	}
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, store.ApplianceArtifact{
		ID: "artifact", CanonicalHash: []byte("canonical"), ImageDigest: "sha256:digest", ImageReference: "agent-vmm.local/mcp@sha256:digest",
		Platform: "linux/arm64", ProductID: "product", Release: "1", Manifest: manifest,
	}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	amount := store.ResourceAmount{PIDs: 1, DiskBytes: 1}
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID,
		ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
		DataVolumeID: "volume", DataVolumeBytes: 1 << 30},
		store.ApplianceGrant{Revision: 1, System: amount, PerEnvironment: amount,
			EgressMode: 1, MaxEgressMode: 1},
		store.RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1",
			PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
			StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "installation"},
		nil, store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	installation.CurrentBootID = "missing-boot"
	installation.CurrentContainerInstance = "missing-instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	support := t.TempDir()
	controller := &Controller{store: database, egress: egress.New(), supportDirectory: support, applianceHealth: make(map[string]*appliancev1.ApplianceHealth), sessions: make(map[string]*sessionServer)}
	prepared, recover, err := controller.prepareApplianceRecoveryLocked(ctx, installation)
	if err != nil {
		t.Fatal(err)
	}
	if !recover || prepared.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || prepared.Revision != installation.Revision+1 ||
		prepared.CurrentBootID != "" || prepared.CurrentContainerInstance != "" || prepared.ArtifactID != installation.ArtifactID ||
		prepared.RegistrationID != installation.RegistrationID || prepared.DataVolumeID != installation.DataVolumeID {
		t.Fatalf("missing runtime was not prepared for bounded recovery: before=%+v after=%+v recover=%t", installation, prepared, recover)
	}
	again, recover, err := controller.prepareApplianceRecoveryLocked(ctx, installation)
	if err != nil || recover || again.Revision != prepared.Revision {
		t.Fatalf("stale reconcile advanced recovery fence twice: after=%+v recover=%t err=%v", again, recover, err)
	}
	stageRecoveryContent(t, controller, artifact)
	client := new(failingApplianceImportClient)
	if err := controller.recoverApplianceRuntime(ctx, client, prepared, nil); err == nil {
		t.Fatal("bounded recovery unexpectedly succeeded")
	}
	failed, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if client.imports != 1 || failed.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED) || applianceRecoveryState(failed.State) {
		t.Fatalf("recovery did not attempt once and fail closed: imports=%d installation=%+v", client.imports, failed)
	}
}

func TestSystemApplianceLaunchRequestStableIdentity(t *testing.T) {
	installation := store.ApplianceInstallation{ID: "installation", DataVolumeBytes: 1 << 30}
	artifact := store.ApplianceArtifact{ImageDigest: "sha256:image", ImageReference: "image@sha256:image"}
	grant := store.ApplianceGrant{System: store.ResourceAmount{PIDs: 32, DiskBytes: 2 << 30}, EgressMode: 1}
	volume := "volume"
	build := func(volume string, boot string, revision uint64) *vmmdv1.EnsureSystemApplianceRequest {
		launchInstallation := installation
		launchInstallation.DataVolumeID = volume
		return systemApplianceLaunchRequest(launchInstallation, artifact, grant, boot, revision)
	}
	first := build(volume, "boot", 2)
	marshal := func(request *vmmdv1.EnsureSystemApplianceRequest) []byte {
		t.Helper()
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	if !bytes.Equal(marshal(first), marshal(build(volume, "boot", 2))) {
		t.Fatal("same launch changed EnsureSystemAppliance input")
	}
	for _, test := range []struct {
		name, volume, boot string
		revision           uint64
	}{
		{name: "reinstall", volume: "new-volume", boot: "boot", revision: 2},
		{name: "new boot", volume: "volume", boot: "new-boot", revision: 2},
		{name: "new revision", volume: "volume", boot: "boot", revision: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := build(test.volume, test.boot, test.revision)
			if next.GetRequestId() == first.GetRequestId() || bytes.Equal(next.GetIdentity().GetBootNonce(), first.GetIdentity().GetBootNonce()) {
				t.Fatal("different launch reused request ID or boot nonce")
			}
		})
	}
	installation.DataVolumeID = volume
	changedArtifact := artifact
	changedArtifact.ImageDigest = "sha256:other"
	changed := systemApplianceLaunchRequest(installation, changedArtifact, grant, "boot", 2)
	if changed.GetRequestId() != first.GetRequestId() || bytes.Equal(marshal(changed), marshal(first)) {
		t.Fatal("same launch payload conflict does not retain its idempotency key")
	}
	installation.ID = "a/b"
	left := build("c", "boot", 2)
	installation.ID = "a"
	right := build("b/c", "boot", 2)
	if left.GetRequestId() == right.GetRequestId() {
		t.Fatal("ambiguous launch ID encoding")
	}
}

type incompleteApplianceStopClient struct {
	failingApplianceImportClient
	identity *vmmdv1.SystemApplianceIdentity
	stops    int
}

func (client *incompleteApplianceStopClient) StopSystemAppliance(_ context.Context, request *vmmdv1.StopSystemApplianceRequest, _ ...grpc.CallOption) (*vmmdv1.SystemApplianceResponse, error) {
	client.stops++
	if !proto.Equal(request.GetIdentity(), client.identity) {
		return nil, errors.New("wrong stopped identity")
	}
	if client.stops <= 2 {
		return nil, errors.New("volume cleanup incomplete")
	}
	return &vmmdv1.SystemApplianceResponse{Appliance: &vmmdv1.SystemAppliance{Identity: client.identity, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED}}, nil
}
func TestIncompleteApplianceStopRetainsRecoveryFence(t *testing.T) {
	controller, installation, identity, _, _, _ := renewalHost(t, time.Now())
	controller.egress = egress.New()
	client := &incompleteApplianceStopClient{identity: identity}
	runtime := &vmmdv1.SystemAppliance{Identity: identity, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED}
	server := &applianceAdminServer{controller: controller}
	response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.setDesiredState(ctx, applianceMutation{installation.ID, "restart-incomplete", installation.Revision, time.Now().Add(time.Minute).UnixMilli()}, installation.Revision, true,
			appliancev1.ApplianceState_APPLIANCE_STATE_STARTING, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART, "restart-pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := server.failApplianceOperation(context.Background(), response.GetOperation().GetOperationId(),
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "restart-failed", "APPLIANCE_RESTART_FAILED", errors.New("volume cleanup incomplete"))
	if err != nil || operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED) {
		t.Fatalf("restart failure receipt: state=%d err=%v", operation.State, err)
	}
	installation, err = controller.store.GetApplianceInstallation(context.Background(), installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) || !applianceRecoveryState(installation.State) {
		t.Fatal("failed operation made installation terminal")
	}
	for range 2 {
		if err := controller.recoverApplianceRuntime(context.Background(), client, installation, runtime); err == nil {
			t.Fatal("incomplete stop allowed replacement")
		}
		current, err := controller.store.GetApplianceInstallation(context.Background(), installation.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Revision != installation.Revision || current.State != installation.State || current.CurrentBootID != installation.CurrentBootID || current.CurrentContainerInstance != installation.CurrentContainerInstance {
			t.Fatal("failed cleanup advanced launch fence")
		}
		if projected := server.applianceStatus(context.Background(), current, runtime); projected.GetApplianceState() != appliancev1.ApplianceState_APPLIANCE_STATE_FAILED {
			t.Fatal("failed guest was hidden by the recoverable installation state")
		}
		persisted, err := controller.store.GetApplianceInstallation(context.Background(), installation.ID)
		if err != nil || persisted.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) || !applianceRecoveryState(persisted.State) {
			t.Fatal("guest failure projection made installation terminal")
		}
		if health := controller.applianceHealthProjection(installation.ID); health.GetHealthy() || health.GetStatus() != "INSTANCE_STOP_INCOMPLETE" {
			t.Fatal("incomplete cleanup reported healthy")
		}
	}
	if client.stops != 2 || client.imports != 0 {
		t.Fatalf("stop attempts = %d, imports = %d", client.stops, client.imports)
	}
	controller.supportDirectory = t.TempDir()
	artifact, err := controller.store.GetApplianceArtifact(context.Background(), installation.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	stageRecoveryContent(t, controller, artifact)
	if err := controller.recoverApplianceRuntime(context.Background(), client, installation, runtime); err == nil {
		t.Fatal("import failure lost")
	}
	if client.stops != 3 || client.imports != 1 {
		t.Fatalf("exact stop did not precede replacement: stops=%d imports=%d", client.stops, client.imports)
	}
}

func TestSealedApplianceAdminStateRemainsClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		enabled bool
		kind    appliancev1.ApplianceOperationKind
	}{
		{"activate", true, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE},
		{"restart", true, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART},
		{"stop", false, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP},
		{"revoke", false, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE},
	} {
		for _, ready := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ready=%t", test.name, ready), func(t *testing.T) {
				controller, installation, _, _, _, _ := renewalHost(t, time.Now())
				if ready {
					registration, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := controller.store.SetRemoteEnrollment(context.Background(), registration.ID, registration.Revision, registration.EnrollmentState, int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", ""); err != nil {
						t.Fatal(err)
					}
				}
				installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
				installation.DesiredEnabled = false
				var err error
				installation, err = controller.store.UpdateApplianceInstallation(context.Background(), installation, installation.Revision)
				if err != nil {
					t.Fatal(err)
				}
				state := appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED
				if test.enabled {
					state = appliancev1.ApplianceState_APPLIANCE_STATE_STARTING
				}
				mutation := applianceMutation{installation.ID, test.name, installation.Revision, time.Now().Add(time.Minute).UnixMilli()}
				_, err = controller.store.CreateApplianceOperation(context.Background(), store.ApplianceOperation{ID: "failed-update", InstallationID: installation.ID, RequestID: "failed-update", Kind: 9, State: 3, Phase: 10, Receipt: []byte("original-target"), ErrorReason: "TARGET_SEALED"})
				if err != nil {
					t.Fatal(err)
				}
				server := &applianceAdminServer{controller: controller}
				response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
					return server.setDesiredState(ctx, mutation, installation.Revision, test.enabled, state, test.kind, test.name)
				})
				if test.enabled {
					if status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("sealed start returned %v", err)
					}
				} else if err != nil || response.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING {
					t.Fatalf("sealed disable was blocked: response=%v err=%v", response, err)
				}
				current, err := controller.store.GetApplianceInstallation(context.Background(), installation.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) || current.DesiredEnabled {
					t.Fatal("admin mutation reopened sealed installation")
				}
				if test.enabled && current.Revision != installation.Revision {
					t.Fatal("rejected start changed revision")
				}
				if current.DataVolumeID != installation.DataVolumeID || current.CurrentContainerInstance != installation.CurrentContainerInstance {
					t.Fatal("admin mutation changed data or execution identity")
				}
				oldUpdate, err := controller.store.GetApplianceOperation(context.Background(), "failed-update")
				if err != nil || oldUpdate.Phase != 10 || string(oldUpdate.Receipt) != "original-target" || (!test.enabled && oldUpdate.State != 4) || (test.enabled && oldUpdate.State != 3) {
					t.Fatalf("sealed update exit=%+v err=%v", oldUpdate, err)
				}
				registration, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
				if err != nil {
					t.Fatal(err)
				}
				expectedState := hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED
				if ready && !test.enabled {
					expectedState = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING
				}
				if registration.State != int32(expectedState) {
					t.Fatal("admin mutation did not preserve registration admission")
				}
			})
		}
	}
}

func TestSealedAppliancePlannedReplayCannotLaunch(t *testing.T) {
	for _, kind := range []appliancev1.ApplianceOperationKind{appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART} {
		t.Run(kind.String(), func(t *testing.T) {
			controller, installation, _, _, _, _ := renewalHost(t, time.Now())
			server := &applianceAdminServer{controller: controller}
			mutation := applianceMutation{installation.ID, "planned", installation.Revision, time.Now().Add(time.Minute).UnixMilli()}
			invoke := func() (*appliancev1.ApplianceOperationResponse, error) {
				return callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
					return server.setDesiredState(ctx, mutation, mutation.expectedRevision, true, appliancev1.ApplianceState_APPLIANCE_STATE_STARTING, kind, "planned")
				})
			}
			if _, err := invoke(); err != nil {
				t.Fatal(err)
			}
			current, err := controller.store.GetApplianceInstallation(context.Background(), installation.ID)
			if err != nil {
				t.Fatal(err)
			}
			current.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
			current.DesiredEnabled = false
			current, err = controller.store.UpdateApplianceInstallation(context.Background(), current, current.Revision)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := invoke(); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("planned replay: %v", err)
			}
			client := new(failingApplianceImportClient)
			if err := controller.startAppliance(context.Background(), client, current); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("sealed execution: %v", err)
			}
			current.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
			current, err = controller.store.UpdateApplianceInstallation(context.Background(), current, current.Revision)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.startAppliance(context.Background(), client, current); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("disabled execution: %v", err)
			}
			if client.imports != 0 {
				t.Fatal("sealed or disabled launch reached image import")
			}
		})
	}
}

func recoveryContentManifest() *appliancev1.ControllerApplianceManifest {
	manifest := verifierManifest()
	data := []byte("import fixture")
	hash := sha256.Sum256(data)
	digest := "sha256:" + repeatedHex('a')
	manifest.ContentRefs = []*appliancev1.ApplianceContentReference{{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, ArchiveSha256: hash[:], ArchiveSize: uint64(len(data)), Platform: "linux/arm64", ImageReference: "docker.io/library/test@" + digest, ImageManifestDigest: digest}}
	return manifest
}
func stageRecoveryContent(t *testing.T, controller *Controller, artifact store.ApplianceArtifact) {
	t.Helper()
	refs, err := artifactContentReferences(artifact)
	if err != nil {
		t.Fatal(err)
	}
	root := controller.applianceContentRoot()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(root, "stage-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("import fixture")); err != nil {
		t.Fatal(err)
	}
	if err := appliancecontent.Publish(root, file, refs[0]); err != nil {
		t.Fatal(err)
	}
}
func (client *failingApplianceImportClient) StageApplianceContent(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[appliancev1.StageContentRequest, appliancev1.ApplianceContentStatus], error) {
	client.imports++
	return nil, errors.New("guest content stage unavailable")
}
