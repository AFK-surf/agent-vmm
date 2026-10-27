//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type blockedContentStream struct {
	grpc.ServerStream
	ctx      context.Context
	header   *appliancev1.StageContentHeader
	release  chan struct{}
	response *appliancev1.StageContentResponse
}

func (s *blockedContentStream) Context() context.Context { return s.ctx }
func (s *blockedContentStream) SendAndClose(response *appliancev1.StageContentResponse) error {
	s.response = response
	return nil
}
func (s *blockedContentStream) Recv() (*appliancev1.StageContentRequest, error) {
	if s.header != nil {
		header := s.header
		s.header = nil
		return &appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_Header{Header: header}}, nil
	}
	<-s.release
	return nil, context.Canceled
}

func TestContentStageDeadlineBoundsBlockedBodyAndAdmission(t *testing.T) {
	for _, locked := range []bool{false, true} {
		t.Run(map[bool]string{false: "body", true: "admission"}[locked], func(t *testing.T) {
			database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			controller := newTestOwner(&Owner{store: database, supportDirectory: t.TempDir()})
			if locked {
				lock := controller.applianceContentStageLock()
				lock.Lock(context.Background())
				defer lock.Unlock()
			}
			server := controller
			release := make(chan struct{})
			defer close(release)
			started := time.Now()
			_, err = callAsOperator(t, func(ctx context.Context) (bool, error) {
				manifest := "sha256:" + strings.Repeat("a", 64)
				stream := &blockedContentStream{ctx: ctx, release: release, header: &appliancev1.StageContentHeader{RequestId: "content-deadline", DeadlineUnixMillis: time.Now().Add(100 * time.Millisecond).UnixMilli(), ContentRef: &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_WORKLOAD, ArchiveSha256: make([]byte, 32), ArchiveSize: 7, Platform: "linux/arm64", ImageReference: "docker.io/library/test@" + manifest, ImageManifestDigest: manifest}}}
				return false, server.StageContent(stream)
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("stage error = %v", err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("stage ignored its body deadline")
			}
		})
	}
}

func TestContentStatusBoundsReferenceFrame(t *testing.T) {
	server := &Owner{}
	_, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.GetContentStatusResponse, error) {
		return server.GetContentStatus(ctx, &appliancev1.GetContentStatusRequest{ContentRefs: []*appliancev1.ApplianceContentReference{{ImageReference: strings.Repeat("x", 33<<10)}}})
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized frame = %v", err)
	}
}

func TestContentStageReusesVerifiedArchiveWithoutBody(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := newTestOwner(&Owner{store: database, supportDirectory: t.TempDir()})
	manifest := recoveryContentManifest()
	encoded, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	stageRecoveryContent(t, controller, store.ApplianceArtifact{Manifest: encoded})
	release := make(chan struct{})
	defer close(release)
	server := controller
	_, err = callAsOperator(t, func(ctx context.Context) (bool, error) {
		stream := &blockedContentStream{ctx: ctx, release: release, header: &appliancev1.StageContentHeader{RequestId: "cached-content", DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ContentRef: manifest.ContentRefs[0]}}
		return true, server.StageContent(stream)
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := database.ApplianceOperationByRequest(context.Background(), "cached-content")
	if err != nil || operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED) {
		t.Fatalf("receipt=%+v err=%v", operation, err)
	}
}

func TestContentStageExpiredOriginalReceiptPrecedesAdmission(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := newTestOwner(&Owner{store: database})
	manifest := "sha256:" + strings.Repeat("a", 64)
	header := &appliancev1.StageContentHeader{RequestId: "expired-original", DeadlineUnixMillis: time.Now().Add(-time.Hour).UnixMilli(), ContentRef: &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_WORKLOAD, ArchiveSha256: make([]byte, 32), ArchiveSize: 7, Platform: "linux/arm64", ImageReference: "docker.io/library/test@" + manifest, ImageManifestDigest: manifest}}
	receipt, err := proto.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.CreateApplianceOperation(context.Background(), store.ApplianceOperation{ID: "original", RequestID: header.RequestId, Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE_CONTENT), State: 1, Phase: 1, DeadlineUnixMillis: header.DeadlineUnixMillis, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	lock := controller.applianceContentStageLock()
	lock.Lock(context.Background())
	defer lock.Unlock()
	server := controller
	for _, tc := range []struct {
		name   string
		change func(*appliancev1.StageContentHeader)
		code   codes.Code
	}{{"original", func(*appliancev1.StageContentHeader) {}, codes.DeadlineExceeded}, {"conflicting", func(h *appliancev1.StageContentHeader) { h.ContentRef.ArchiveSize++ }, codes.Aborted}, {"busy", func(h *appliancev1.StageContentHeader) { h.RequestId = "busy" }, codes.DeadlineExceeded}, {"new", func(h *appliancev1.StageContentHeader) { h.RequestId = "new" }, codes.DeadlineExceeded}} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "new" {
				lock.Unlock()
				defer lock.Lock(context.Background())
			}
			copy := proto.Clone(header).(*appliancev1.StageContentHeader)
			tc.change(copy)
			_, err := callAsOperator(t, func(ctx context.Context) (bool, error) {
				return false, server.StageContent(&blockedContentStream{ctx: ctx, header: copy})
			})
			if status.Code(err) != tc.code {
				t.Fatalf("stage=%v", err)
			}
			if tc.name == "new" || tc.name == "busy" {
				found := false
				for _, detail := range status.Convert(err).Details() {
					if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "CONTENT_STAGE_NOT_ADMITTED" && info.Metadata["admission"] == "not_accepted" {
						found = true
					}
				}
				if found != (tc.name == "new") {
					t.Fatalf("expiry admission fact does not match admission lock: %v", err)
				}
			}
		})
	}
}

func TestContentStageReopenSettlesPublishedOrInterruptedWriter(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprint(published), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			controller := newTestOwner(&Owner{store: database, supportDirectory: t.TempDir()})
			manifest := recoveryContentManifest()
			if published {
				encoded, _ := proto.Marshal(manifest)
				stageRecoveryContent(t, controller, store.ApplianceArtifact{Manifest: encoded})
			}
			header := &appliancev1.StageContentHeader{RequestId: "lost-stage", DeadlineUnixMillis: time.Now().Add(-time.Minute).UnixMilli(), ContentRef: manifest.ContentRefs[0]}
			receipt, _ := proto.Marshal(header)
			if _, err := database.CreateApplianceOperation(context.Background(), store.ApplianceOperation{ID: "original", RequestID: header.RequestId, Kind: 10, State: 1, Phase: 1, DeadlineUnixMillis: header.DeadlineUnixMillis, Receipt: receipt}); err != nil {
				t.Fatal(err)
			}
			database.Close()
			database, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			controller.store = database
			for range 2 {
				_, err := callAsOperator(t, func(ctx context.Context) (bool, error) {
					stream := &blockedContentStream{ctx: ctx, header: header}
					err := (controller).StageContent(stream)
					want := appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED
					if published {
						want = appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED
					}
					if err == nil && stream.response.GetOperation().GetState() != want {
						t.Fatalf("stage=%v want=%v", stream.response, want)
					}
					return true, err
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestApplianceContentCollectionUsesActivePendingAndStageFacts(t *testing.T) {
	controller, original := updateDrainFixture(t)
	controller.supportDirectory = t.TempDir()
	ctx := context.Background()
	makeArchive := func(name string) *appliancev1.ApplianceContentReference {
		body := []byte(name)
		sha := sha256.Sum256(body)
		manifest := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("manifest"+name)))
		ref := &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_WORKLOAD, ArchiveSha256: sha[:], ArchiveSize: uint64(len(body)), ImageReference: "docker.io/library/test@" + manifest, ImageManifestDigest: manifest, Platform: "linux/arm64"}
		root := controller.applianceContentRoot()
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
		file, err := os.CreateTemp(root, "stage-")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := appliancecontent.Publish(root, file, ref); err != nil {
			t.Fatal(err)
		}
		return ref
	}
	active, pending, shared, cold, fresh := makeArchive("active"), makeArchive("pending"), makeArchive("shared"), makeArchive("cold"), makeArchive("fresh")
	stageArtifact := func(id string, refs ...*appliancev1.ApplianceContentReference) {
		model := verifierManifest()
		model.ContentRefs = refs
		manifest, err := proto.Marshal(model)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = controller.store.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: id, CanonicalHash: []byte(id), Manifest: manifest, ProductID: "product"}, store.ApplianceOperation{ID: "stage-" + id, RequestID: "stage-" + id, Kind: 1, State: 2, Phase: 5, DeadlineUnixMillis: time.Now().Add(-time.Minute).UnixMilli(), Receipt: []byte(id)})
		if err != nil {
			t.Fatal(err)
		}
	}
	stageArtifact("active-content", active, shared)
	stageArtifact("pending-content", pending, shared)
	stageArtifact("cold-content", cold)
	installation := original
	installation.ID = "content-installation"
	installation.ArtifactID = "active-content"
	installation.DataVolumeID = "content-volume"
	installation.RegistrationID = "content-registration"
	grant, err := controller.store.GetApplianceGrant(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	grant.InstallationID = installation.ID
	registration, err := controller.store.GetRemoteRegistration(ctx, original.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	registration.ID = installation.RegistrationID
	registration.InstallationID = installation.ID
	_, _, _, err = controller.store.InstallApplianceDomain(ctx, installation, grant, registration, nil, store.ApplianceOperation{ID: "content-install", RequestID: "content-install", Kind: 2, State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	receipt, _ := proto.Marshal(&appliancev1.ApplianceUpdateProgress{Request: &appliancev1.UpdateApplianceRequest{ArtifactId: "pending-content"}})
	_, err = controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "content-update", RequestID: "content-update", InstallationID: installation.ID, Kind: 9, State: 3, Phase: 6, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	header, _ := proto.Marshal(&appliancev1.StageContentHeader{ContentRef: fresh})
	stage, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "fresh-stage", RequestID: "fresh-stage", Kind: 10, State: 2, Phase: 5, DeadlineUnixMillis: time.Now().Add(300 * time.Millisecond).UnixMilli(), Receipt: header})
	if err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(controller.applianceContentRoot(), ".content-stage-interrupted")
	unrelated := filepath.Join(controller.applianceContentRoot(), "operator-notes")
	if err := os.WriteFile(stale, []byte("partial archive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unrelated, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	collect := func() {
		lock := controller.applianceContentStageLock()
		lock.Lock(context.Background())
		defer lock.Unlock()
		if _, err := controller.collectApplianceContent(ctx); err != nil {
			t.Fatal(err)
		}
	}
	collect()
	for _, ref := range []*appliancev1.ApplianceContentReference{active, pending, shared, fresh} {
		if exists, err := appliancecontent.Available(controller.applianceContentRoot(), ref); err != nil || !exists {
			t.Fatalf("retained %s=%v %v", ref.ImageManifestDigest, exists, err)
		}
	}
	if exists, _ := appliancecontent.Available(controller.applianceContentRoot(), cold); exists {
		t.Fatal("expired unused artifact retained archive")
	}
	controller.systemCapacity.PIDs = 10
	controller.systemCapacity.DiskBytes = 10
	withTestRemoteCapacity(controller, store.Environment{PIDs: 10, DiskBytes: 10})
	server := controller
	_, admissionErr := callAsOperator(t, func(ctx context.Context) (*appliancev1.InstallResponse, error) {
		return server.Install(ctx, &appliancev1.InstallRequest{RequestId: "collected-install", InstallationId: "collected-installation", ArtifactId: "cold-content", DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ApprovedGrant: verifierGrant()})
	})
	if status.Code(admissionErr) != codes.FailedPrecondition || !strings.Contains(admissionErr.Error(), "must be staged") {
		t.Fatalf("collected content admission=%v", admissionErr)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale upload remained: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated CAS-root file removed: %v", err)
	}
	// Existing Stop interrupts the failed target without releasing current content.
	_, err = controller.store.BeginApplianceStateMutation(ctx, installation, installation.Revision, store.ApplianceOperation{ID: "content-stop", RequestID: "content-stop", InstallationID: installation.ID, Kind: 4, State: 1, Phase: 1})
	if err != nil {
		t.Fatal(err)
	}
	// The original staging deadline, rather than a GC-specific TTL, owns this grace.
	_, err = controller.store.TransitionApplianceOperation(ctx, stage.ID, store.ApplianceOperationTransition{ExpectedState: 2, ExpectedPhase: 5, State: 3, Phase: 5, Receipt: header})
	if err != nil {
		t.Fatal(err)
	}
	collect()
	if exists, _ := appliancecontent.Available(controller.applianceContentRoot(), pending); exists {
		t.Fatal("cancelled pending archive pinned forever")
	}
	for _, ref := range []*appliancev1.ApplianceContentReference{active, shared} {
		if exists, _ := appliancecontent.Available(controller.applianceContentRoot(), ref); !exists {
			t.Fatal("Stop removed active/shared archive")
		}
	}
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := controller.store.BeginApplianceRemove(ctx, store.ApplianceRemove{InstallationID: current.ID, RegistrationID: current.RegistrationID, OperationID: "content-remove", ExpectedRevision: current.Revision, ExpectedState: current.State, PendingState: 4, ExpectedPhase: 1, Operation: store.ApplianceOperation{ID: "content-remove", RequestID: "content-remove", InstallationID: current.ID, Kind: 7, State: 1, Phase: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CommitApplianceRemove(ctx, store.ApplianceRemove{InstallationID: current.ID, RegistrationID: current.RegistrationID, OperationID: "content-remove", CommitRevision: removed.Revision}); err != nil {
		t.Fatal(err)
	}
	collect()
	for _, ref := range []*appliancev1.ApplianceContentReference{active, shared} {
		if exists, _ := appliancecontent.Available(controller.applianceContentRoot(), ref); exists {
			t.Fatal("removed installation kept archive")
		}
	}
	if exists, _ := appliancecontent.Available(controller.applianceContentRoot(), fresh); !exists {
		t.Fatal("original stage deadline lost protection")
	}
	time.Sleep(time.Until(time.UnixMilli(stage.DeadlineUnixMillis)) + time.Millisecond)
	collect()
	if exists, _ := appliancecontent.Available(controller.applianceContentRoot(), fresh); exists {
		t.Fatal("expired original stage deadline pinned content")
	}
	// A reclaimed archive is explicitly missing and the unchanged content identity can be staged again.
	stageArtifact("cold-new", cold)
	root := controller.applianceContentRoot()
	file, err := os.CreateTemp(root, "restage-")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("cold"))
	if err := appliancecontent.Publish(root, file, cold); err != nil {
		t.Fatal(err)
	}
	if exists, err := appliancecontent.Available(root, cold); err != nil || !exists {
		t.Fatalf("restage=%v %v", exists, err)
	}
	controller.applianceSecrets, err = NewSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	admitted := make(chan error, 1)
	collected := make(chan error, 1)
	go func() {
		<-start
		_, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.InstallResponse, error) {
			return server.Install(ctx, &appliancev1.InstallRequest{RequestId: "racing-install", InstallationId: "racing-installation", ArtifactId: "cold-new", DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ApprovedGrant: verifierGrant(), OpaqueBootstrapSeed: []byte("fixture-bootstrap")})
		})
		admitted <- err
	}()
	go func() {
		<-start
		lock := controller.applianceContentStageLock()
		lock.Lock(context.Background())
		_, err := controller.collectApplianceContent(ctx)
		lock.Unlock()
		collected <- err
	}()
	close(start)
	if err := <-collected; err != nil {
		t.Fatal(err)
	}
	installErr := <-admitted
	if installErr == nil {
		if exists, err := appliancecontent.Available(root, cold); err != nil || !exists {
			t.Fatalf("accepted concurrent Install lost archive: %v", err)
		}
	} else if status.Code(installErr) != codes.FailedPrecondition || !strings.Contains(installErr.Error(), "must be staged") {
		t.Fatalf("racing admission=%v", installErr)
	}
}
