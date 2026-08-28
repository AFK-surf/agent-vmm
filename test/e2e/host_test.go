//go:build darwin && e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func TestE2E(t *testing.T) {
	t.Run("boot", testBoot)
	t.Run("state_initialization", testStateInitialization)

	harness := startHostWithState(t, 8<<30)
	harness.start(t)
	t.Run("system_appliance", func(t *testing.T) { testSystemAppliance(t, harness) })
	client := harness.runtime(t)
	t.Run("session_auth_and_revocation", func(t *testing.T) { testSessionAuthAndRevocation(t, harness, client) })
	t.Run("remote_workflow", func(t *testing.T) { testRemoteWorkflow(t, harness) })
	t.Run("three_remote_domains", func(t *testing.T) { testThreeRemoteDomains(t, harness) })
	t.Run("mcp_workflow", func(t *testing.T) { testMCPWorkflow(t, harness) })
	t.Run("workspace_file_workflow", func(t *testing.T) { testWorkspaceFileWorkflow(t, client) })
	t.Run("image_record_lifecycle", func(t *testing.T) { testImageRecordLifecycle(t, client) })
	parallel := []struct {
		name string
		run  func(*testing.T, *productClient)
	}{
		{"container_lifecycle", testContainerLifecycle},
		{"named_volume", testNamedVolume},
		{"disk_quota", testDiskQuota},
		{"same_namespace", testSameNamespace},
		{"default_isolation", testDefaultIsolation},
		{"partial_link", testPartialLink},
		{"link_revoke", testLinkRevoke},
		{"port_forward", testPortForward},
		{"dockerfile_build", testDockerfileBuild},
		{"parallel_agents", testParallelAgents},
		{"cancellation_cleanup", testCancellationCleanup},
	}
	limit := make(chan struct{}, 4)
	var active, peak atomic.Int32
	t.Run("parallel", func(t *testing.T) {
		for _, test := range parallel {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				limit <- struct{}{}
				current := active.Add(1)
				for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
				}
				defer func() { active.Add(-1); <-limit }()
				test.run(t, client)
			})
		}
	})
	t.Logf("E2E_PARALLEL_PEAK=%d", peak.Load())
	t.Run("egress", func(t *testing.T) { testEgress(t, client) })
	t.Run("imported_image_forced_restart", func(t *testing.T) { testImportedImageForcedRestart(t, harness) })
	t.Run("VM_restart", func(t *testing.T) { testVMRestart(t, harness) })
	t.Run("forced_VM_stop", func(t *testing.T) { testForcedVMStop(t, harness) })
	t.Run("cleanup_contract", func(t *testing.T) { testCleanupContract(t, harness, client) })
	t.Run("residual_scan", func(t *testing.T) { testResidualScan(t, harness.runtime(t)) })
	harness.stop(t, false)
	t.Run("pgvector_image_and_volume", func(t *testing.T) {
		// A failed registry download can intentionally leave an unknown runtime
		// outcome. Keep this network-dependent contract on a dedicated final VM so
		// it cannot cascade projection drift into the local lifecycle matrix.
		isolated := startHostWithState(t, 8<<30)
		isolated.start(t)
		t.Cleanup(func() { isolated.stop(t, true) })
		testPGVectorImageAndVolume(t, isolated.runtime(t))
	})
}

func testSystemAppliance(t *testing.T, harness *hostHarness) {
	bundle := os.Getenv("AGENT_VMM_E2E_BUNDLE")
	if bundle == "" {
		bundle = "/tmp/agent-vmm-e2e-bundle"
	}
	lockedDigest, err := os.ReadFile(filepath.Join(bundle, "reference-appliance.digest"))
	if err != nil {
		t.Fatal(err)
	}
	imageDigest := strings.TrimSpace(string(lockedDigest))
	limits := func(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
	}
	manifest := &appliancev1.ControllerApplianceManifest{ContractVersion: "1", ProductId: "reference",
		Release: "e2e", ImageReference: "agent-vmm.local/reference@" + imageDigest,
		ImageManifestDigest: imageDigest, Platform: "linux/arm64", BootstrapProtocolVersion: "appliance-v1",
		ControllerProtocolVersion: "controller-ready-v1", SystemResources: &appliancev1.ResourceRange{
			Floor: limits(200, 128<<20, 64, 1<<30), Ceiling: limits(400, 256<<20, 128, 1<<30)},
		DataVolumeBytes: 1 << 30, RequestedCapacity: &appliancev1.CapacityGrant{MaxEnvironments: 2,
			AggregateLimits: limits(200, 256<<20, 128, 512<<20), PerEnvironmentLimits: limits(100, 128<<20, 64, 256<<20),
			MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, MaxLeaseSeconds: 300, DisconnectGraceSeconds: 5},
		RequestedEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StartupDeadlineSeconds: 60,
		ProbationSeconds: 1, HealthTimeoutSeconds: 10, MinimumApplianceRuntime: "1"}
	deadline := time.Now().Add(5 * time.Minute).UnixMilli()
	client := appliancev1.NewApplianceAdminServiceClient(harness.connection)
	secretA := []byte("phase2-e2e-bootstrap-secret-a-4d57af9b")
	secretB := []byte("phase2-e2e-bootstrap-secret-b-9c34b572")
	statusA := activateReferenceAppliance(t, client, "e2e-reference-a", manifest, deadline, secretA)
	manifestB := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
	manifestB.Release = "e2e-b"
	statusB := activateReferenceAppliance(t, client, "e2e-reference-b", manifestB, deadline, secretB)
	for _, status := range []*appliancev1.ApplianceStatus{statusA, statusB} {
		if status.GetApplianceState() != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || !status.GetHealth().GetHealthy() || status.GetHealth().GetStatus() != "ready;bootstrap-secret-surfaces-clean" {
			t.Fatalf("appliance status = %+v", status)
		}
	}
	if _, err := client.Restart(context.Background(), &appliancev1.RestartApplianceRequest{InstallationId: "e2e-reference-b", RequestId: "reference-b-explicit-restart",
		ExpectedRevision: statusB.GetRevision(), DeadlineUnixMillis: deadline}); err != nil {
		t.Fatal(err)
	}
	statusB = waitApplianceState(t, client, "e2e-reference-b", appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	if statusB.GetApplianceState() != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || !statusB.GetHealth().GetHealthy() {
		t.Fatalf("explicit appliance restart failed: %+v", statusB)
	}
	support, err := harness.client.ExportSupportBundle(context.Background(), &hostv1.ExportSupportBundleRequest{RequestId: "phase2-secret-scan"})
	if err != nil || support.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("support bundle = %+v, %v", support, err)
	}
	for _, secret := range [][]byte{secretA, secretB} {
		assertSecretAbsent(t, harness.database, secret)
		assertSecretAbsent(t, harness.instance, secret)
		assertSecretAbsent(t, support.GetOperation().GetOutputPath(), secret)
		assertProcessSecretAbsent(t, harness.process, secret)
	}
	harness.stop(t, false)
	harness.start(t)
	for _, installationID := range []string{"e2e-reference-a", "e2e-reference-b"} {
		status := waitApplianceState(t, client, installationID, appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
		if status.GetApplianceState() != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || !status.GetHealth().GetHealthy() {
			t.Fatalf("appliance did not unlock after ordinary restart: %+v", status)
		}
	}
}

func activateReferenceAppliance(t *testing.T, client appliancev1.ApplianceAdminServiceClient, installationID string, manifest *appliancev1.ControllerApplianceManifest, deadline int64, secret []byte) *appliancev1.ApplianceStatus {
	t.Helper()
	prefix := strings.TrimPrefix(installationID, "e2e-")
	stage, err := client.StageArtifact(context.Background(), &appliancev1.StageArtifactRequest{
		RequestId: prefix + "-stage", DeadlineUnixMillis: deadline, Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	artifactID := stage.GetArtifactId()
	current := applianceStatus(t, client, installationID)
	grant := &appliancev1.ApprovedGrant{SystemResources: manifest.GetSystemResources(), Capacity: manifest.GetRequestedCapacity(), EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, Revision: current.GetRevision()}
	_, err = client.PlanInstall(context.Background(), &appliancev1.PlanInstallRequest{InstallationId: installationID, RequestId: prefix + "-install-plan", ExpectedRevision: current.GetRevision(), ArtifactId: artifactID, DeadlineUnixMillis: deadline, ApprovedGrant: grant})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Install(context.Background(), &appliancev1.InstallRequest{InstallationId: installationID, RequestId: prefix + "-install", ExpectedRevision: current.GetRevision(), ArtifactId: artifactID,
		DeadlineUnixMillis: deadline, ApprovedGrant: grant, OpaqueBootstrapSeed: append([]byte(nil), secret...)}); err != nil {
		t.Fatal(err)
	}
	current = applianceStatus(t, client, installationID)
	_, err = client.PlanActivation(context.Background(), &appliancev1.PlanActivationRequest{InstallationId: installationID, RequestId: prefix + "-activation-plan", ExpectedRevision: current.GetRevision(), DeadlineUnixMillis: deadline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Activate(context.Background(), &appliancev1.ActivateRequest{InstallationId: installationID, RequestId: prefix + "-activate", ExpectedRevision: current.GetRevision(), DeadlineUnixMillis: deadline}); err != nil {
		t.Fatal(err)
	}
	return applianceStatus(t, client, installationID)
}

func assertProcessSecretAbsent(t *testing.T, process *exec.Cmd, secret []byte) {
	t.Helper()
	if bytes.Contains([]byte(strings.Join(process.Args, "\x00")), secret) || bytes.Contains([]byte(strings.Join(process.Env, "\x00")), secret) {
		t.Fatal("bootstrap secret found in host process argv or environment")
	}
}

func applianceStatus(t *testing.T, client appliancev1.ApplianceAdminServiceClient, installationID string) *appliancev1.ApplianceStatus {
	t.Helper()
	response, err := client.GetStatus(context.Background(), &appliancev1.GetApplianceStatusRequest{InstallationId: installationID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return &appliancev1.ApplianceStatus{InstallationId: installationID}
		}
		t.Fatal(err)
	}
	return response.GetStatus()
}

func waitApplianceState(t *testing.T, client appliancev1.ApplianceAdminServiceClient, installationID string, expected appliancev1.ApplianceState) *appliancev1.ApplianceStatus {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var current *appliancev1.ApplianceStatus
	for time.Now().Before(deadline) {
		current = applianceStatus(t, client, installationID)
		if current.GetApplianceState() == expected && (expected != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || current.GetHealth().GetHealthy()) {
			return current
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("appliance %q did not reach %s: %+v", installationID, expected, current)
	return nil
}

func assertSecretAbsent(t *testing.T, root string, secret []byte) {
	t.Helper()
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{root}
	if info.IsDir() {
		paths = paths[:0]
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr == nil && !entry.IsDir() {
				paths = append(paths, path)
			}
			return walkErr
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 1<<20)
		window := make([]byte, 0, len(secret)-1)
		for {
			count, readErr := file.Read(buffer)
			if count != 0 {
				data := append(window, buffer[:count]...)
				if bytes.Contains(data, secret) {
					file.Close()
					t.Fatalf("bootstrap secret found in %s", path)
				}
				keep := min(len(secret)-1, len(data))
				window = append(window[:0], data[len(data)-keep:]...)
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				t.Fatal(readErr)
			}
		}
		file.Close()
	}
}

func testWorkspaceFileWorkflow(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "workspace-files", 1<<30)
	session, err := client.session("workspace-files")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prefix := []byte("resume-")
	first, err := session.client.WriteWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Header{Header: &hostv1.WriteWorkspaceHeader{
		RequestId: "workspace-resume", Path: "workspace.txt", Mode: 0o600}}}); err != nil {
		t.Fatal(err)
	}
	if err := first.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Data{Data: prefix}}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.CloseAndRecv(); err == nil {
		t.Fatal("unfinished workspace upload unexpectedly committed")
	}
	prefixDigest := sha256.Sum256(prefix)
	second, err := session.client.WriteWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Header{Header: &hostv1.WriteWorkspaceHeader{
		RequestId: "workspace-resume", Path: "workspace.txt", Offset: uint64(len(prefix)), ExpectedPrefixSha256: prefixDigest[:], Mode: 0o600}}}); err != nil {
		t.Fatal(err)
	}
	if err := second.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Data{Data: []byte("complete")}}); err != nil {
		t.Fatal(err)
	}
	if err := second.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Finish{Finish: true}}); err != nil {
		t.Fatal(err)
	}
	written, err := second.CloseAndRecv()
	if err != nil || written.GetEntry().GetSize() != uint64(len("resume-complete")) {
		t.Fatalf("workspace write=%+v err=%v", written, err)
	}
	read, err := session.client.ReadWorkspace(ctx, &hostv1.ReadWorkspaceRequest{Path: "workspace.txt"})
	if err != nil {
		t.Fatal(err)
	}
	var contents []byte
	for {
		message, err := read.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		contents = append(contents, message.GetData()...)
	}
	if string(contents) != "resume-complete" {
		t.Fatalf("workspace contents=%q", contents)
	}
	listed, err := session.client.ListWorkspace(ctx, &hostv1.ListWorkspaceRequest{Path: ".", Limit: 10})
	if err != nil || len(listed.GetEntries()) != 1 || listed.GetEntries()[0].GetPath() != "workspace.txt" {
		t.Fatalf("workspace list=%+v err=%v", listed, err)
	}
	if _, err := session.client.StatWorkspace(ctx, &hostv1.WorkspacePathRequest{Path: "../escape"}); err == nil {
		t.Fatal("workspace traversal path was accepted")
	}
	archive, err := session.client.ArchiveWorkspace(ctx, &hostv1.ArchiveWorkspaceRequest{Path: "workspace.txt"})
	if err != nil {
		t.Fatal(err)
	}
	var archiveBytes []byte
	for {
		message, err := archive.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		archiveBytes = append(archiveBytes, message.GetData()...)
	}
	extract, err := session.client.ExtractWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := extract.Send(&hostv1.ExtractWorkspaceRequest{Payload: &hostv1.ExtractWorkspaceRequest_Header{Header: &hostv1.ExtractWorkspaceHeader{
		RequestId: "workspace-extract", Path: "restored"}}}); err != nil {
		t.Fatal(err)
	}
	for len(archiveBytes) != 0 {
		count := min(len(archiveBytes), 64<<10)
		if err := extract.Send(&hostv1.ExtractWorkspaceRequest{Payload: &hostv1.ExtractWorkspaceRequest_Data{Data: archiveBytes[:count]}}); err != nil {
			t.Fatal(err)
		}
		archiveBytes = archiveBytes[count:]
	}
	if err := extract.Send(&hostv1.ExtractWorkspaceRequest{Payload: &hostv1.ExtractWorkspaceRequest_Finish{Finish: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := extract.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	if restored, err := session.client.StatWorkspace(ctx, &hostv1.WorkspacePathRequest{Path: "restored/workspace.txt"}); err != nil || restored.GetSha256() == nil {
		t.Fatalf("restored workspace entry=%+v err=%v", restored, err)
	}
}

func testImageRecordLifecycle(t *testing.T, client *productClient) {
	const firstEnvironment = "image-record-a"
	const secondEnvironment = "image-record-b"
	ensureNamespace(t, client, firstEnvironment, 1<<30)
	ensureNamespace(t, client, secondEnvironment, 1<<30)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if _, err := pullImageAttempt(ctx, client, firstEnvironment, "pull-image-record-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := pullImageAttempt(ctx, client, secondEnvironment, "pull-image-record-b"); err != nil {
		t.Fatal(err)
	}
	first := imageByReference(t, client, firstEnvironment, workloadImage)
	imageByReference(t, client, secondEnvironment, workloadImage)
	container := createContainer(t, client, firstEnvironment, "image-user", []string{"sleep", "300"}, nil)
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-created-image", first)), "IMAGE_IN_USE")
	if _, err := client.StartContainer(ctx, firstEnvironment, &hostv1.ContainerMutationRequest{RequestId: "start-image-user", ContainerId: container.GetId()}); err != nil {
		t.Fatal(err)
	}
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-running-image", first)), "IMAGE_IN_USE")
	seconds := uint32(2)
	if _, err := client.StopContainer(ctx, firstEnvironment, &hostv1.StopContainerRequest{RequestId: "stop-image-user", ContainerId: container.GetId(), TimeoutSeconds: &seconds}); err != nil {
		t.Fatal(err)
	}
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-stopped-image", first)), "IMAGE_IN_USE")
	if err := client.DeleteContainer(ctx, firstEnvironment, &hostv1.ContainerMutationRequest{RequestId: "remove-image-user", ContainerId: container.GetId(), ExpectedGenerationId: container.GetGenerationId()}); err != nil {
		t.Fatal(err)
	}
	badDigest := deleteImageRequest("delete-bad-digest", first)
	badDigest.ExpectedDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, badDigest), "IMAGE_DIGEST_CONFLICT")
	badGeneration := deleteImageRequest("delete-bad-generation", first)
	badGeneration.ExpectedGenerationId = "stale-generation"
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, badGeneration), "IMAGE_GENERATION_CONFLICT")
	if err := client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-image-record-a", first)); err != nil {
		t.Fatal(err)
	}
	if imageByReferenceOrNil(t, client, firstEnvironment, workloadImage) != nil {
		t.Fatal("deleted namespace image record is still listed")
	}
	if imageByReferenceOrNil(t, client, secondEnvironment, workloadImage) == nil {
		t.Fatal("shared image record disappeared from the other namespace")
	}
	shared := createContainer(t, client, secondEnvironment, "shared-image-user", []string{"true"}, nil)
	if _, err := client.StartContainer(ctx, secondEnvironment, &hostv1.ContainerMutationRequest{RequestId: "start-shared-image-user", ContainerId: shared.GetId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := pullImageAttempt(ctx, client, firstEnvironment, "recreate-image-record-a"); err != nil {
		t.Fatal(err)
	}
	recreated := imageByReference(t, client, firstEnvironment, workloadImage)
	if recreated.GetDigest() != first.GetDigest() || recreated.GetGenerationId() == first.GetGenerationId() {
		t.Fatalf("recreated image identity = %+v, old = %+v", recreated, first)
	}
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-aba-image", first)), "IMAGE_GENERATION_CONFLICT")
	start := make(chan struct{})
	deleteDone := make(chan error, 1)
	createDone := make(chan error, 1)
	go func() {
		<-start
		deleteDone <- client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-image-race", recreated))
	}()
	go func() {
		<-start
		_, err := client.CreateContainer(ctx, firstEnvironment, &hostv1.CreateContainerRequest{
			RequestId: "create-image-race", ContainerId: "image-race", Image: workloadImage,
			EntrypointOverride: &hostv1.StringList{Values: []string{"true"}}, CommandOverride: &hostv1.StringList{}, LogLimitBytes: 1 << 20,
		})
		createDone <- err
	}()
	close(start)
	deleteErr, createErr := <-deleteDone, <-createDone
	if deleteErr == nil {
		if status.Code(createErr) != codes.NotFound {
			t.Fatalf("delete won but create = %v", createErr)
		}
	} else if createErr == nil {
		assertImageDeleteReason(t, deleteErr, "IMAGE_IN_USE")
		if err := client.DeleteContainer(ctx, firstEnvironment, &hostv1.ContainerMutationRequest{RequestId: "delete-image-race-container", ContainerId: "image-race"}); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatalf("image create/delete race failed on both sides: delete=%v create=%v", deleteErr, createErr)
	}
}

func imageByReference(t *testing.T, client *productClient, environmentID, reference string) *hostv1.Image {
	t.Helper()
	image := imageByReferenceOrNil(t, client, environmentID, reference)
	if image == nil {
		t.Fatalf("image %q is not listed in %s", reference, environmentID)
	}
	return image
}

func imageByReferenceOrNil(t *testing.T, client *productClient, environmentID, reference string) *hostv1.Image {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	images, err := client.ListImages(ctx, environmentID)
	if err != nil {
		t.Fatal(err)
	}
	canonical := reference
	if !strings.Contains(reference, "/") {
		canonical = "docker.io/library/" + reference
	}
	for _, image := range images.GetImages() {
		if image.GetReference() == canonical {
			if !slices.Equal(image.GetPlatforms(), []string{"linux/arm64"}) {
				t.Fatalf("image %q platforms = %v, want linux/arm64", canonical, image.GetPlatforms())
			}
			return image
		}
	}
	return nil
}

func deleteImageRequest(requestID string, image *hostv1.Image) *hostv1.DeleteImageRequest {
	return &hostv1.DeleteImageRequest{RequestId: requestID, Reference: image.GetReference(), ExpectedGenerationId: image.GetGenerationId(), ExpectedDigest: image.GetDigest()}
}

func assertImageDeleteReason(t *testing.T, err error, reason string) {
	t.Helper()
	if err == nil || !strings.Contains(fmt.Sprint(status.Convert(err).Details()), reason) {
		t.Fatalf("delete image error = %v, want %s", err, reason)
	}
}

func testPGVectorImageAndVolume(t *testing.T, client *productClient) {
	const environmentID = "pgvector"
	// Mirror the same immutable upstream image through the registry already used
	// by the E2E workload fixture. Docker Hub is intermittently unreachable from
	// the VM network and must not poison the shared namespace projection.
	const image = "mirror.gcr.io/pgvector/pgvector@sha256:386f17b2364a613752d23b4e23c6e27b87b2997b3ac3ea23dac42df579670524"
	ensureNamespace(t, client, environmentID, 2<<30)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	if digest, err := pullImageReference(ctx, client, environmentID, "pull-pgvector", image); err != nil || !strings.HasPrefix(digest, "sha256:") {
		if snapshot, snapshotErr := client.Snapshot(ctx); snapshotErr == nil {
			t.Logf("pgvector pull failure snapshot: health=%+v usage=%+v", snapshot.GetHealth(), snapshot.GetUsage())
		} else {
			t.Logf("pgvector pull failure snapshot unavailable: %v", snapshotErr)
		}
		if externalRegistryUnavailable(err) {
			t.Skipf("external pgvector registry unavailable: %v", err)
		}
		t.Fatalf("pull pgvector = %q, %v details=%v", digest, err, status.Convert(err).Details())
	}
	owner, mode := uint32(999), uint32(0o700)
	user := "999:999"
	if _, err := client.CreateVolume(ctx, environmentID, &hostv1.CreateVolumeRequest{RequestId: "create-pgvector-data", VolumeId: "data", OwnerUid: &owner, OwnerGid: &owner, Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	created, err := client.CreateContainer(ctx, environmentID, &hostv1.CreateContainerRequest{
		RequestId: "create-pgvector", ContainerId: "postgres", Image: image,
		User:    &user,
		Env:     []string{"POSTGRES_PASSWORD=agent-vmm-e2e", "PGDATA=/var/lib/postgresql/data/pgdata"},
		Volumes: []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/var/lib/postgresql/data"}}, LogLimitBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := client.StartContainer(ctx, environmentID, &hostv1.ContainerMutationRequest{RequestId: "start-pgvector", ContainerId: created.GetContainer().GetId()})
	if err != nil || started.GetContainer().GetInstanceId() == "" {
		t.Fatalf("start pgvector = %+v, %v", started.GetContainer(), err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		current, err := client.GetContainer(ctx, environmentID, &hostv1.ContainerRequest{ContainerId: "postgres"})
		if err != nil {
			t.Fatal(err)
		}
		if current.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING {
			t.Fatalf("pgvector stopped before readiness: %q", readLogs(t, client, environmentID, "postgres"))
		}
		output, code := execContainer(t, client, environmentID, "postgres", []string{"pg_isready", "-U", "postgres"})
		if code == 0 && bytes.Contains(output, []byte("accepting connections")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pgvector did not become ready: %q", output)
		}
		time.Sleep(time.Second)
	}
	stopDeleteContainer(t, client, environmentID, "postgres")
	deleteVolume(t, client, environmentID, "data")
	deleteNamespace(t, client, environmentID)
}

func externalRegistryUnavailable(err error) bool {
	if status.Code(err) != codes.Unavailable {
		return false
	}
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		switch info.GetReason() {
		case "IMAGE_REGISTRY_UNAVAILABLE", "IMAGE_AUTH_UNAVAILABLE", "IMAGE_DOWNLOAD_TIMEOUT",
			"IMAGE_DOWNLOAD_INTERRUPTED", "IMAGE_MANIFEST_FETCH_FAILED":
			return true
		}
	}
	return false
}

func TestExternalRegistryUnavailable(t *testing.T) {
	registry, err := status.New(codes.Unavailable, "image registry could not be reached").WithDetails(
		&errdetails.ErrorInfo{Reason: "IMAGE_REGISTRY_UNAVAILABLE", Domain: "agent-vmm"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !externalRegistryUnavailable(registry.Err()) {
		t.Fatal("registry-specific unavailable error was not classified as external")
	}
	blobFailure, err := status.New(codes.Unavailable, "image content could not be downloaded").WithDetails(
		&errdetails.ErrorInfo{Reason: "IMAGE_BLOB_FETCH_FAILED", Domain: "agent-vmm"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if externalRegistryUnavailable(blobFailure.Err()) {
		t.Fatal("unclassified image blob failure was incorrectly classified as external registry failure")
	}
	if externalRegistryUnavailable(status.Error(codes.Unavailable, "guest control service is unavailable")) {
		t.Fatal("generic VM/control unavailable error was classified as external registry failure")
	}
}

func testCleanupContract(t *testing.T, harness *hostHarness, client *productClient) {
	ensureNamespace(t, client, "cleanup-source", 64<<20)
	ensureNamespace(t, client, "cleanup-target", 64<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	beforePolicy, err := client.operator.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	invalidLimits := &hostv1.ResourceLimits{CpuMillis: optional(uint32(1000))}
	if _, err := client.operator.UpdateEnvironmentPolicy(ctx, &hostv1.UpdateEnvironmentPolicyRequest{
		RequestId: "cleanup-invalid-policy", EnvironmentId: "cleanup-source", Limits: invalidLimits,
		EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, ExpectedRevision: beforePolicy.GetEnvironment().GetPolicyRevision(),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid policy error = %v", err)
	}
	afterPolicy, err := client.operator.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil || afterPolicy.GetEnvironment().GetPolicyRevision() != beforePolicy.GetEnvironment().GetPolicyRevision() {
		t.Fatalf("invalid policy changed revision: before=%v after=%v err=%v", beforePolicy.GetEnvironment().GetPolicyRevision(), afterPolicy.GetEnvironment().GetPolicyRevision(), err)
	}
	database, err := openE2EDatabase(harness.database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	link := &hostv1.NetworkLink{Id: "cleanup-link", SourceEnvironmentId: "cleanup-source", DestinationEnvironmentId: "cleanup-target", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "cleanup-service"}
	invalidLink := proto.Clone(link).(*hostv1.NetworkLink)
	invalidLink.Id = "cleanup-invalid-link"
	invalidLink.DnsAlias = "Invalid_Alias"
	if err := client.ApplyNetworkLink(ctx, &hostv1.ApplyNetworkLinkRequest{RequestId: "cleanup-invalid-link", Link: invalidLink}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid network link error = %v", err)
	}
	var invalidGrantCount int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE id='cleanup-invalid-link'").Scan(&invalidGrantCount); err != nil || invalidGrantCount != 0 {
		t.Fatalf("invalid network link persisted: count=%d err=%v", invalidGrantCount, err)
	}
	if err := client.ApplyNetworkLink(ctx, &hostv1.ApplyNetworkLinkRequest{RequestId: "cleanup-link-apply", Link: link}); err != nil {
		t.Fatal(err)
	}
	result, err := database.ExecContext(ctx, "UPDATE grants SET revoked_at=1 WHERE id='cleanup-link'")
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("mark cleanup grant tombstone: rows=%d err=%v", rows, err)
	}
	session, err := client.session("cleanup-source")
	if err != nil {
		t.Fatal(err)
	}
	agentPlan, err := session.client.PlanResetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
	if err != nil || agentPlan.GetPlan().GetKind() != hostv1.CleanupKind_CLEANUP_KIND_RESET {
		t.Fatalf("agent reset plan = %v, %v", agentPlan, err)
	}
	malformed := proto.Clone(agentPlan.GetPlan()).(*hostv1.CleanupPlan)
	malformed.Kind = hostv1.CleanupKind_CLEANUP_KIND_UNSPECIFIED
	if _, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-malformed", Plan: malformed}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed cleanup error = %v", err)
	}
	reset, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-reset", Plan: agentPlan.GetPlan()})
	if err != nil || reset.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("reset = %v, %v", reset, err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE operations SET state=?, stage='failed', error_reason='TEST_FAILURE' WHERE request_id='cleanup-reset'", int32(hostv1.OperationState_OPERATION_STATE_FAILED)); err != nil {
		t.Fatal(err)
	}
	retried, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-reset", Plan: agentPlan.GetPlan()})
	if err != nil || retried.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED || retried.GetOperation().GetId() != reset.GetOperation().GetId() {
		t.Fatalf("cleanup retry = %v, %v", retried, err)
	}
	client.forgetSession("cleanup-source")
	if _, err := client.EnsureEnvironment(ctx, "cleanup-source", "cleanup-reconnect", &hostv1.ResourceLimits{CpuMillis: optional(uint32(1000)), MemoryBytes: optional(uint64(3 << 30)), Pids: optional(uint32(512)), DiskBytes: optional(uint64(64 << 20))}, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatal(err)
	}
	link.TcpPorts = []uint32{9090}
	if err := client.ApplyNetworkLink(ctx, &hostv1.ApplyNetworkLinkRequest{RequestId: "cleanup-link-reapply", Link: link}); err != nil {
		t.Fatalf("stale guest link survived reset: %v", err)
	}
	if _, err := client.CreateVolume(ctx, "cleanup-source", &hostv1.CreateVolumeRequest{RequestId: "cleanup-volume", VolumeId: "retained"}); err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveEnvironmentAccess(ctx, "cleanup-source", "cleanup-remove-access"); err != nil {
		t.Fatal(err)
	}
	stalePlan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	currentPolicy, err := client.operator.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.operator.UpdateEnvironmentPolicy(ctx, &hostv1.UpdateEnvironmentPolicyRequest{RequestId: "cleanup-plan-change", EnvironmentId: "cleanup-source", Limits: currentPolicy.GetEnvironment().GetLimits(), EgressMode: currentPolicy.GetEnvironment().GetEgressMode(), ExpectedRevision: currentPolicy.GetEnvironment().GetPolicyRevision()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-stale-plan", Plan: stalePlan.GetPlan(), ConfirmedVolumeIds: []string{"retained"}}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale cleanup plan error = %v", err)
	}
	plan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-discard", Plan: plan.GetPlan()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("discard without volume confirmation error = %v", err)
	}
	discard, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-discard", Plan: plan.GetPlan(), ConfirmedVolumeIds: []string{"retained"}})
	if err != nil || discard.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("discard with active link = %v, %v", discard, err)
	}
	client.forgetSession("cleanup-source")
	environment := afterPolicy.GetEnvironment()
	now := time.Now().UnixNano()
	if _, err := database.ExecContext(ctx, `INSERT INTO environments
		(id, namespace_id, display_name, cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, policy_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, environment.GetId(), environment.GetNamespaceId(), environment.GetDisplayName(),
		environment.GetLimits().GetCpuMillis(), environment.GetLimits().GetMemoryBytes(), environment.GetLimits().GetPids(), environment.GetLimits().GetDiskBytes(),
		int32(environment.GetEgressMode()), environment.GetPolicyRevision(), now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE operations SET state=?, stage='namespace-deleted', error_reason='TEST_INTERRUPTION' WHERE request_id='cleanup-discard'", int32(hostv1.OperationState_OPERATION_STATE_FAILED)); err != nil {
		t.Fatal(err)
	}
	recovered, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-discard", Plan: plan.GetPlan(), ConfirmedVolumeIds: []string{"retained"}})
	if err != nil || recovered.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED || recovered.GetOperation().GetId() != discard.GetOperation().GetId() {
		t.Fatalf("partial discard recovery = %v, %v", recovered, err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO environments
		(id, namespace_id, display_name, cpu_millis, memory_bytes, pids, disk_bytes, egress_mode, policy_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, environment.GetId(), environment.GetNamespaceId(), environment.GetDisplayName(),
		environment.GetLimits().GetCpuMillis(), environment.GetLimits().GetMemoryBytes(), environment.GetLimits().GetPids(), environment.GetLimits().GetDiskBytes(),
		int32(environment.GetEgressMode()), environment.GetPolicyRevision(), now+1, now+1); err != nil {
		t.Fatal(err)
	}
	freshPlan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatalf("fresh plan after missing namespace = %v", err)
	}
	freshRecovery, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-discard-fresh-recovery", Plan: freshPlan.GetPlan()})
	if err != nil || freshRecovery.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("fresh request recovery after missing namespace = %v, %v", freshRecovery, err)
	}
	if _, err := client.operator.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: "cleanup-source"}); status.Code(err) != codes.NotFound {
		t.Fatalf("partial discard left host environment: %v", err)
	}
	var failed, succeeded int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit WHERE method='ExecuteCleanup' AND result_code=?", int32(codes.InvalidArgument)).Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit WHERE method='ExecuteCleanup' AND result_code=?", int32(codes.OK)).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if failed == 0 || succeeded == 0 {
		t.Fatalf("cleanup audit failure/success rows = %d/%d", failed, succeeded)
	}
}

func testSessionAuthAndRevocation(t *testing.T, harness *hostHarness, client *productClient) {
	ensureNamespace(t, client, "session-auth", 64<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := client.session("session-auth")
	if err != nil {
		t.Fatal(err)
	}
	created, err := harness.client.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: "second-session", IntegrationId: first.integrationID})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient("passthrough:///agent", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", created.GetSocketPath())
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	agent := hostv1.NewAgentRuntimeServiceClient(connection)
	forged := metadata.NewOutgoingContext(ctx, metadata.Pairs("actor_namespace_id", "forged", "role", "operator"))
	environment, err := agent.GetEnvironment(forged, &hostv1.GetOwnEnvironmentRequest{})
	projection := environment.GetEnvironment()
	if err != nil || projection.GetId() != "" || projection.GetNamespaceId() != "" || projection.GetOwner() != nil || projection.GetOrigin() != hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL {
		t.Fatalf("session identity = %+v, %v", environment, err)
	}
	if _, err := hostv1.NewOperatorServiceClient(connection).GetVM(ctx, &hostv1.GetVMRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("operator API exposed on agent socket: %v", err)
	}
	if _, err := harness.client.RevokeAgentSession(ctx, &hostv1.RevokeAgentSessionRequest{RequestId: "revoke-second-session", SessionId: created.GetSessionId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{}); err == nil {
		t.Fatal("revoked session remained usable")
	}
	deleteNamespace(t, client, "session-auth")
}

func testBoot(t *testing.T) {
	harness := startHost(t)
	vm := harness.start(t)
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING || vm.GetBootId() == "" {
		t.Fatalf("StartVM() = %+v", vm)
	}
	if _, err := os.Stat(harness.runtimeSocket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw runtime socket is exposed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	snapshot, err := harness.client.GetSystemSnapshot(ctx, &hostv1.GetSystemSnapshotRequest{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.GetUsage() != nil && (snapshot.GetUsage().GetSampledAt() == nil || snapshot.GetUsage().GetStale() && snapshot.GetUsage().GetUnavailableReason() == "") {
		t.Fatalf("system network usage = %+v", snapshot.GetUsage())
	}
	previousBootID := vm.GetBootId()
	for cycle := 1; cycle < 20; cycle++ {
		harness.stop(t, false)
		vm = harness.start(t)
		if vm.GetBootId() == previousBootID {
			t.Fatalf("cycle %d reused boot ID", cycle)
		}
		previousBootID = vm.GetBootId()
	}
	harness.stop(t, false)
}

func testStateInitialization(t *testing.T) {
	harness := startHost(t)
	if err := os.MkdirAll(harness.instance, 0o700); err != nil {
		t.Fatal(err)
	}
	state, err := os.OpenFile(filepath.Join(harness.instance, "state.raw"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Truncate(1 << 30); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	marker, err := os.OpenFile(filepath.Join(harness.instance, "state.initializing"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	harness.start(t)
	if _, err := os.Stat(filepath.Join(harness.instance, "state.initializing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initialization marker remains: %v", err)
	}
	info, err := os.Stat(filepath.Join(harness.instance, "state.raw"))
	if err != nil || info.Size() != 1<<30 {
		t.Fatalf("state disk = %v, %v", info, err)
	}
	harness.stop(t, true)

	state, err = os.OpenFile(filepath.Join(harness.instance, "state.raw"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.WriteAt(make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := state.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := harness.client.StartVM(ctx, &hostv1.StartVMRequest{}); err == nil {
		t.Fatal("corrupt initialized state disk was reformatted")
	}
	if _, err := os.Stat(filepath.Join(harness.instance, "state.initializing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt disk recreated initialization marker: %v", err)
	}
}

func testContainerLifecycle(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "lifecycle", 128<<20)
	pullImage(t, client, "lifecycle")
	container := createContainer(t, client, "lifecycle", "persistent", []string{"/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"}, nil)
	startContainer(t, client, "lifecycle", container.GetId())
	if output, code := execContainer(t, client, "lifecycle", container.GetId(), []string{"/bin/sh", "-c", "echo snapshot > /root/value && echo hello"}); code != 0 || string(output) != "hello\n" {
		t.Fatalf("exec = %q, exit %d", output, code)
	}
	if output, code := execContainer(t, client, "lifecycle", container.GetId(), []string{"cat", "/root/value"}); code != 0 || string(output) != "snapshot\n" {
		t.Fatalf("live root = %q, exit %d", output, code)
	}
	stopContainer(t, client, "lifecycle", container.GetId())
	startContainer(t, client, "lifecycle", container.GetId())
	if output, code := execContainer(t, client, "lifecycle", container.GetId(), []string{"cat", "/root/value"}); code != 0 || string(output) != "snapshot\n" {
		t.Fatalf("persistent root = %q, exit %d", output, code)
	}
	stopContainer(t, client, "lifecycle", container.GetId())
	deleteContainer(t, client, "lifecycle", container.GetId())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	limited, err := client.CreateContainer(ctx, "lifecycle", &hostv1.CreateContainerRequest{
		RequestId: "create-limited-log", ContainerId: "limited-log", Image: workloadImage,
		EntrypointOverride: &hostv1.StringList{Values: []string{"/bin/sh", "-c", "dd if=/dev/zero bs=8192 count=1 2>/dev/null | tr '\\000' x"}}, CommandOverride: &hostv1.StringList{}, LogLimitBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	startContainer(t, client, "lifecycle", limited.GetContainer().GetId())
	waitContainer(t, client, "lifecycle", limited.GetContainer().GetId(), 0)
	stream, err := client.Logs(ctx, "lifecycle", &hostv1.LogsRequest{ContainerId: limited.GetContainer().GetId()})
	if err != nil {
		t.Fatal(err)
	}
	truncated := false
	for {
		record, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		truncated = truncated || record.GetTruncated() && record.GetDroppedRecords() > 0
	}
	if !truncated {
		t.Fatal("bounded log did not report truncation and dropped records")
	}
	deleteContainer(t, client, "lifecycle", limited.GetContainer().GetId())
	deleteNamespace(t, client, "lifecycle")
}

func testNamedVolume(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "volume-owner", 128<<20)
	ensureNamespace(t, client, "volume-other", 128<<20)
	pullImage(t, client, "volume-owner")
	pullImage(t, client, "volume-other")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	createVolume(t, client, "volume-owner", "data")
	mount := []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/data"}}
	writer := createContainer(t, client, "volume-owner", "writer", []string{"/bin/sh", "-c", "echo durable > /data/value"}, mount)
	startContainer(t, client, "volume-owner", writer.GetId())
	waitContainer(t, client, "volume-owner", writer.GetId(), 0)
	deleteContainer(t, client, "volume-owner", writer.GetId())
	reader := createContainer(t, client, "volume-owner", "reader", []string{"cat", "/data/value"}, mount)
	startContainer(t, client, "volume-owner", reader.GetId())
	waitContainer(t, client, "volume-owner", reader.GetId(), 0)
	if logs := readLogs(t, client, "volume-owner", reader.GetId()); !bytes.Contains(logs, []byte("durable")) {
		t.Fatalf("volume contents = %q", logs)
	}
	if err := client.DeleteVolume(ctx, "volume-owner", &hostv1.DeleteVolumeRequest{RequestId: "delete-mounted-data", VolumeId: "data"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mounted volume delete error = %v", err)
	}
	deleteContainer(t, client, "volume-owner", reader.GetId())
	if _, err := client.CreateContainer(ctx, "volume-other", &hostv1.CreateContainerRequest{RequestId: "cross-mount", ContainerId: "cross", Image: workloadImage, EntrypointOverride: &hostv1.StringList{Values: []string{"true"}}, CommandOverride: &hostv1.StringList{}, Volumes: mount, LogLimitBytes: 1 << 20}); err == nil {
		t.Fatal("cross-namespace volume mount succeeded")
	}
	listed, err := client.ListContainers(ctx, "volume-other")
	if err != nil || len(listed.GetContainers()) != 0 {
		t.Fatalf("other namespace containers = %+v, %v", listed, err)
	}
	deleteVolume(t, client, "volume-owner", "data")
	deleteNamespace(t, client, "volume-owner")
	deleteNamespace(t, client, "volume-other")
}

func testDiskQuota(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "quota", 32<<20)
	ensureNamespace(t, client, "healthy", 64<<20)
	ensureNamespace(t, client, "shared-small", 2<<20)
	pullImage(t, client, "quota")
	pullImage(t, client, "healthy")
	if err := pullImageError(client, "shared-small"); err == nil || !strings.Contains(err.Error(), "ResourceExhausted") {
		t.Fatalf("shared content bypassed logical quota: %v", err)
	}
	createVolume(t, client, "quota", "data")
	full := createContainer(t, client, "quota", "full", []string{"dd", "if=/dev/zero", "of=/data/blob", "bs=1048576", "count=64"}, []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	startContainer(t, client, "quota", full.GetId())
	if code := waitContainer(t, client, "quota", full.GetId(), 1); code == 0 {
		t.Fatal("write beyond project quota succeeded")
	}
	healthy := createContainer(t, client, "healthy", "healthy", []string{"/bin/sh", "-c", "echo unaffected"}, nil)
	startContainer(t, client, "healthy", healthy.GetId())
	waitContainer(t, client, "healthy", healthy.GetId(), 0)
	if logs := readLogs(t, client, "healthy", healthy.GetId()); !bytes.Contains(logs, []byte("unaffected")) {
		t.Fatalf("healthy namespace logs = %q", logs)
	}
	deleteContainer(t, client, "quota", full.GetId())
	deleteContainer(t, client, "healthy", healthy.GetId())
	deleteVolume(t, client, "quota", "data")
	deleteNamespace(t, client, "quota")
	deleteNamespace(t, client, "healthy")
	deleteNamespace(t, client, "shared-small")
}

func testSameNamespace(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "same", 128<<20)
	pullImage(t, client, "same")
	server := createContainer(t, client, "same", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "same", "caller", idleProcess(), nil)
	startContainer(t, client, "same", server.GetId())
	startContainer(t, client, "same", caller.GetId())
	if output, code := execContainer(t, client, "same", caller.GetId(), []string{"sh", "-c", "printf same | nc -w 5 server 8080"}); code != 0 || string(output) != "same" {
		t.Fatalf("same namespace request = %q, exit %d", output, code)
	}
	stopDeleteContainer(t, client, "same", caller.GetId())
	stopDeleteContainer(t, client, "same", server.GetId())
	deleteNamespace(t, client, "same")
}

func testDefaultIsolation(t *testing.T, client *productClient) {
	for _, namespaceID := range []string{"isolated-source", "isolated-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		pullImage(t, client, namespaceID)
	}
	server := createContainer(t, client, "isolated-destination", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "isolated-source", "caller", idleProcess(), nil)
	startContainer(t, client, "isolated-destination", server.GetId())
	startContainer(t, client, "isolated-source", caller.GetId())
	address := containerAddress(t, client, "isolated-destination", server.GetId())
	if _, code := execContainer(t, client, "isolated-source", caller.GetId(), []string{"nc", "-z", "-w", "2", address, "8080"}); code == 0 {
		t.Fatal("cross-namespace connection succeeded without a link")
	}
	stopDeleteContainer(t, client, "isolated-source", caller.GetId())
	stopDeleteContainer(t, client, "isolated-destination", server.GetId())
	deleteNamespace(t, client, "isolated-source")
	deleteNamespace(t, client, "isolated-destination")
}

func testPartialLink(t *testing.T, client *productClient) {
	for _, namespaceID := range []string{"link-source", "link-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		pullImage(t, client, namespaceID)
	}
	server := createContainer(t, client, "link-destination", "server", []string{"sh", "-c", "nc -lk -p 8080 -e /bin/cat & nc -lk -p 9090 -e /bin/cat & wait"}, nil)
	other := createContainer(t, client, "link-destination", "other", echoServer(8080), nil)
	caller := createContainer(t, client, "link-source", "caller", idleProcess(), nil)
	unselected := createContainer(t, client, "link-source", "unselected", idleProcess(), nil)
	for _, item := range []struct{ namespace, id string }{{"link-destination", server.GetId()}, {"link-destination", other.GetId()}, {"link-source", caller.GetId()}, {"link-source", unselected.GetId()}} {
		startContainer(t, client, item.namespace, item.id)
	}
	applyLink(t, client, &hostv1.NetworkLink{Id: "partial", SourceEnvironmentId: "link-source", DestinationEnvironmentId: "link-destination", Source: idsSelector(caller.GetId()), Destination: idsSelector(server.GetId()), TcpPorts: []uint32{8080}, DnsAlias: "service"})
	if output, code := execContainer(t, client, "link-source", caller.GetId(), []string{"sh", "-c", "printf linked | nc -w 5 service 8080"}); code != 0 || string(output) != "linked" {
		t.Fatalf("allowed link = %q, exit %d", output, code)
	}
	serverIP := containerAddress(t, client, "link-destination", server.GetId())
	otherIP := containerAddress(t, client, "link-destination", other.GetId())
	for _, denial := range []struct{ container, address, port string }{{caller.GetId(), serverIP, "9090"}, {caller.GetId(), otherIP, "8080"}, {unselected.GetId(), serverIP, "8080"}} {
		if _, code := execContainer(t, client, "link-source", denial.container, []string{"nc", "-z", "-w", "2", denial.address, denial.port}); code == 0 {
			t.Fatalf("selector/port denial failed for %+v", denial)
		}
	}
	for _, item := range []struct{ namespace, id string }{{"link-source", unselected.GetId()}, {"link-source", caller.GetId()}, {"link-destination", other.GetId()}, {"link-destination", server.GetId()}} {
		stopDeleteContainer(t, client, item.namespace, item.id)
	}
	deleteLink(t, client, "partial")
	deleteNamespace(t, client, "link-source")
	deleteNamespace(t, client, "link-destination")
}

func testLinkRevoke(t *testing.T, client *productClient) {
	for _, namespaceID := range []string{"revoke-source", "revoke-destination"} {
		ensureNamespace(t, client, namespaceID, 128<<20)
		pullImage(t, client, namespaceID)
	}
	server := createContainer(t, client, "revoke-destination", "server", echoServer(8080), nil)
	caller := createContainer(t, client, "revoke-source", "caller", idleProcess(), nil)
	startContainer(t, client, "revoke-destination", server.GetId())
	startContainer(t, client, "revoke-source", caller.GetId())
	applyLink(t, client, &hostv1.NetworkLink{Id: "revocable", SourceEnvironmentId: "revoke-source", DestinationEnvironmentId: "revoke-destination", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "service"})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := client.Exec(ctx, "revoke-source")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{RequestId: "revoke-exec", ContainerId: caller.GetId(), Argv: []string{"nc", "service", "8080"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("before")}}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || string(response.GetStdout()) != "before" {
		t.Fatalf("established link = %q, %v", response.GetStdout(), err)
	}
	deleteLink(t, client, "revocable")
	_ = stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("after")}})
	if response, err := stream.Recv(); err == nil && bytes.Contains(response.GetStdout(), []byte("after")) {
		t.Fatal("established connection remained usable after link deletion")
	}
	stopDeleteContainer(t, client, "revoke-source", caller.GetId())
	stopDeleteContainer(t, client, "revoke-destination", server.GetId())
	deleteNamespace(t, client, "revoke-source")
	deleteNamespace(t, client, "revoke-destination")
}

func testEgress(t *testing.T, client *productClient) {
	ensureNamespaceWithEgress(t, client, "egress-deny", 128<<20, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
	ensureNamespaceWithEgress(t, client, "egress-public", 128<<20, hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET)
	for _, namespaceID := range []string{"egress-deny", "egress-public"} {
		pullImage(t, client, namespaceID)
	}
	deny := createContainer(t, client, "egress-deny", "caller", idleProcess(), nil)
	public := createContainer(t, client, "egress-public", "caller", idleProcess(), nil)
	startContainer(t, client, "egress-deny", deny.GetId())
	startContainer(t, client, "egress-public", public.GetId())
	if _, code := execContainer(t, client, "egress-deny", deny.GetId(), []string{"wget", "-qO-", "-T", "3", "https://example.com"}); code == 0 {
		t.Fatal("DENY_ALL reached the public internet")
	}
	if _, code := execContainer(t, client, "egress-deny", deny.GetId(), []string{"nslookup", "example.com", "1.1.1.1"}); code == 0 {
		t.Fatal("DENY_ALL reached public DNS over UDP")
	}
	if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"sh", "-c", "for attempt in 1 2 3 4 5 6 7 8; do nslookup example.com && exit 0; sleep 3; done; exit 1"}); code != 0 {
		t.Fatal("PUBLIC_INTERNET could not use virtual DNS over UDP")
	}
	if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"sh", "-c", "for attempt in 1 2 3 4 5 6 7 8; do nslookup example.com 1.1.1.1 && exit 0; sleep 3; done; exit 1"}); code != 0 {
		t.Fatal("PUBLIC_INTERNET could not reach public DNS over UDP")
	}
	for _, address := range []string{"192.168.1.1", "169.254.169.254", "100.127.255.1"} {
		if _, code := execContainer(t, client, "egress-public", public.GetId(), []string{"nc", "-z", "-w", "2", address, "80"}); code == 0 {
			t.Fatalf("PUBLIC_INTERNET reached blocked destination %s", address)
		}
	}
	stopDeleteContainer(t, client, "egress-deny", deny.GetId())
	stopDeleteContainer(t, client, "egress-public", public.GetId())
	deleteNamespace(t, client, "egress-deny")
	deleteNamespace(t, client, "egress-public")
}

func testPortForward(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "forward", 128<<20)
	pullImage(t, client, "forward")
	server := createContainer(t, client, "forward", "server", echoServer(8080), nil)
	startContainer(t, client, "forward", server.GetId())
	waitForContainerPort(t, client, "forward", server.GetId(), 8080)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.ForwardTCP(ctx, "forward")
	if err != nil {
		t.Fatal(err)
	}
	running, err := client.GetContainer(ctx, "forward", &hostv1.ContainerRequest{ContainerId: server.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Header{Header: &hostv1.ForwardTCPHeader{ContainerId: server.GetId(), Port: 8080, ContainerInstanceId: running.GetContainer().GetInstanceId()}}}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("forward-data-"), 8192)
	for len(payload) != 0 {
		count := min(len(payload), 32<<10)
		if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Data{Data: payload[:count]}}); err != nil {
			t.Fatal(err)
		}
		payload = payload[count:]
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}}); err != nil {
		t.Fatal(err)
	}
	var received bytes.Buffer
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if response.GetCloseRead() {
			break
		}
		received.Write(response.GetData())
	}
	want := bytes.Repeat([]byte("forward-data-"), 8192)
	if !bytes.Equal(received.Bytes(), want) {
		t.Fatalf("forwarded %d bytes, want %d", received.Len(), len(want))
	}
	stopDeleteContainer(t, client, "forward", server.GetId())
	deleteNamespace(t, client, "forward")
}

func testDockerfileBuild(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "dockerfile-build", 768<<20)
	tool := buildShell(t)
	contextArchive := dockerfileContext(t, tool)
	secret := []byte("build-secret-7a5f07acbcaf")
	image := "agent.local/dockerfile-result:latest"
	first := buildImage(t, client, "dockerfile-build", image, contextArchive, secret)
	second := buildImage(t, client, "dockerfile-build", image, contextArchive, secret)
	if first.digest != second.digest {
		t.Fatalf("cached build digest changed: %s != %s", first.digest, second.digest)
	}
	if !slices.Contains(second.progress, "namespace cache hit") {
		t.Fatalf("second build progress = %q", second.progress)
	}
	container := createContainerWithImage(t, client, "dockerfile-build", "result", image, []string{"/bin/result", "--assert-absent", string(secret)})
	startContainer(t, client, "dockerfile-build", container.GetId())
	waitContainer(t, client, "dockerfile-build", container.GetId(), 0)
	if output := readLogs(t, client, "dockerfile-build", container.GetId()); string(output) != "build-ok\n" {
		t.Fatalf("built image output = %q", output)
	}
	deleteContainer(t, client, "dockerfile-build", container.GetId())
	deleteNamespace(t, client, "dockerfile-build")
}

func testParallelAgents(t *testing.T, client *productClient) {
	namespaces := []string{"parallel-a", "parallel-b"}
	for _, namespaceID := range namespaces {
		ensureNamespace(t, client, namespaceID, 1<<30)
	}
	contextArchive := dockerfileContext(t, buildShell(t))
	secret := []byte("parallel-build-secret-e291d1")
	type outcome struct {
		namespaceID string
		result      imageBuildResult
		err         error
	}
	outcomes := make(chan outcome, len(namespaces))
	for _, namespaceID := range namespaces {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			result, err := buildImageRequest(ctx, client, namespaceID, "agent.local/shared-name:latest", "build-ok", hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, secret)
			outcomes <- outcome{namespaceID: namespaceID, result: result, err: err}
		}()
	}
	for range namespaces {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatalf("build %s: %v", outcome.namespaceID, outcome.err)
		}
		if slices.Contains(outcome.result.progress, "namespace cache hit") {
			t.Fatalf("namespace %s reused another namespace's cache", outcome.namespaceID)
		}
	}
	for _, namespaceID := range namespaces {
		container := createContainerWithImage(t, client, namespaceID, "result", "agent.local/shared-name:latest", []string{"/bin/result"})
		startContainer(t, client, namespaceID, container.GetId())
	}
	for _, namespaceID := range namespaces {
		waitContainer(t, client, namespaceID, "result", 0)
		if output := readLogs(t, client, namespaceID, "result"); string(output) != "build-ok\n" {
			t.Fatalf("namespace %s output = %q", namespaceID, output)
		}
		deleteContainer(t, client, namespaceID, "result")
		createContainerWithImage(t, client, namespaceID, "worker", "agent.local/shared-name:latest", []string{"/bin/result", "--hold"})
		startContainer(t, client, namespaceID, "worker")
	}
	for index := len(namespaces) - 1; index >= 0; index-- {
		stopDeleteContainer(t, client, namespaces[index], "worker")
		deleteNamespace(t, client, namespaces[index])
	}
}

func testCancellationCleanup(t *testing.T, client *productClient) {
	namespaceID := "cancel-build"
	ensureNamespace(t, client, namespaceID, 768<<20)
	contextArchive := dockerfileContext(t, buildShell(t))
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.BuildImage(ctx, namespaceID)
	if err != nil {
		t.Fatal(err)
	}
	header := &hostv1.BuildImageHeader{
		RequestId: "cancel-build", DockerfilePath: "Dockerfile",
		Image: "agent.local/cancelled:latest", NetworkMode: hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NAMESPACE_EGRESS,
		BuildArgs: []*hostv1.BuildArgument{{Name: "MESSAGE", Value: "build-cancel"}}, Secrets: []*hostv1.BuildSecret{{Id: "token"}},
	}
	if err := sendBuildInput(stream, header, contextArchive, []byte("build-cancel-secret-053f")); err != nil {
		t.Fatal(err)
	}
	started := false
	for !started {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		started = response.GetProgress() == "builder task started"
	}
	cancelledAt := time.Now()
	cancel()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("cancelled build completed without an error")
	}
	if elapsed := time.Since(cancelledAt); elapsed > 20*time.Second {
		t.Fatalf("build cancellation took %s", elapsed)
	}
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer checkCancel()
	listed, err := client.ListContainers(checkCtx, namespaceID)
	if err != nil || len(listed.GetContainers()) != 0 {
		t.Fatalf("containers after cancellation = %+v, %v", listed.GetContainers(), err)
	}
	if _, err := client.CreateContainer(checkCtx, namespaceID, &hostv1.CreateContainerRequest{
		RequestId: "cancelled-image", ContainerId: "partial", Image: "agent.local/cancelled:latest", EntrypointOverride: &hostv1.StringList{Values: []string{"/bin/result"}}, CommandOverride: &hostv1.StringList{}, LogLimitBytes: 1 << 20,
	}); err == nil {
		t.Fatal("cancelled image was imported")
	}
	deleteNamespaceEventually(t, client, namespaceID)
	ensureNamespace(t, client, "quota-build", 4<<20)
	quotaCtx, quotaCancel := context.WithTimeout(context.Background(), time.Minute)
	_, err = buildImageRequest(quotaCtx, client, "quota-build", "agent.local/quota:latest", "build-ok", hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, []byte("quota-secret"))
	quotaCancel()
	if err == nil {
		t.Fatal("build exceeded its namespace disk quota")
	}
	deleteNamespaceEventually(t, client, "quota-build")
}

func testVMRestart(t *testing.T, harness *hostHarness) {
	client := harness.runtime(t)
	for _, namespaceID := range []string{"restart-source", "restart-destination"} {
		ensureNamespace(t, client, namespaceID, 256<<20)
		pullImage(t, client, namespaceID)
	}
	createVolume(t, client, "restart-source", "data")
	caller := createContainer(t, client, "restart-source", "caller", idleProcess(), []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	server := createContainer(t, client, "restart-destination", "server", idleProcess(), nil)
	startContainer(t, client, "restart-source", caller.GetId())
	startContainer(t, client, "restart-destination", server.GetId())
	startEchoServer(t, client, "restart-destination", server.GetId(), 8080)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "echo restart-durable > /data/value; echo root-durable > /root/value; sync; cat /data/value"}); code != 0 || string(output) != "restart-durable\n" {
		t.Fatalf("fsync volume write = %q, exit %d", output, code)
	}
	link := &hostv1.NetworkLink{Id: "restart-link", SourceEnvironmentId: "restart-source", DestinationEnvironmentId: "restart-destination", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "service"}
	applyLink(t, client, link)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "printf before | nc -w 5 service 8080"}); code != 0 || string(output) != "before" {
		t.Fatalf("link before restart = %q, exit %d", output, code)
	}
	eventCtx, eventCancel := context.WithTimeout(context.Background(), 30*time.Second)
	events, err := harness.client.WatchEvents(eventCtx, &hostv1.WatchEventsRequest{})
	if err != nil {
		eventCancel()
		t.Fatal(err)
	}
	beforeRestartEvent, err := events.Recv()
	if err != nil || beforeRestartEvent.GetGuestBootId() == "" || beforeRestartEvent.GetSequence() == 0 {
		eventCancel()
		t.Fatalf("event before restart = %+v, %v", beforeRestartEvent, err)
	}
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer streamCancel()
	stream, err := client.Exec(streamCtx, "restart-source")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{RequestId: "restart-connection", ContainerId: caller.GetId(), Argv: []string{"nc", "service", "8080"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("open")}}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || string(response.GetStdout()) != "open" {
		t.Fatalf("connection before restart = %q, %v", response.GetStdout(), err)
	}
	tty, err := client.Exec(streamCtx, "restart-source")
	if err != nil {
		t.Fatal(err)
	}
	if err := tty.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{RequestId: "restart-tty", ContainerId: caller.GetId(), Argv: []string{"sh"}, Tty: true, Rows: 24, Columns: 80}}}); err != nil {
		t.Fatal(err)
	}
	if err := tty.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte("echo tty-open\n")}}); err != nil {
		t.Fatal(err)
	}
	for {
		response, err := tty.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(response.GetStdout(), []byte("tty-open")) {
			break
		}
	}
	harness.stop(t, false)
	for {
		if _, err := events.Recv(); err != nil {
			break
		}
	}
	eventCancel()
	harness.start(t)
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), 30*time.Second)
	snapshot, err := client.Snapshot(snapshotCtx)
	snapshotCancel()
	if err != nil || snapshot.GetVm().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		t.Fatalf("snapshot after event disconnect = %+v, %v", snapshot.GetVm(), err)
	}
	reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer reconnectCancel()
	reconnected, err := harness.client.WatchEvents(reconnectCtx, &hostv1.WatchEventsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	afterRestartEvent, err := reconnected.Recv()
	if err != nil || afterRestartEvent.GetGuestBootId() == "" || afterRestartEvent.GetGuestBootId() == beforeRestartEvent.GetGuestBootId() || afterRestartEvent.GetSequence() == 0 {
		t.Fatalf("event after snapshot-first reconnect = %+v, %v", afterRestartEvent, err)
	}
	for name, oldStream := range map[string]grpc.BidiStreamingClient[hostv1.ExecRequest, hostv1.ExecResponse]{"exec connection": stream, "TTY": tty} {
		marker := []byte("recovered-" + strings.ReplaceAll(name, " ", "-"))
		_ = oldStream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: append([]byte("echo "), append(marker, '\n')...)}})
		for {
			response, err := oldStream.Recv()
			if err != nil {
				break
			}
			if bytes.Contains(response.GetStdout(), marker) {
				t.Fatalf("%s recovered across VM restart", name)
			}
		}
	}
	client = harness.runtime(t)
	for _, item := range []struct{ namespaceID, containerID string }{{"restart-source", caller.GetId()}, {"restart-destination", server.GetId()}} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		response, err := client.GetContainer(ctx, item.namespaceID, &hostv1.ContainerRequest{ContainerId: item.containerID})
		cancel()
		if err != nil || response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_STOPPED {
			t.Fatalf("process restored after restart: %+v, %v", response.GetContainer(), err)
		}
	}
	startContainer(t, client, "restart-source", caller.GetId())
	startContainer(t, client, "restart-destination", server.GetId())
	startEchoServer(t, client, "restart-destination", server.GetId(), 8080)
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "cat /data/value /root/value"}); code != 0 || string(output) != "restart-durable\nroot-durable\n" {
		t.Fatalf("filesystem and volume after restart = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"nc", "-z", "-w", "2", containerAddress(t, client, "restart-destination", server.GetId()), "8080"}); code != 0 {
		t.Fatal("host policy was not replayed after restart")
	}
	if output, code := execContainer(t, client, "restart-source", caller.GetId(), []string{"sh", "-c", "printf replayed | nc -w 5 service 8080"}); code != 0 || string(output) != "replayed" {
		t.Fatalf("replayed link = %q, exit %d", output, code)
	}
	deleteLink(t, client, link.GetId())
	stopDeleteContainer(t, client, "restart-source", caller.GetId())
	stopDeleteContainer(t, client, "restart-destination", server.GetId())
	deleteVolume(t, client, "restart-source", "data")
	deleteNamespace(t, client, "restart-source")
	deleteNamespace(t, client, "restart-destination")
}

func testForcedVMStop(t *testing.T, harness *hostHarness) {
	client := harness.runtime(t)
	for _, namespaceID := range []string{"force-source", "force-destination"} {
		ensureNamespace(t, client, namespaceID, 256<<20)
		pullImage(t, client, namespaceID)
	}
	createVolume(t, client, "force-source", "data")
	caller := createContainer(t, client, "force-source", "caller", idleProcess(), []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	server := createContainer(t, client, "force-destination", "server", idleProcess(), nil)
	startContainer(t, client, "force-source", caller.GetId())
	startContainer(t, client, "force-destination", server.GetId())
	startEchoServer(t, client, "force-destination", server.GetId(), 8080)
	if _, code := execContainer(t, client, "force-source", caller.GetId(), []string{"sh", "-c", "echo force-durable > /data/value; sync"}); code != 0 {
		t.Fatalf("fsync before forced stop exited %d", code)
	}
	if _, code := execContainer(t, client, "force-destination", server.GetId(), []string{"sync"}); code != 0 {
		t.Fatalf("server fsync before forced stop exited %d", code)
	}
	applyLink(t, client, &hostv1.NetworkLink{Id: "force-link", SourceEnvironmentId: "force-source", DestinationEnvironmentId: "force-destination", Source: allSelector(), Destination: allSelector(), TcpPorts: []uint32{8080}, DnsAlias: "service"})
	if _, err := client.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	harness.stop(t, true)
	stopped, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, environmentID := range []string{"force-source", "force-destination"} {
		found := false
		for _, environment := range stopped.GetEnvironments() {
			if environment.GetId() == environmentID {
				found = len(environment.GetContainers()) == 1 && environment.GetContainers()[0].GetState() == hostv1.ContainerState_CONTAINER_STATE_UNKNOWN && environment.GetUsage().GetStale()
			}
		}
		if !found {
			t.Fatalf("forced-stop snapshot did not expose %s container as UNKNOWN with stale usage", environmentID)
		}
	}
	harness.start(t)
	client = harness.runtime(t)
	startContainer(t, client, "force-source", caller.GetId())
	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), 30*time.Second)
	response, err := client.GetContainer(inspectCtx, "force-destination", &hostv1.ContainerRequest{ContainerId: server.GetId()})
	inspectCancel()
	if err != nil || response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatalf("process restored after forced stop: %+v, %v", response.GetContainer(), err)
	}
	startContainer(t, client, "force-destination", server.GetId())
	startEchoServer(t, client, "force-destination", server.GetId(), 8080)
	if output, code := execContainer(t, client, "force-source", caller.GetId(), []string{"cat", "/data/value"}); code != 0 || string(output) != "force-durable\n" {
		t.Fatalf("fsync data after forced stop = %q, exit %d", output, code)
	}
	if _, code := execContainer(t, client, "force-source", caller.GetId(), []string{"nc", "-z", "-w", "2", containerAddress(t, client, "force-destination", server.GetId()), "8080"}); code != 0 {
		t.Fatal("host policy was not replayed after forced restart")
	}
	deleteLink(t, client, "force-link")
	stopDeleteContainer(t, client, "force-source", caller.GetId())
	stopDeleteContainer(t, client, "force-destination", server.GetId())
	deleteVolume(t, client, "force-source", "data")
	deleteNamespace(t, client, "force-source")
	deleteNamespace(t, client, "force-destination")
}

func testResidualScan(t *testing.T, client *productClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	for _, namespaceID := range []string{
		"lifecycle", "volume-owner", "volume-other", "quota", "healthy", "shared-small", "same",
		"isolated-source", "isolated-destination", "link-source", "link-destination", "revoke-source",
		"revoke-destination", "egress-deny", "egress-public", "forward", "dockerfile-build", "parallel-a",
		"parallel-b", "cancel-build", "quota-build", "restart-source", "restart-destination", "force-source", "force-destination",
	} {
		response, err := client.ListContainers(ctx, namespaceID)
		if err == nil && len(response.GetContainers()) != 0 {
			t.Fatalf("residual namespace %q has %d visible containers", namespaceID, len(response.GetContainers()))
		}
	}
}

// Keep the fixture on the immutable linux/arm64 child manifest. The mirror's
// multi-platform index has intermittently evicted older child blobs.
const workloadImage = "mirror.gcr.io/library/alpine@sha256:1832327faf048390adc33852575d37c7ba155e064a339e78b9bd81983a8c7a00"

type hostHarness struct {
	client              hostv1.OperatorServiceClient
	connection          *grpc.ClientConn
	process             *exec.Cmd
	binary              string
	processArgs         []string
	serial              string
	instance            string
	database            string
	runtimeSocket       string
	operatorSocket      string
	remoteComputeSocket string
	trustSocket         string
	trustCapabilityFile string
	runtimeClient       *productClient
	starts              int
}

func startHost(t *testing.T) *hostHarness {
	return startHostWithState(t, 1<<30)
}

func openE2EDatabase(path string) (*sql.DB, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if _, err := database.Exec("PRAGMA busy_timeout=10000"); err != nil {
		database.Close()
		return nil, err
	}
	return database, nil
}

func startHostWithState(t *testing.T, stateBytes uint64) *hostHarness {
	t.Helper()
	bundle := os.Getenv("AGENT_VMM_E2E_BUNDLE")
	if bundle == "" {
		bundle = "/tmp/agent-vmm-e2e-bundle"
	}
	directory, err := os.MkdirTemp("/tmp", "agent-vmm-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	binary := os.Getenv("AGENT_VMM_E2E_HOST_BINARY")
	if binary == "" {
		binary = filepath.Join(directory, "agent-vmm-host")
		runCommand(t, 2*time.Minute, "go", "build", "-tags=e2e", "-o", binary, "../../cmd/agent-vmm-host")
		runCommand(t, time.Minute, "codesign", "--force", "--sign", "-", "--entitlements", "../../build/host/vz.entitlements.plist", binary)
	}
	operatorSocket := filepath.Join(directory, "operator.sock")
	remoteComputeSocket := filepath.Join(directory, "remote-compute.sock")
	trustSocket := filepath.Join(directory, "trust.sock")
	trustCapabilityFile := filepath.Join(directory, "connector-trust.cap")
	runtimeSocket := filepath.Join(directory, "runtime.sock")
	database := filepath.Join(directory, "host.sqlite")
	sessions := filepath.Join(directory, "sessions")
	instance := filepath.Join(directory, "instance")
	serial := filepath.Join(directory, "serial.log")
	processArgs := []string{
		"--bundle", bundle, "--instance", instance, "--state-size", fmt.Sprint(stateBytes),
		"--cpus", "4", "--memory", "4294967296",
		"--serial-log", serial, "--operator-socket", operatorSocket, "--remote-compute-socket", remoteComputeSocket,
		"--database", database, "--session-directory", sessions,
	}
	if dataWorker := os.Getenv("AGENT_VMM_E2E_DATA_BINARY"); dataWorker != "" {
		processArgs = append(processArgs, "--data-worker", dataWorker)
	}
	harness := &hostHarness{binary: binary, processArgs: processArgs, serial: serial, instance: instance, database: database,
		runtimeSocket: runtimeSocket, operatorSocket: operatorSocket, remoteComputeSocket: remoteComputeSocket,
		trustSocket: trustSocket, trustCapabilityFile: trustCapabilityFile}
	harness.launchHostProcess(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("serial log tail:\n%s", readFileTail(serial, 64<<10))
		}
	})
	t.Cleanup(func() { harness.shutdownHostProcess() })
	waitForPath(t, operatorSocket, 10*time.Second)
	waitForPath(t, trustSocket, 10*time.Second)
	waitForPath(t, trustCapabilityFile, 10*time.Second)
	connection, err := grpc.NewClient("passthrough:///host", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", operatorSocket)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	operator := hostv1.NewOperatorServiceClient(connection)
	runtimeClient := newProductClient(operator)
	t.Cleanup(runtimeClient.close)
	harness.client, harness.connection, harness.runtimeClient = operator, connection, runtimeClient
	return harness
}

func (harness *hostHarness) launchHostProcess(t *testing.T) {
	t.Helper()
	process := exec.Command(harness.binary, harness.processArgs...)
	process.Stdout = os.Stdout
	process.Stderr = os.Stderr
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	harness.process = process
}

func (harness *hostHarness) shutdownHostProcess() {
	process := harness.process
	if process == nil || (process.ProcessState != nil && process.ProcessState.Exited()) {
		return
	}
	_ = process.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = process.Process.Kill()
		<-done
	}
}

func (harness *hostHarness) crashAndRestartHost(t *testing.T) {
	t.Helper()
	if err := harness.process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := harness.process.Wait(); err == nil {
		t.Fatal("SIGKILL host unexpectedly exited successfully")
	}
	for _, path := range []string{harness.operatorSocket, harness.remoteComputeSocket, harness.trustSocket, harness.trustCapabilityFile} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	harness.launchHostProcess(t)
	waitForPath(t, harness.operatorSocket, 10*time.Second)
	waitForPath(t, harness.trustSocket, 10*time.Second)
	waitForPath(t, harness.trustCapabilityFile, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := harness.client.GetVM(ctx, &hostv1.GetVMRequest{})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted host did not become ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	harness.start(t)
}

func readFileTail(path string, limit int64) []byte {
	file, err := os.Open(path)
	if err != nil {
		return []byte(err.Error())
	}
	defer file.Close()
	if info, err := file.Stat(); err == nil && info.Size() > limit {
		_, _ = file.Seek(-limit, io.SeekEnd)
	}
	data, _ := io.ReadAll(io.LimitReader(file, limit))
	return data
}

func (harness *hostHarness) start(t *testing.T) *hostv1.VM {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, err := harness.client.StartVM(ctx, &hostv1.StartVMRequest{RequestId: fmt.Sprintf("start-%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	if harness.starts == 0 {
		t.Logf("E2E_COLD_START_MS=%d", time.Since(started).Milliseconds())
	}
	harness.starts++
	response, err := harness.client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetVm()
}

func (harness *hostHarness) stop(t *testing.T, force bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := harness.client.StopVM(ctx, &hostv1.StopVMRequest{RequestId: fmt.Sprintf("stop-%d", time.Now().UnixNano()), Force: force})
	if err != nil {
		t.Fatal(err)
	}
	response, err := harness.client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetVm().GetState() != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM() = %+v", response.GetVm())
	}
}

func (harness *hostHarness) runtime(t *testing.T) *productClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := harness.runtimeClient.refreshSessions(ctx); err != nil {
		t.Fatal(err)
	}
	return harness.runtimeClient
}

func ensureNamespace(t *testing.T, client *productClient, namespaceID string, diskBytes uint64) {
	ensureNamespaceWithEgress(t, client, namespaceID, diskBytes, hostv1.EgressMode_EGRESS_MODE_DENY_ALL)
}

func ensureNamespaceWithEgress(t *testing.T, client *productClient, namespaceID string, diskBytes uint64, egress hostv1.EgressMode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.EnsureEnvironment(ctx, namespaceID, "ensure-"+namespaceID, &hostv1.ResourceLimits{
		CpuMillis: optional(uint32(1000)), MemoryBytes: optional(uint64(3 << 30)), Pids: optional(uint32(512)), DiskBytes: optional(diskBytes),
	}, egress)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetNamespaceId() == "" {
		t.Fatalf("environment allocation = %+v", response)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := client.DeleteEnvironment(ctx, namespaceID, "cleanup-"+namespaceID); err != nil && status.Code(err) != codes.NotFound {
			t.Errorf("cleanup environment %q: %v", namespaceID, err)
		}
	})
}

func createVolume(t *testing.T, client *productClient, namespaceID, volumeID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateVolume(ctx, namespaceID, &hostv1.CreateVolumeRequest{RequestId: "create-" + namespaceID + "-" + volumeID, VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.DeleteVolume(ctx, namespaceID, &hostv1.DeleteVolumeRequest{RequestId: "cleanup-" + namespaceID + "-" + volumeID, VolumeId: volumeID})
	})
}

func deleteVolume(t *testing.T, client *productClient, namespaceID, volumeID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.DeleteVolume(ctx, namespaceID, &hostv1.DeleteVolumeRequest{RequestId: "delete-" + namespaceID + "-" + volumeID, VolumeId: volumeID}); err != nil {
		t.Fatal(err)
	}
}

func idleProcess() []string {
	return []string{"tail", "-f", "/dev/null"}
}

func echoServer(port uint32) []string {
	return []string{"nc", "-lk", "-p", fmt.Sprint(port), "-e", "/bin/cat"}
}

func allSelector() *hostv1.ContainerSelector {
	return &hostv1.ContainerSelector{Selection: &hostv1.ContainerSelector_AllContainers{AllContainers: true}}
}

func idsSelector(ids ...string) *hostv1.ContainerSelector {
	return &hostv1.ContainerSelector{Selection: &hostv1.ContainerSelector_ContainerIds{ContainerIds: &hostv1.ContainerIDList{Ids: ids}}}
}

func applyLink(t *testing.T, client *productClient, link *hostv1.NetworkLink) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.ApplyNetworkLink(ctx, &hostv1.ApplyNetworkLinkRequest{RequestId: "apply-" + link.GetId(), Link: link}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.DeleteNetworkLink(ctx, &hostv1.DeleteNetworkLinkRequest{RequestId: "cleanup-" + link.GetId(), LinkId: link.GetId()})
	})
}

func deleteLink(t *testing.T, client *productClient, linkID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.DeleteNetworkLink(ctx, &hostv1.DeleteNetworkLinkRequest{RequestId: "delete-" + linkID, LinkId: linkID}); err != nil {
		t.Fatal(err)
	}
}

func containerAddress(t *testing.T, client *productClient, namespaceID, containerID string) string {
	t.Helper()
	output, code := execContainer(t, client, namespaceID, containerID, []string{"sh", "-c", "ip -4 -o addr show dev eth0 | awk '{split($4,a,\"/\"); print a[1]}'"})
	address := strings.TrimSpace(string(output))
	if code != 0 || net.ParseIP(address) == nil {
		t.Fatalf("container address = %q, exit %d", output, code)
	}
	return address
}

func waitForContainerPort(t *testing.T, client *productClient, namespaceID, containerID string, port uint32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, code := execContainer(t, client, namespaceID, containerID, []string{"nc", "-z", "-w", "1", "127.0.0.1", fmt.Sprint(port)}); code == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("container %s/%s did not listen on TCP %d", namespaceID, containerID, port)
}

func startEchoServer(t *testing.T, client *productClient, namespaceID, containerID string, port uint32) {
	t.Helper()
	if output, code := execContainer(t, client, namespaceID, containerID, []string{"sh", "-c", fmt.Sprintf("nc -lk -p %d -e /bin/cat >/dev/null 2>&1 &", port)}); code != 0 {
		t.Fatalf("start echo server = %q, exit %d", output, code)
	}
	waitForContainerPort(t, client, namespaceID, containerID, port)
}

func stopDeleteContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	stopContainer(t, client, namespaceID, containerID)
	deleteContainerEventually(t, client, namespaceID, containerID)
}

func pullImage(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	var digest string
	var err error
	for attempt := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		digest, err = pullImageAttempt(ctx, client, namespaceID, fmt.Sprintf("pull-%s-%d", namespaceID, attempt))
		cancel()
		if err == nil || strings.Contains(err.Error(), "429 Too Many Requests") {
			break
		}
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("pull digest = %q", digest)
	}
}

func pullImageAttempt(ctx context.Context, client *productClient, namespaceID, requestID string) (string, error) {
	return pullImageReference(ctx, client, namespaceID, requestID, workloadImage)
}

func pullImageReference(ctx context.Context, client *productClient, namespaceID, requestID, reference string) (string, error) {
	stream, err := client.PullImage(ctx, namespaceID, &hostv1.PullImageRequest{RequestId: requestID, Reference: reference})
	if err != nil {
		return "", err
	}
	var digest string
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return digest, nil
		}
		if err != nil {
			return "", err
		}
		if response.Digest != nil {
			digest = response.GetDigest()
		}
	}
}

func pullImageError(client *productClient, namespaceID string) error {
	var lastErr error
	for attempt := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		_, lastErr = pullImageAttempt(ctx, client, namespaceID, fmt.Sprintf("pull-%s-error-%d", namespaceID, attempt))
		cancel()
		if lastErr == nil || strings.Contains(lastErr.Error(), "ResourceExhausted") {
			return lastErr
		}
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	return lastErr
}

func createContainer(t *testing.T, client *productClient, namespaceID, containerID string, argv []string, volumes []*hostv1.VolumeMount) *hostv1.Container {
	return createContainerWithImageAndVolumes(t, client, namespaceID, containerID, workloadImage, argv, volumes)
}

func createContainerWithImage(t *testing.T, client *productClient, namespaceID, containerID, image string, argv []string) *hostv1.Container {
	return createContainerWithImageAndVolumes(t, client, namespaceID, containerID, image, argv, nil)
}

func createContainerWithImageAndVolumes(t *testing.T, client *productClient, namespaceID, containerID, image string, argv []string, volumes []*hostv1.VolumeMount) *hostv1.Container {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.CreateContainer(ctx, namespaceID, &hostv1.CreateContainerRequest{
		RequestId:   "create-" + namespaceID + "-" + containerID,
		ContainerId: containerID, Image: image, EntrypointOverride: &hostv1.StringList{Values: argv}, CommandOverride: &hostv1.StringList{}, Volumes: volumes, LogLimitBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		seconds := uint32(1)
		_, _ = client.StopContainer(ctx, namespaceID, &hostv1.StopContainerRequest{RequestId: "cleanup-stop-" + namespaceID + "-" + containerID, ContainerId: containerID, TimeoutSeconds: &seconds})
		_ = client.DeleteContainer(ctx, namespaceID, &hostv1.ContainerMutationRequest{RequestId: "cleanup-delete-" + namespaceID + "-" + containerID, ContainerId: containerID})
	})
	return response.GetContainer()
}

func buildShell(t *testing.T) string {
	t.Helper()
	output := filepath.Join(t.TempDir(), "buildshell")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags=-s -w -buildid=", "-o", output, "./testdata/buildshell")
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	if bytes, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build E2E helper: %v: %s", err, bytes)
	}
	return output
}

func dockerfileContext(t *testing.T, tool string) []byte {
	t.Helper()
	binary, err := os.ReadFile(tool)
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := []byte("FROM scratch AS build\nADD buildshell /bin/sh\nARG MESSAGE\nRUN --mount=type=secret,id=token /bin/sh -c build\nFROM scratch\nCOPY --from=build /bin/sh /bin/result\nCOPY --from=build /result /result\nENTRYPOINT [\"/bin/result\"]\n")
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, file := range []struct {
		name string
		mode int64
		data []byte
	}{{"Dockerfile", 0o644, dockerfile}, {"buildshell", 0o755, binary}} {
		if err := writer.WriteHeader(&tar.Header{Name: file.name, Mode: file.mode, Size: int64(len(file.data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(file.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

type imageBuildResult struct {
	digest   string
	progress []string
}

func buildImage(t *testing.T, client *productClient, namespaceID, image string, contextArchive, secret []byte) imageBuildResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := buildImageRequest(ctx, client, namespaceID, image, "build-ok", hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, secret)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(result.digest, "sha256:") {
		t.Fatalf("build digest = %q", result.digest)
	}
	return result
}

func buildImageRequest(ctx context.Context, client *productClient, namespaceID, image, message string, networkMode hostv1.BuildNetworkMode, contextArchive, secret []byte) (imageBuildResult, error) {
	stream, err := client.BuildImage(ctx, namespaceID)
	if err != nil {
		return imageBuildResult{}, err
	}
	header := &hostv1.BuildImageHeader{
		RequestId:      "build-" + namespaceID + "-" + time.Now().Format("150405.000000000"),
		DockerfilePath: "Dockerfile", Image: image, NetworkMode: networkMode,
		BuildArgs: []*hostv1.BuildArgument{{Name: "MESSAGE", Value: message}}, Secrets: []*hostv1.BuildSecret{{Id: "token"}},
	}
	if err := sendBuildInput(stream, header, contextArchive, secret); err != nil {
		return imageBuildResult{}, err
	}
	var result imageBuildResult
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return imageBuildResult{}, err
		}
		if response.GetProgress() != "" {
			result.progress = append(result.progress, response.GetProgress())
		}
		if response.GetResult() != nil {
			result.digest = response.GetResult().GetDigest()
		}
	}
	return result, nil
}

func sendBuildInput(stream grpc.BidiStreamingClient[hostv1.BuildImageRequest, hostv1.BuildImageResponse], header *hostv1.BuildImageHeader, contextArchive, secret []byte) error {
	if err := stream.Send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Header{Header: header}}); err != nil {
		return err
	}
	for len(contextArchive) > 0 {
		count := min(len(contextArchive), 1<<20)
		chunk := &hostv1.BuildContextChunk{Data: contextArchive[:count], Final: count == len(contextArchive)}
		if err := stream.Send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Context{Context: chunk}}); err != nil {
			return err
		}
		contextArchive = contextArchive[count:]
	}
	if err := stream.Send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Secret{Secret: &hostv1.BuildSecretChunk{Id: "token", Data: secret, Final: true}}}); err != nil {
		return err
	}
	return stream.CloseSend()
}

func startContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.StartContainer(ctx, namespaceID, &hostv1.ContainerMutationRequest{RequestId: "start-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000"), ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("StartContainer() = %+v", response.GetContainer())
	}
}

func stopContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	timeout := uint32(3)
	response, err := client.StopContainer(ctx, namespaceID, &hostv1.StopContainerRequest{RequestId: "stop-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000"), ContainerId: containerID, TimeoutSeconds: &timeout})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatalf("StopContainer() = %+v", response.GetContainer())
	}
}

func waitContainer(t *testing.T, client *productClient, namespaceID, containerID string, expected uint32) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.WaitContainer(ctx, namespaceID, &hostv1.ContainerRequest{ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	if expected == 0 && response.GetExitCode() != 0 {
		t.Fatalf("WaitContainer() exit = %d", response.GetExitCode())
	}
	return response.GetExitCode()
}

func deleteContainer(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.DeleteContainer(ctx, namespaceID, &hostv1.ContainerMutationRequest{RequestId: "delete-" + namespaceID + "-" + containerID, ContainerId: containerID}); err != nil {
		t.Fatal(err)
	}
}

func deleteContainerEventually(t *testing.T, client *productClient, namespaceID, containerID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lastErr = client.DeleteContainer(ctx, namespaceID, &hostv1.ContainerMutationRequest{RequestId: fmt.Sprintf("delete-%d", time.Now().UnixNano()), ContainerId: containerID})
		cancel()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("DeleteContainer(%q/%q): %v", namespaceID, containerID, lastErr)
		}
		<-ticker.C
	}
}

func deleteNamespace(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.DeleteEnvironment(ctx, namespaceID, "delete-"+namespaceID); err != nil {
		t.Fatal(err)
	}
}

func deleteNamespaceEventually(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = client.DeleteEnvironment(ctx, namespaceID, "delete-"+namespaceID)
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("DeleteNamespace(%q): %v", namespaceID, lastErr)
}

func execContainer(t *testing.T, client *productClient, namespaceID, containerID string, argv []string) ([]byte, uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Exec(ctx, namespaceID)
	if err != nil {
		t.Fatalf("exec %s: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
	}
	requestID := "exec-" + namespaceID + "-" + containerID + time.Now().Format("150405.000000000")
	if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{RequestId: requestID, ContainerId: containerID, Argv: argv}}}); err != nil {
		t.Fatalf("exec %s send: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("exec %s close: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
	}
	var output bytes.Buffer
	var exitCode uint32
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return output.Bytes(), exitCode
		}
		if err != nil {
			t.Fatalf("exec %s receive: %v; logs=%q", containerID, err, readLogs(t, client, namespaceID, containerID))
		}
		output.Write(response.GetStdout())
		output.Write(response.GetStderr())
		if _, ok := response.Event.(*hostv1.ExecResponse_ExitCode); ok {
			exitCode = response.GetExitCode()
		}
	}
}

func readLogs(t *testing.T, client *productClient, namespaceID, containerID string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Logs(ctx, namespaceID, &hostv1.LogsRequest{ContainerId: containerID, TailRecords: 4096})
	if err != nil {
		t.Fatal(err)
	}
	var result bytes.Buffer
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return result.Bytes()
		}
		if err != nil {
			t.Fatal(err)
		}
		result.Write(response.GetData())
	}
}

func waitForPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("wait for %s: timeout", path)
}

func runCommand(t *testing.T, timeout time.Duration, name string, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, name, arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", name, err, output)
	}
}
