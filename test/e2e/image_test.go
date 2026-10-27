//go:build darwin && e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	if _, err := session.client.StatWorkspace(ctx, &hostv1.StatWorkspaceRequest{Path: "../escape"}); err == nil {
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
	if restored, err := session.client.StatWorkspace(ctx, &hostv1.StatWorkspaceRequest{Path: "restored/workspace.txt"}); err != nil || restored.GetEntry().GetSha256() == nil {
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
	if _, err := importRegistryImageReference(ctx, client, firstEnvironment, "pull-image-record-a", workloadImage); err != nil {
		t.Fatal(err)
	}
	if _, err := importRegistryImageReference(ctx, client, secondEnvironment, "pull-image-record-b", workloadImage); err != nil {
		t.Fatal(err)
	}
	first := imageByReference(t, client, firstEnvironment, workloadImage)
	imageByReference(t, client, secondEnvironment, workloadImage)
	container := createContainer(t, client, firstEnvironment, "image-user", []string{"sleep", "300"}, nil)
	sameExternalID := createContainer(t, client, secondEnvironment, "image-user", []string{"true"}, nil)
	if container.GetId() != sameExternalID.GetId() || container.GetGenerationId() == sameExternalID.GetGenerationId() {
		t.Fatalf("cross-environment container identity was not independently fenced: first=%+v second=%+v", container, sameExternalID)
	}
	if listed, err := client.ListContainers(ctx, firstEnvironment); err != nil || len(listed.GetContainers()) != 1 || listed.GetContainers()[0].GetGenerationId() != container.GetGenerationId() {
		t.Fatalf("first environment observed another environment container: response=%+v err=%v", listed, err)
	}
	if listed, err := client.ListContainers(ctx, secondEnvironment); err != nil || len(listed.GetContainers()) != 1 || listed.GetContainers()[0].GetGenerationId() != sameExternalID.GetGenerationId() {
		t.Fatalf("second environment observed another environment container: response=%+v err=%v", listed, err)
	}
	if err := client.DeleteContainer(ctx, secondEnvironment, &hostv1.DeleteContainerRequest{RequestId: "remove-second-image-user", ContainerId: sameExternalID.GetId(), ExpectedGenerationId: sameExternalID.GetGenerationId()}); err != nil {
		t.Fatal(err)
	}
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-created-image", first)), "IMAGE_IN_USE")
	if _, err := client.StartContainer(ctx, firstEnvironment, &hostv1.StartContainerRequest{RequestId: "start-image-user", ContainerId: container.GetId(), ExpectedGenerationId: container.GetGenerationId()}); err != nil {
		t.Fatal(err)
	}
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-running-image", first)), "IMAGE_IN_USE")
	seconds := uint32(2)
	if _, err := client.StopContainer(ctx, firstEnvironment, &hostv1.StopContainerRequest{RequestId: "stop-image-user", ContainerId: container.GetId(), TimeoutSeconds: &seconds}); err != nil {
		t.Fatal(err)
	}
	assertImageDeleteReason(t, client.DeleteImage(ctx, firstEnvironment, deleteImageRequest("delete-stopped-image", first)), "IMAGE_IN_USE")
	if err := client.DeleteContainer(ctx, firstEnvironment, &hostv1.DeleteContainerRequest{RequestId: "remove-image-user", ContainerId: container.GetId(), ExpectedGenerationId: container.GetGenerationId()}); err != nil {
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
	if _, err := client.StartContainer(ctx, secondEnvironment, &hostv1.StartContainerRequest{RequestId: "start-shared-image-user", ContainerId: shared.GetId(), ExpectedGenerationId: shared.GetGenerationId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := importRegistryImageReference(ctx, client, firstEnvironment, "recreate-image-record-a", workloadImage); err != nil {
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
		request, err := currentDeleteContainerRequest(ctx, client, firstEnvironment, "image-race", "delete-image-race-container")
		if err == nil {
			err = client.DeleteContainer(ctx, firstEnvironment, request)
		}
		if err != nil {
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
	if digest, err := importRegistryImageReference(ctx, client, environmentID, "pull-pgvector", image); err != nil || !strings.HasPrefix(digest, "sha256:") {
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
		User: &user,
		Env:  []string{"POSTGRES_PASSWORD=agent-vmm-e2e", "PGDATA=/var/lib/postgresql/data/pgdata"},
		Volumes: []*hostv1.VolumeMount{
			{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/var/lib/postgresql/data"},
			{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/var/run/postgresql"},
		}, LogLimitBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := client.StartContainer(ctx, environmentID, &hostv1.StartContainerRequest{RequestId: "start-pgvector", ContainerId: created.GetContainer().GetId(), ExpectedGenerationId: created.GetContainer().GetGenerationId()})
	if err != nil || started.GetContainer().GetInstanceId() == "" {
		t.Fatalf("start pgvector = %+v, %v", started.GetContainer(), err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		current, err := client.GetContainer(ctx, environmentID, &hostv1.GetContainerRequest{ContainerId: "postgres"})
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

func testDockerfileBuild(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "dockerfile-build", 768<<20)
	startBuildAnchor(t, client, "dockerfile-build")
	tool := buildShell(t)
	contextArchive := dockerfileContext(t, tool)
	secret := []byte("build-secret-7a5f07acbcaf")
	image := "agent.local/dockerfile-result:latest"
	first := buildImage(t, client, "dockerfile-build", image, contextArchive, secret)
	second := buildImage(t, client, "dockerfile-build", image, contextArchive, secret)
	if first.digest != second.digest {
		t.Fatalf("cached build digest changed: %s != %s", first.digest, second.digest)
	}
	if !strings.Contains(strings.Join(second.progress, ""), "CACHED") {
		t.Fatalf("second build did not reuse the namespace build cache: %q", second.progress)
	}
	container := createContainerWithImage(t, client, "dockerfile-build", "result", image, []string{"/bin/result", "--assert-absent", string(secret)})
	startAndWaitContainer(t, client, "dockerfile-build", container.GetId(), 0)
	if output := readLogs(t, client, "dockerfile-build", container.GetId()); string(output) != "build-ok\n" {
		t.Fatalf("built image output = %q", output)
	}
	deleteContainer(t, client, "dockerfile-build", container.GetId())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rebuilt, err := buildImageRequest(ctx, client, "dockerfile-build", image, "rebuilt-ok", hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, secret)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.digest == first.digest {
		t.Fatal("changed build retained the old digest")
	}
	container = createContainerWithImage(t, client, "dockerfile-build", "rebuilt", image, []string{"/bin/result", "--assert-absent", string(secret)})
	startAndWaitContainer(t, client, "dockerfile-build", container.GetId(), 0)
	if output := readLogs(t, client, "dockerfile-build", container.GetId()); string(output) != "rebuilt-ok\n" {
		t.Fatalf("rebuilt image output = %q", output)
	}
	deleteContainer(t, client, "dockerfile-build", container.GetId())
	stopDeleteContainer(t, client, "dockerfile-build", buildAnchorID)
	deleteNamespace(t, client, "dockerfile-build")
}

func testParallelAgents(t *testing.T, client *productClient) {
	namespaces := []string{"parallel-a", "parallel-b"}
	for _, namespaceID := range namespaces {
		ensureNamespace(t, client, namespaceID, 1<<30)
		startBuildAnchor(t, client, namespaceID)
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
			t.Errorf("build %s: %v", outcome.namespaceID, outcome.err)
			continue
		}
		if strings.Contains(strings.Join(outcome.result.progress, ""), "CACHED") {
			t.Fatalf("namespace %s reused another namespace's cache", outcome.namespaceID)
		}
	}
	if t.Failed() {
		return
	}
	for _, namespaceID := range namespaces {
		container := createContainerWithImage(t, client, namespaceID, "result", "agent.local/shared-name:latest", []string{"/bin/result"})
		startExitingContainer(t, client, namespaceID, container.GetId())
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
		stopDeleteContainer(t, client, namespaces[index], buildAnchorID)
		deleteNamespace(t, client, namespaces[index])
	}
}

func testCancellationCleanup(t *testing.T, client *productClient) {
	namespaceID := "cancel-build"
	ensureNamespace(t, client, namespaceID, 768<<20)
	startBuildAnchor(t, client, namespaceID)
	contextArchive := dockerfileContext(t, buildShell(t))
	// A disconnected caller does not cancel an accepted build; its execution
	// deadline does. The fixture build sleeps far beyond this deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	header, err := buildHeader(ctx, client, namespaceID, "cancel-build", "agent.local/cancelled:latest", "build-cancel", hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NAMESPACE_EGRESS)
	if err != nil {
		t.Fatal(err)
	}
	cancelAt := time.Now().Add(30 * time.Second)
	header.ExecutionDeadlineUnixNano = cancelAt.UnixNano()
	stream, err := client.BuildImage(ctx, namespaceID)
	if err != nil {
		t.Fatal(err)
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
	for {
		response, err := stream.Recv()
		if err != nil {
			break
		}
		if response.GetResult() != nil {
			t.Fatal("cancelled build completed")
		}
	}
	if elapsed := time.Since(cancelAt); elapsed > 20*time.Second {
		t.Fatalf("build cancellation took %s after its deadline", elapsed)
	}
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer checkCancel()
	listed, err := client.ListContainers(checkCtx, namespaceID)
	if err != nil || len(listed.GetContainers()) != 1 || listed.GetContainers()[0].GetId() != buildAnchorID {
		t.Fatalf("containers after cancellation = %+v, %v", listed.GetContainers(), err)
	}
	if _, err := client.CreateContainer(checkCtx, namespaceID, &hostv1.CreateContainerRequest{
		RequestId: "cancelled-image", ContainerId: "partial", Image: "agent.local/cancelled:latest", EntrypointOverride: &hostv1.StringList{Values: []string{"/bin/result"}}, CommandOverride: &hostv1.StringList{}, LogLimitBytes: 1 << 20,
	}); err == nil {
		t.Fatal("cancelled image was imported")
	}
	stopDeleteContainer(t, client, namespaceID, buildAnchorID)
	deleteNamespaceEventually(t, client, namespaceID)
	ensureNamespace(t, client, "quota-build", 4<<20)
	startBuildAnchor(t, client, "quota-build")
	quotaCtx, quotaCancel := context.WithTimeout(context.Background(), time.Minute)
	_, err = buildImageRequest(quotaCtx, client, "quota-build", "agent.local/quota:latest", "build-ok", hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE, contextArchive, []byte("quota-secret"))
	quotaCancel()
	if err == nil {
		t.Fatal("build exceeded its namespace disk quota")
	}
	stopDeleteContainer(t, client, "quota-build", buildAnchorID)
	deleteNamespaceEventually(t, client, "quota-build")
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

// buildAnchorID names the running container that owns a namespace's build
// activities; the Host admits a build only on a current container instance.
const buildAnchorID = "build-anchor"

func startBuildAnchor(t *testing.T, client *productClient, namespaceID string) {
	t.Helper()
	importRegistryImage(t, client, namespaceID)
	createContainer(t, client, namespaceID, buildAnchorID, idleProcess(), nil)
	startContainer(t, client, namespaceID, buildAnchorID)
}

func buildHeader(ctx context.Context, client *productClient, namespaceID, requestID, image, message string, networkMode hostv1.BuildNetworkMode) (*hostv1.BuildImageHeader, error) {
	anchor, err := client.GetContainer(ctx, namespaceID, &hostv1.GetContainerRequest{ContainerId: buildAnchorID})
	if err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("build context has no deadline")
	}
	return &hostv1.BuildImageHeader{
		RequestId: requestID, DockerfilePath: "Dockerfile", Image: image, NetworkMode: networkMode,
		BuildArgs: []*hostv1.BuildArgument{{Name: "MESSAGE", Value: message}}, Secrets: []*hostv1.BuildSecret{{Id: "token"}},
		ContainerId: buildAnchorID, ExpectedInstanceId: anchor.GetContainer().GetInstanceId(), ExecutionId: requestID,
		ExecutionDeadlineUnixNano: deadline.UnixNano(),
	}, nil
}

func buildImageRequest(ctx context.Context, client *productClient, namespaceID, image, message string, networkMode hostv1.BuildNetworkMode, contextArchive, secret []byte) (imageBuildResult, error) {
	header, err := buildHeader(ctx, client, namespaceID, "build-"+namespaceID+"-"+time.Now().Format("150405.000000000"), image, message, networkMode)
	if err != nil {
		return imageBuildResult{}, err
	}
	stream, err := client.BuildImage(ctx, namespaceID)
	if err != nil {
		return imageBuildResult{}, err
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

// sendBuildInput returns the Host's status when it ends the stream early; gRPC
// reports such a rejection to Send only as io.EOF.
func sendBuildInput(stream grpc.BidiStreamingClient[hostv1.BuildImageRequest, hostv1.BuildImageResponse], header *hostv1.BuildImageHeader, contextArchive, secret []byte) error {
	send := func(request *hostv1.BuildImageRequest) error {
		err := stream.Send(request)
		if errors.Is(err, io.EOF) {
			_, err = stream.Recv()
		}
		return err
	}
	if err := send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Header{Header: header}}); err != nil {
		return err
	}
	for len(contextArchive) > 0 {
		count := min(len(contextArchive), 1<<20)
		chunk := &hostv1.BuildContextChunk{Data: contextArchive[:count], Final: count == len(contextArchive)}
		if err := send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Context{Context: chunk}}); err != nil {
			return err
		}
		contextArchive = contextArchive[count:]
	}
	if err := send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Secret{Secret: &hostv1.BuildSecretChunk{Id: "token", Data: secret, Final: true}}}); err != nil {
		return err
	}
	return stream.CloseSend()
}
