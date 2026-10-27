//go:build darwin && e2e

package e2e

import (
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

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testSystemAppliance(t *testing.T, harness *hostHarness) {
	archive, imageDigest := buildExecutableArchive(t, "./cmd/agent-vmm-reference-appliance", "bin/reference-appliance", true)
	deadline := time.Now().Add(5 * time.Minute).UnixMilli()
	client := appliancev1.NewApplianceAdminServiceClient(harness.connection)
	content := stageApplianceContent(t, client, archive, "agent-vmm.local/reference@"+imageDigest, imageDigest, deadline)
	manifest := &appliancev1.ControllerApplianceManifest{ContractVersion: "1", ProductId: "reference",
		Release: "e2e", ImageReference: content.GetImageReference(), ContentRefs: []*appliancev1.ApplianceContentReference{content},
		ImageManifestDigest: imageDigest, Platform: "linux/arm64",
		SystemLimits:    isolationLimits(256, 256<<20),
		DataVolumeBytes: 1 << 30, RequestedCapacity: &appliancev1.CapacityGrant{
			PerEnvironmentLimits: isolationLimits(512, 256<<20),
			MaxEgressMode:        hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		RequestedEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StartupDeadlineSeconds: 60,
	}
	secretA := []byte("appliance-bootstrap-secret-a-4d57af9b")
	secretB := []byte("appliance-bootstrap-secret-b-9c34b572")
	statusA := activateReferenceAppliance(t, client, "e2e-reference-a", manifest, deadline, secretA)
	waitReferenceContract(t, harness.client, statusA.GetRegistrationId(), deadline)
	manifestB := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
	manifestB.Release = "e2e-b"
	statusB := activateReferenceAppliance(t, client, "e2e-reference-b", manifestB, deadline, secretB)
	waitReferenceContract(t, harness.client, statusB.GetRegistrationId(), deadline)
	t.Run("workload_after_activation", func(t *testing.T) {
		runtime := harness.runtime(t)
		const environment = "appliance-activation-workload"
		ensureNamespace(t, runtime, environment, 128<<20)
		importRegistryImage(t, runtime, environment)
		container := createContainer(t, runtime, environment, "worker", []string{"/bin/sh", "-c", "while :; do sleep 1; done"}, nil)
		startContainer(t, runtime, environment, container.GetId())
		if output, code := execContainer(t, runtime, environment, container.GetId(), []string{"/bin/echo", "activation-ready"}); code != 0 || string(output) != "activation-ready\n" {
			t.Fatalf("workload after appliance activation: output=%q code=%d", output, code)
		}
		stopContainer(t, runtime, environment, container.GetId())
		deleteContainer(t, runtime, environment, container.GetId())
	})
	for _, status := range []*appliancev1.ApplianceStatus{statusA, statusB} {
		if status.GetApplianceState() != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || !status.GetHealth().GetHealthy() || status.GetChannelState() != appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE {
			t.Fatalf("appliance status = %+v", status)
		}
	}
	database, err := openE2EDatabase(harness.database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, installationID := range []string{"e2e-reference-a", "e2e-reference-b"} {
		var bootstrapID string
		if err := database.QueryRow("SELECT bootstrap_id FROM appliance_installations WHERE id = ?", installationID).Scan(&bootstrapID); err != nil {
			t.Fatal(err)
		}
		if bootstrapID != "" {
			t.Fatal("activated appliance retained an unconsumed bootstrap")
		}
	}
	bootstrapFiles, err := filepath.Glob(filepath.Join(harness.instance, "appliance-secrets", "*.bootstrap"))
	if err != nil || len(bootstrapFiles) != 0 {
		t.Fatalf("consumed bootstrap files remain: count=%d error=%v", len(bootstrapFiles), err)
	}
	// Reconciliation advances the revision after activation; restart the current one.
	statusB = applianceStatus(t, client, "e2e-reference-b")
	if _, err := client.RestartAppliance(context.Background(), &appliancev1.RestartApplianceRequest{InstallationId: "e2e-reference-b", RequestId: "reference-b-explicit-restart",
		ExpectedRevision: statusB.GetRevision(), DeadlineUnixMillis: deadline}); err != nil {
		t.Fatal(err)
	}
	statusB = waitApplianceState(t, client, "e2e-reference-b", appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	if statusB.GetApplianceState() != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || !statusB.GetHealth().GetHealthy() {
		t.Fatalf("explicit appliance restart failed: %+v", statusB)
	}
	support, err := harness.client.ExportSupportBundle(context.Background(), &hostv1.ExportSupportBundleRequest{RequestId: "appliance-secret-scan"})
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
	log, err := os.ReadFile(harness.serial + ".host.log")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(log, []byte("system appliance launch failed:")) {
		t.Fatalf("appliance recovery failed before reaching ACTIVE:\n%s", log)
	}
}

// stageApplianceContent publishes a controller image archive through the
// operator content stage that a manifest content reference names.
func stageApplianceContent(t *testing.T, client appliancev1.ApplianceAdminServiceClient, archive []byte, reference, imageDigest string, deadline int64) *appliancev1.ApplianceContentReference {
	t.Helper()
	sum := sha256.Sum256(archive)
	content := &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, ArchiveSha256: sum[:],
		ArchiveSize: uint64(len(archive)), Platform: "linux/arm64", ImageReference: reference, ImageManifestDigest: imageDigest}
	stream, err := client.StageContent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_Header{Header: &appliancev1.StageContentHeader{RequestId: fmt.Sprintf("stage-content-%x", sum[:8]), DeadlineUnixMillis: deadline, ContentRef: content}}}); err != nil {
		t.Fatal(err)
	}
	for chunk := range slices.Chunk(archive, 1<<20) {
		if err := stream.Send(&appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_ArchiveChunk{ArchiveChunk: chunk}}); err != nil {
			break
		}
	}
	receipt, err := stream.CloseAndRecv()
	if err != nil || receipt.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("stage appliance content = %+v, %v", receipt.GetOperation(), err)
	}
	return content
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
	grant := &appliancev1.ApprovedGrant{SystemLimits: manifest.GetSystemLimits(), Capacity: manifest.GetRequestedCapacity(), EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, Revision: current.GetRevision()}
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
	return waitApplianceState(t, client, installationID, appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
}

// Ready precedes the reference workload. Its task allocation disappears only
// after both image imports, service traffic, and cleanup complete.
func waitReferenceContract(t *testing.T, client hostv1.OperatorServiceClient, registrationID string, deadline int64) {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.UnixMilli(deadline))
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var durable *hostv1.AgentEnvironment
	var ephemeral bool
	for {
		response, err := client.ListEnvironments(ctx, &hostv1.ListEnvironmentsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		durable, ephemeral = nil, false
		for _, environment := range response.GetEnvironments() {
			if environment.GetOwner().GetRegistrationId() != registrationID {
				continue
			}
			switch environment.GetOwner().GetAllocationId() {
			case "durable-service":
				durable = environment
			case "ephemeral-task":
				ephemeral = true
			}
		}
		if durable.GetInventoryState() == hostv1.InventoryState_INVENTORY_STATE_AVAILABLE && !ephemeral {
			running := make(map[string]bool)
			for _, container := range durable.GetContainers() {
				running[container.GetId()] = container.GetState() == hostv1.ContainerState_CONTAINER_STATE_RUNNING
			}
			if running["service"] && running["sidecar"] {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("reference contract %q did not complete: durable=%v ephemeral=%t: %v", registrationID, durable, ephemeral, ctx.Err())
		}
	}
}

func assertProcessSecretAbsent(t *testing.T, process *exec.Cmd, secret []byte) {
	t.Helper()
	if bytes.Contains([]byte(strings.Join(process.Args, "\x00")), secret) || bytes.Contains([]byte(strings.Join(process.Env, "\x00")), secret) {
		t.Fatal("bootstrap secret found in host process argv or environment")
	}
}

func applianceStatus(t *testing.T, client appliancev1.ApplianceAdminServiceClient, installationID string) *appliancev1.ApplianceStatus {
	t.Helper()
	response, err := client.GetApplianceStatus(context.Background(), &appliancev1.GetApplianceStatusRequest{InstallationId: installationID})
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
		if current.GetApplianceState() == expected && (expected != appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING || (current.GetHealth().GetHealthy() && current.GetChannelState() == appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE)) {
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
