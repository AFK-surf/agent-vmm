//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestSixResidentWorkloadsE2E(t *testing.T) {
	harness := startHostWithState(t, 32<<30)
	harness.start(t)
	t.Cleanup(func() { harness.stop(t, true) })
	client := harness.runtime(t)
	for index := range 6 {
		namespace := fmt.Sprintf("resident-%d", index)
		if _, err := client.EnsureEnvironment(context.Background(), namespace, "shared-"+namespace,
			&hostv1.ResourceLimits{Pids: optional(uint32(512)), DiskBytes: optional(uint64(256 << 20))}, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
			t.Fatal(err)
		}
		importRegistryImage(t, client, namespace)
		container := createContainer(t, client, namespace, "worker", idleProcess(), nil)
		startContainer(t, client, namespace, container.GetId())
	}
	if output, code := execContainer(t, client, "resident-0", "worker", []string{"awk", `BEGIN { s="x"; for (i=0;i<27;i++) s=s s; print length(s) }`}); code != 0 || string(output) != "134217728\n" {
		t.Fatalf("workload could not exceed former 64 MiB template limit: %q, exit %d", output, code)
	}
	// Idle beyond the reclaim threshold without pressure must retain all instances.
	time.Sleep(65 * time.Second)
	for index := range 6 {
		namespace := fmt.Sprintf("resident-%d", index)
		if output, code := execContainer(t, client, namespace, "worker", []string{"sh", "-c", "echo durable > /root/resident; sync; cat /root/resident"}); code != 0 || string(output) != "durable\n" {
			t.Fatalf("resident workload %d: %q, exit %d", index, output, code)
		}
	}
	harness.stop(t, false)
	harness.start(t)
	client = harness.runtime(t)
	assertResident(t, client)

	harness.crashAndRestartHost(t)
	client = harness.runtime(t)
	assertResident(t, client)
}

func testCleanupContract(t *testing.T, harness *hostHarness, client *productClient) {
	ensureNamespace(t, client, "cleanup-source", 64<<20)
	ensureNamespace(t, client, "cleanup-target", 64<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	beforePolicy, err := client.operator.GetEnvironment(ctx, &hostv1.OperatorServiceGetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	invalidLimits := &hostv1.ResourceLimits{}
	if _, err := client.operator.UpdateEnvironmentPolicy(ctx, &hostv1.UpdateEnvironmentPolicyRequest{
		RequestId: "cleanup-invalid-policy", EnvironmentId: "cleanup-source", Limits: invalidLimits,
		EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, ExpectedRevision: beforePolicy.GetEnvironment().GetPolicyRevision(),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid policy error = %v", err)
	}
	afterPolicy, err := client.operator.GetEnvironment(ctx, &hostv1.OperatorServiceGetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil || afterPolicy.GetEnvironment().GetPolicyRevision() != beforePolicy.GetEnvironment().GetPolicyRevision() {
		t.Fatalf("invalid policy changed revision: before=%v after=%v err=%v", beforePolicy.GetEnvironment().GetPolicyRevision(), afterPolicy.GetEnvironment().GetPolicyRevision(), err)
	}
	database, err := openE2EDatabase(harness.database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	agentPlan, err := client.operator.PlanResetEnvironment(ctx, &hostv1.PlanResetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
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
	client.forgetSession("cleanup-source")
	if _, err := client.EnsureEnvironment(ctx, "cleanup-source", "cleanup-reconnect", &hostv1.ResourceLimits{Pids: optional(uint32(512)), DiskBytes: optional(uint64(64 << 20))}, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateVolume(ctx, "cleanup-source", &hostv1.CreateVolumeRequest{RequestId: "cleanup-volume", VolumeId: "retained"}); err != nil {
		t.Fatal(err)
	}
	retried, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-reset", Plan: agentPlan.GetPlan()})
	if err != nil || retried.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_FAILED || retried.GetOperation().GetId() != reset.GetOperation().GetId() {
		t.Fatalf("terminal cleanup replay = %v, %v", retried, err)
	}
	volumeSession, err := client.session("cleanup-source")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := volumeSession.client.GetVolume(ctx, &hostv1.GetVolumeRequest{VolumeId: "retained"}); err != nil {
		t.Fatalf("same-request cleanup replay deleted new volume: %v", err)
	}
	incarnationPlan, err := client.operator.PlanResetEnvironment(ctx, &hostv1.PlanResetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteVolume(ctx, "cleanup-source", &hostv1.DeleteVolumeRequest{RequestId: "cleanup-volume-delete", VolumeId: "retained"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateVolume(ctx, "cleanup-source", &hostv1.CreateVolumeRequest{RequestId: "cleanup-volume-recreate", VolumeId: "retained"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-stale-volume-generation", Plan: incarnationPlan.GetPlan()}); status.Code(err) != codes.Aborted {
		t.Fatalf("same-ID volume recreation stale-plan error = %v", err)
	}
	client.forgetSession("cleanup-source")
	if err := client.RemoveEnvironmentAccess(ctx, "cleanup-source", "cleanup-remove-access"); err != nil {
		t.Fatal(err)
	}
	stalePlan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanDiscardEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	currentPolicy, err := client.operator.GetEnvironment(ctx, &hostv1.OperatorServiceGetEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.operator.UpdateEnvironmentPolicy(ctx, &hostv1.UpdateEnvironmentPolicyRequest{RequestId: "cleanup-plan-change", EnvironmentId: "cleanup-source", Limits: currentPolicy.GetEnvironment().GetLimits(), EgressMode: currentPolicy.GetEnvironment().GetEgressMode(), ExpectedRevision: currentPolicy.GetEnvironment().GetPolicyRevision()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-stale-plan", Plan: stalePlan.GetPlan(), ConfirmedVolumeIds: []string{"retained"}}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale cleanup plan error = %v", err)
	}
	plan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanDiscardEnvironmentRequest{EnvironmentId: "cleanup-source"})
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
		(id, namespace_id, display_name, pids, disk_bytes, egress_mode, policy_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, environment.GetId(), environment.GetNamespaceId(), environment.GetDisplayName(),
		environment.GetLimits().GetPids(), environment.GetLimits().GetDiskBytes(),
		int32(environment.GetEgressMode()), environment.GetPolicyRevision(), now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE operations SET state=?, stage='namespace-deleted', error_reason='TEST_INTERRUPTION' WHERE request_id='cleanup-discard'", int32(hostv1.OperationState_OPERATION_STATE_FAILED)); err != nil {
		t.Fatal(err)
	}
	recovered, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-discard", Plan: plan.GetPlan(), ConfirmedVolumeIds: []string{"retained"}})
	if err != nil || recovered.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_FAILED || recovered.GetOperation().GetId() != discard.GetOperation().GetId() {
		t.Fatalf("partial discard terminal replay = %v, %v", recovered, err)
	}
	freshPlan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanDiscardEnvironmentRequest{EnvironmentId: "cleanup-source"})
	if err != nil {
		t.Fatalf("fresh plan after missing namespace = %v", err)
	}
	freshRecovery, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "cleanup-discard-fresh-recovery", Plan: freshPlan.GetPlan()})
	if err != nil || freshRecovery.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("fresh request recovery after missing namespace = %v, %v", freshRecovery, err)
	}
	if _, err := client.operator.GetEnvironment(ctx, &hostv1.OperatorServiceGetEnvironmentRequest{EnvironmentId: "cleanup-source"}); status.Code(err) != codes.NotFound {
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
	environment, err := agent.GetEnvironment(forged, &hostv1.GetEnvironmentRequest{})
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
	if _, err := agent.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{}); err == nil {
		t.Fatal("revoked session remained usable")
	}
	deleteNamespace(t, client, "session-auth")
}

func testContainerLifecycle(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "lifecycle", 128<<20)
	importRegistryImage(t, client, "lifecycle")
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
	startAndWaitContainer(t, client, "lifecycle", limited.GetContainer().GetId(), 0)
	stream, err := client.Logs(ctx, "lifecycle", &hostv1.LogsRequest{ContainerId: limited.GetContainer().GetId()})
	if err != nil {
		t.Fatal(err)
	}
	truncated := false
	var logBytes int
	for {
		record, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		truncated = truncated || record.GetTruncated()
		logBytes += len(record.GetData())
	}
	if !truncated || logBytes == 0 || logBytes > 4096 {
		t.Fatalf("bounded log: truncated=%t bytes=%d", truncated, logBytes)
	}
	deleteContainer(t, client, "lifecycle", limited.GetContainer().GetId())
	deleteNamespace(t, client, "lifecycle")
}

func testNamedVolume(t *testing.T, client *productClient) {
	const image = "agent-vmm.local/provider-state:latest"
	ensureNamespace(t, client, "volume-owner", 128<<20)
	ensureNamespace(t, client, "volume-other", 128<<20)
	archive, digest := buildImportArchive(t)
	importTestImage(t, client, "volume-owner", "provider-state-owner", image, archive, digest)
	importTestImage(t, client, "volume-other", "provider-state-other", image, archive, digest)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owner, mode := uint32(1000), uint32(0o700)
	if _, err := client.CreateVolume(ctx, "volume-owner", &hostv1.CreateVolumeRequest{
		RequestId: "create-volume-owner-provider-state", VolumeId: "vol-provider-state",
		OwnerUid: &owner, OwnerGid: &owner, Mode: &mode,
	}); err != nil {
		t.Fatal(err)
	}
	mount := []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "vol-provider-state"}, Destination: "/workspace"}}
	env := []string{"HOME=/workspace"}
	writer := createContainerAsUser(t, client, "volume-owner", "writer", image, "1000:1000", []string{"/bin/provider-state", "write-codex"}, env, mount)
	startAndWaitContainer(t, client, "volume-owner", writer.GetId(), 0)
	deleteContainer(t, client, "volume-owner", writer.GetId())
	reader := createContainerAsUser(t, client, "volume-owner", "reader", image, "1000:1000", []string{"/bin/provider-state", "read-codex"}, env, mount)
	startAndWaitContainer(t, client, "volume-owner", reader.GetId(), 0)
	if logs := readLogs(t, client, "volume-owner", reader.GetId()); string(logs) != "authenticated\n" {
		t.Fatalf("provider readiness = %q", logs)
	}
	// Mutation receipts carry only identity and state; Get projects the launch.
	projected, err := client.GetContainer(ctx, "volume-owner", &hostv1.GetContainerRequest{ContainerId: reader.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if keys := projected.GetContainer().GetEnvironmentKeys(); len(keys) != 2 || !slices.Contains(keys, "HOME") || !slices.Contains(keys, "PATH") {
		t.Fatalf("provider state container projected unexpected environment keys: %v", keys)
	}
	if argv := projected.GetContainer().GetArgv(); !slices.Equal(argv, []string{"/bin/provider-state", "read-codex"}) {
		t.Fatalf("provider state container projected unexpected argv: %v", argv)
	}
	if err := client.DeleteVolume(ctx, "volume-owner", &hostv1.DeleteVolumeRequest{RequestId: "delete-mounted-provider-state", VolumeId: "vol-provider-state"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mounted volume delete error = %v", err)
	}
	deleteContainer(t, client, "volume-owner", reader.GetId())
	denied := createContainerAsUser(t, client, "volume-owner", "other-user", image, "1001:1001", []string{"/bin/provider-state", "read-codex"}, env, mount)
	if code := startAndWaitContainer(t, client, "volume-owner", denied.GetId(), 1); code == 0 {
		t.Fatal("another workload uid read the provider-owned 0700 volume")
	}
	deleteContainer(t, client, "volume-owner", denied.GetId())
	if _, err := client.CreateContainer(ctx, "volume-other", &hostv1.CreateContainerRequest{RequestId: "cross-mount", ContainerId: "cross", Image: image, EntrypointOverride: &hostv1.StringList{Values: []string{"/bin/provider-state", "read-codex"}}, CommandOverride: &hostv1.StringList{}, Volumes: mount, LogLimitBytes: 1 << 20}); err == nil {
		t.Fatal("cross-namespace volume mount succeeded")
	}
	listed, err := client.ListContainers(ctx, "volume-other")
	if err != nil || len(listed.GetContainers()) != 0 {
		t.Fatalf("other namespace containers = %+v, %v", listed, err)
	}
	deleteVolume(t, client, "volume-owner", "vol-provider-state")
	deleteNamespace(t, client, "volume-owner")
	deleteNamespace(t, client, "volume-other")
}

func testDiskQuota(t *testing.T, client *productClient) {
	ensureNamespace(t, client, "quota", 32<<20)
	ensureNamespace(t, client, "healthy", 64<<20)
	ensureNamespace(t, client, "shared-small", 2<<20)
	importRegistryImage(t, client, "quota")
	importRegistryImage(t, client, "healthy")
	// Shared OCI content is outside the workload's writable project quota.
	importRegistryImage(t, client, "shared-small")
	if imageByReference(t, client, "shared-small", workloadImage).GetDigest() != imageByReference(t, client, "quota", workloadImage).GetDigest() {
		t.Fatal("shared image identity differs across writable quota sizes")
	}
	createVolume(t, client, "quota", "data")
	full := createContainer(t, client, "quota", "full", []string{"dd", "if=/dev/zero", "of=/data/blob", "bs=1048576", "count=64"}, []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data"}})
	if code := startAndWaitContainer(t, client, "quota", full.GetId(), 1); code == 0 {
		t.Fatal("write beyond project quota succeeded")
	}
	healthy := createContainer(t, client, "healthy", "healthy", []string{"/bin/sh", "-c", "echo unaffected"}, nil)
	startAndWaitContainer(t, client, "healthy", healthy.GetId(), 0)
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

func testResidualScan(t *testing.T, client *productClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	for _, namespaceID := range []string{
		"lifecycle", "volume-owner", "volume-other", "file-cache", "quota", "healthy", "shared-small", "same",
		"isolated-source", "isolated-destination", "link-source", "link-source", "revoke-source",
		"revoke-source", "egress-deny", "egress-public", "forward", "dockerfile-build", "parallel-a",
		"parallel-b", "cancel-build", "quota-build", "restart-source", "restart-source", "force-source", "force-source",
	} {
		response, err := client.ListContainers(ctx, namespaceID)
		if err == nil && len(response.GetContainers()) != 0 {
			t.Fatalf("residual namespace %q has %d visible containers", namespaceID, len(response.GetContainers()))
		}
	}
}

func assertResident(t *testing.T, client *productClient) {
	t.Helper()
	for index := range 6 {
		namespace := fmt.Sprintf("resident-%d", index)
		waitRestoredContainer(t, client, namespace, "worker")
		if output, code := execContainer(t, client, namespace, "worker", []string{"cat", "/root/resident"}); code != 0 || string(output) != "durable\n" {
			t.Fatalf("recovered workload %d lost durable files: %q, exit %d", index, output, code)
		}
	}
}
