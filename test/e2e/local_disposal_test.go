//go:build darwin && e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Every destructive target is inside this harness's new temporary Host and Guest disks.
func TestLocalDisposalE2E(t *testing.T) {
	harness := startHostWithState(t, 16<<30)
	harness.start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	local := harness.runtime(t)
	ensureNamespace(t, local, "disposal-local", 256<<20)
	createVolume(t, local, "disposal-local", "data")
	importRegistryImage(t, local, "disposal-local")
	container := createContainerWithImageAndVolumes(t, local, "disposal-local", "running", workloadImage, idleProcess(), []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data"}})
	startContainer(t, local, "disposal-local", container.GetId())
	if out, code := execContainer(t, local, "disposal-local", container.GetId(), []string{"sh", "-c", "echo local-private > /data/value; echo local-layer > /root/value; sync"}); code != 0 {
		t.Fatalf("local sentinel: %q, exit %d", out, code)
	}

	type domain struct {
		id          string
		environment string
		runtime     hostv1.AgentRuntimeServiceClient
		connection  *grpc.ClientConn
		stop        context.CancelFunc
		connect     func() (*grpc.ClientConn, hostv1.AgentRuntimeServiceClient)
	}
	domains := make([]domain, 0, 2)
	for index := range 2 {
		controller := &isolatedDomainController{session: make(chan net.Conn, 1), finish: make(chan struct{}), cleaned: make(chan struct{}), allowExistingInventory: true}
		endpoint, roots, stopServer := startRemoteController(t, controller)
		defer stopServer()
		id := fmt.Sprintf("disposal-domain-%d", index)
		pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: remoteLimits(100, 128<<20, 64, 256<<20), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StopGrace: durationpb.New(time.Second)}
		registration, err := harness.client.BeginRemoteEnrollment(ctx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: id, RegistrationId: id, Endpoint: endpoint, ProtocolVersion: remoteconnector.ProtocolVersion, DisplayName: id, Pool: pool})
		if err != nil {
			t.Fatal(err)
		}
		registration, err = harness.client.CompleteRemoteEnrollment(ctx, &hostv1.CompleteRemoteEnrollmentRequest{RequestId: id + "-complete", RegistrationId: id, ExpectedRevision: registration.GetRegistration().GetRevision()})
		if err != nil {
			t.Fatal(err)
		}
		controller.registrationID = id
		connectorContext, stop := context.WithCancel(ctx)
		defer stop()
		go func() {
			_ = remoteconnector.Run(connectorContext, remoteconnector.Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: id, RemoteComputeSocket: harness.remoteComputeSocket, CredentialStore: &e2eCredentialStore{value: []byte("remote-e2e")}, TLSRoots: roots})
		}()
		var raw net.Conn
		select {
		case raw = <-controller.session:
		case <-ctx.Done():
			t.Fatal("remote session did not open")
		}
		connection, err := grpc.NewClient("passthrough:///disposal-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return raw, nil }))
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		runtime := hostv1.NewAgentRuntimeServiceClient(connection)
		registration, err = harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: id})
		if err != nil || len(registration.GetRegistration().GetAllocations()) != 1 {
			t.Fatalf("registration=%v, %v", registration, err)
		}
		item := domain{id: id, environment: registration.GetRegistration().GetAllocations()[0].GetEnvironmentId(), runtime: runtime, connection: connection, stop: stop}
		before, err := harness.client.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: item.environment})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("fixture %s environment=%s namespace=%s", id, item.environment, before.GetEnvironment().GetNamespaceId())
		item.connect = func() (*grpc.ClientConn, hostv1.AgentRuntimeServiceClient) {
			reconnectContext, reconnectCancel := context.WithCancel(ctx)
			t.Cleanup(reconnectCancel)
			go func() {
				_ = remoteconnector.Run(reconnectContext, remoteconnector.Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: id, RemoteComputeSocket: harness.remoteComputeSocket, CredentialStore: &e2eCredentialStore{value: []byte("remote-e2e")}, TLSRoots: roots})
			}()
			var next net.Conn
			select {
			case next = <-controller.session:
			case <-ctx.Done():
				t.Fatal("preserved registration did not reopen")
			}
			c, err := grpc.NewClient("passthrough:///reopened-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return next, nil }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			return c, hostv1.NewAgentRuntimeServiceClient(c)
		}
		domains = append(domains, item)
		count := 1
		if index == 0 {
			count = 101
		}
		for volume := 0; volume < count; volume++ {
			volumeID := fmt.Sprintf("data-%03d", volume)
			if _, err := runtime.CreateVolume(ctx, &hostv1.CreateVolumeRequest{RequestId: id + "-" + volumeID, VolumeId: volumeID}); err != nil {
				t.Fatal(err)
			}
			writeDisposalSentinel(t, ctx, runtime, volumeID, id+"-private")
		}
	}
	// Ordinary removal retains the target's real writable layer and all 101 volumes.
	remote := newProductClient(harness.client)
	remote.archiveDirectory, remote.archiveURL, remote.registryArchives = local.archiveDirectory, local.archiveURL, local.registryArchives
	remote.sessions[domains[0].environment] = &productSession{connection: domains[0].connection, client: domains[0].runtime}
	importRegistryImage(t, remote, domains[0].environment)
	remoteContainer := createContainerWithImageAndVolumes(t, remote, domains[0].environment, "retained", workloadImage, idleProcess(), []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data-000"}, Destination: "/data"}})
	startContainer(t, remote, domains[0].environment, remoteContainer.GetId())
	if out, code := execContainer(t, remote, domains[0].environment, remoteContainer.GetId(), []string{"sh", "-c", "echo remote-layer > /root/value; sync"}); code != 0 {
		t.Fatalf("remote layer sentinel: %q, exit %d", out, code)
	}
	beforeRevoke, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: domains[0].id})
	if err != nil {
		t.Fatal(err)
	}
	beginRequest := &hostv1.BeginRemoteRevocationRequest{RequestId: "normal-remove-zero", RegistrationId: domains[0].id, ExpectedRevision: beforeRevoke.GetRegistration().GetRevision()}
	revoking, err := harness.client.BeginRemoteRevocation(ctx, beginRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.client.BeginRemoteRevocation(ctx, beginRequest); status.Code(err) != codes.Aborted {
		t.Fatalf("stale revision accepted: %v", err)
	}
	beginRequest.ExpectedRevision = revoking.GetRegistration().GetRevision()
	repeated, err := harness.client.BeginRemoteRevocation(ctx, beginRequest)
	if err != nil || repeated.GetRegistration().GetRevision() != revoking.GetRegistration().GetRevision() {
		t.Fatalf("repeat revoke=%v,%v", repeated, err)
	}
	retained, err := harness.client.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: domains[0].environment})
	if err != nil || len(revoking.GetRegistration().GetAllocations()) != 1 || revoking.GetRegistration().GetAllocations()[0].GetState() != hostv1.AllocationState_ALLOCATION_STATE_RETAINED || len(retained.GetEnvironment().GetVolumes()) != 101 {
		t.Fatalf("retained=%v,%v", retained, err)
	}
	if len(retained.GetEnvironment().GetContainers()) != 1 || retained.GetEnvironment().GetContainers()[0].GetState() != hostv1.ContainerState_CONTAINER_STATE_STOPPED {
		t.Fatalf("retained container still running: %v", retained)
	}
	revoked, err := harness.client.CompleteRemoteRevocation(ctx, &hostv1.CompleteRemoteRevocationRequest{RequestId: "normal-remove-zero-complete", RegistrationId: domains[0].id, ExpectedRevision: revoking.GetRegistration().GetRevision()})
	if err != nil || revoked.GetRegistration().GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
		t.Fatalf("complete revoke=%v,%v", revoked, err)
	}
	readDisposalSentinel(t, ctx, domains[1].runtime, "data-000", domains[1].id+"-private")
	// Remote environments cannot silently narrow the deletion scope to one allocation.
	if _, err := harness.client.PlanLocalDisposal(ctx, &hostv1.PlanLocalDisposalRequest{TargetKind: hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT, TargetId: domains[0].environment}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("remote environment plan: %v", err)
	}
	plan, err := harness.client.PlanLocalDisposal(ctx, &hostv1.PlanLocalDisposalRequest{TargetKind: hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION, TargetId: domains[0].id})
	if err != nil || plan.GetPlan().GetEnvironmentCount() != 1 {
		t.Fatalf("plan=%v,%v", plan, err)
	}
	request := &hostv1.ExecuteLocalDisposalRequest{RequestId: "dispose-domain-zero", Plan: plan.GetPlan(), IrreversibleConfirmation: true}
	if _, err := harness.client.ExecuteLocalDisposal(ctx, &hostv1.ExecuteLocalDisposalRequest{RequestId: request.RequestId, Plan: request.Plan}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unconfirmed deletion: %v", err)
	}
	// Guest unavailable makes the fence/receipt commit precede every deletion side effect.
	// Stop this sacrificial Guest through its shutdown owner before crashing the Host receipt owner.
	harness.stop(t, false)
	verifyRetainedDisposalDisk(t, harness.instance, retained.GetEnvironment().GetNamespaceId())
	accepted, err := harness.client.ExecuteLocalDisposal(ctx, request)
	if err != nil || accepted.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_RUNNING {
		t.Fatalf("accept=%v,%v", accepted, err)
	}
	for _, current := range domains {
		current.stop()
	}
	if _, err := harness.client.SetRemoteRegistrationState(ctx, &hostv1.SetRemoteRegistrationStateRequest{RequestId: "old-domain-control", RegistrationId: domains[0].id, ExpectedRevision: plan.GetPlan().GetRevision() + 1, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED}); err == nil {
		t.Fatal("disposed registration re-enabled")
	}
	harness.crashAndRestartHost(t)
	awaitDisposal(t, ctx, harness, accepted.GetOperation().GetId())
	replay, err := harness.client.ExecuteLocalDisposal(ctx, request)
	if err != nil || replay.GetOperation().GetId() != accepted.GetOperation().GetId() || replay.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("replay=%v,%v", replay, err)
	}
	if _, err := harness.client.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: domains[0].environment}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted environment still readable: %v", err)
	}
	if _, err := harness.client.BeginRemoteEnrollment(ctx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: domains[0].id, RegistrationId: domains[0].id, Endpoint: "https://localhost", ProtocolVersion: remoteconnector.ProtocolVersion, Pool: &hostv1.RemotePoolPolicy{PerEnvironmentLimits: remoteLimits(100, 128<<20, 64, 256<<20), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StopGrace: durationpb.New(time.Second)}}); err == nil {
		t.Fatal("old enrollment replayed")
	}
	_, preservedRuntime := domains[1].connect()
	other, err := harness.client.GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{EnvironmentId: domains[1].environment})
	if err != nil || other.GetEnvironment().GetId() != domains[1].environment {
		t.Fatalf("other registration environment: %v,%v", other, err)
	}
	t.Logf("preserved environment namespace=%s volumes=%v", other.GetEnvironment().GetNamespaceId(), other.GetEnvironment().GetVolumes())
	// Reopen the preserved local integration after the isolated Host restart and check real bytes.
	local = harness.runtime(t)
	startContainer(t, local, "disposal-local", container.GetId())
	if out, code := execContainer(t, local, "disposal-local", container.GetId(), []string{"sh", "-c", "cat /data/value /root/value"}); code != 0 || string(out) != "local-private\nlocal-layer\n" {
		t.Fatalf("local data changed: %q, exit %d", out, code)
	}
	readDisposalSentinel(t, ctx, preservedRuntime, "data-000", domains[1].id+"-private")
	// The same highest local authority can dispose an active OwnerLocal namespace.
	localPlan, err := harness.client.PlanLocalDisposal(ctx, &hostv1.PlanLocalDisposalRequest{TargetKind: hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT, TargetId: "disposal-local"})
	if err != nil {
		t.Fatal(err)
	}
	localAccepted, err := harness.client.ExecuteLocalDisposal(ctx, &hostv1.ExecuteLocalDisposalRequest{RequestId: "dispose-local", Plan: localPlan.GetPlan(), IrreversibleConfirmation: true})
	if err != nil {
		t.Fatal(err)
	}
	awaitDisposal(t, ctx, harness, localAccepted.GetOperation().GetId())
	if _, err := local.EnsureEnvironment(ctx, "disposal-local", "old-local-replay", nil, hostv1.EgressMode_EGRESS_MODE_DENY_ALL); err == nil {
		t.Fatal("old local environment recreated")
	}
	ensureNamespace(t, local, "new-disposal-local", 128<<20)
	readDisposalSentinel(t, ctx, preservedRuntime, "data-000", domains[1].id+"-private")
	deleteNamespace(t, local, "new-disposal-local")
	harness.stop(t, false)
}

func writeDisposalSentinel(t *testing.T, ctx context.Context, runtime hostv1.AgentRuntimeServiceClient, volumeID, value string) {
	t.Helper()
	stream, err := runtime.WriteWorkspace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Header{Header: &hostv1.WriteWorkspaceHeader{RequestId: "sentinel-" + value, Path: "value", VolumeId: volumeID, Mode: 0600}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Data{Data: []byte(value)}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.WriteWorkspaceRequest{Payload: &hostv1.WriteWorkspaceRequest_Finish{Finish: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	readDisposalSentinel(t, ctx, runtime, volumeID, value)
}
func readDisposalSentinel(t *testing.T, ctx context.Context, runtime hostv1.AgentRuntimeServiceClient, volumeID, value string) {
	t.Helper()
	read, err := runtime.ReadWorkspace(ctx, &hostv1.ReadWorkspaceRequest{Path: "value", VolumeId: volumeID, MaxBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	var result []byte
	for {
		chunk, err := read.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, chunk.GetData()...)
	}
	if string(result) != value {
		t.Fatalf("sentinel=%q", result)
	}
}

func awaitDisposal(t *testing.T, ctx context.Context, harness *hostHarness, id string) {
	t.Helper()
	for {
		receipt, err := harness.client.GetOperation(ctx, &hostv1.GetOperationRequest{OperationId: id})
		if err != nil {
			t.Fatal(err)
		}
		if receipt.GetOperation().GetState() == hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("disposal pending: %v", receipt)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// The opt-in disk check reads only this test's offline disk. CI without Docker still
// exercises the real Guest retirement and durable allocation/volume contracts.
func verifyRetainedDisposalDisk(t *testing.T, instance, namespaceID string) {
	t.Helper()
	if os.Getenv("AGENT_VMM_E2E_OFFLINE_DISK_CHECK") != "1" {
		t.Log("offline retained-byte check not requested")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	script := `set -eu
 apk add --no-cache util-linux jq >/dev/null
 start=$(sfdisk --json /fixture/state.raw | jq -r ".partitiontable.partitions[1].start")
 loop=$(losetup --find --show --read-only --offset "$((start*512))" /fixture/state.raw)
 trap 'umount /pool 2>/dev/null || true; losetup -d "$loop"' EXIT
 mkdir /pool
 mount -t ext4 -o ro,noload "$loop" /pool
 test "$(cat "/pool/volumes/$1/data-000/value")" = disposal-domain-0-private
 count=0
 for path in /pool/containerd/overlayfs/snapshots/*/fs/root/value; do
  test -f "$path" || continue
  if test "$(cat "$path")" = remote-layer; then count=$((count+1)); fi
 done
 test "$count" = 1
 echo 'retained volume and writable layer bytes verified'
`
	command := exec.CommandContext(ctx, "docker", "run", "--rm", "--platform", "linux/arm64", "--privileged", "-v", filepath.Join(instance, "state.raw")+":/fixture/state.raw:ro", "alpine:latest", "sh", "-c", script, "retained-byte-check", namespaceID)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("offline retained disk: %v: %s", err, output)
	}
	t.Log(string(output))
}
