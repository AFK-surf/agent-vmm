package store

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestHostShareMutationFencesAndFrozenSourceSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "service", NamespaceID: "service", OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	owner, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "owner", EnvironmentID: "service", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "config", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	old := HostShare{ID: "share", EnvironmentID: "service", Source: HostShareSource{Path: "/original", FileSystemUUID: "original-fs", FileID: math.MaxUint64}, Revision: 1, State: hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE}
	createReceipt, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	create := Operation{ID: "create", Kind: "host-share-create", EnvironmentID: "service", RequestID: "create", Receipt: createReceipt, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING)}
	if _, err := database.BeginHostShareMutation(ctx, create, old, 0); err != nil {
		t.Fatal(err)
	}
	old.OperationID = create.ID
	create.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	if err := database.FinishHostShareMutation(ctx, create, old); err != nil {
		t.Fatal(err)
	}
	candidate := old
	candidate.Source = HostShareSource{Path: "/replacement", FileSystemUUID: "new-fs", FileID: 9}
	candidate.Revision = 2
	candidate.ReadOnly = true
	frozen, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	operation := Operation{ID: "rebind", Kind: "host-share-rebind", RequestID: "rebind", EnvironmentID: "service", RequestSHA256: []byte("payload"), Receipt: frozen, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "accepted"}
	if _, err := database.BeginHostShareMutation(ctx, operation, candidate, 1); err != nil {
		t.Fatal(err)
	}
	database.Close()
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := database.GetHostShare(ctx, "share")
	if err != nil || current.Source != old.Source || current.Revision != 2 || current.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING {
		t.Fatal(current, err)
	}
	replay, err := database.BeginHostShareMutation(ctx, operation, candidate, 1)
	if err != nil || replay.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || string(replay.Receipt) != string(frozen) {
		t.Fatal(replay, err)
	}
	conflict := operation
	conflict.RequestSHA256 = []byte("changed")
	if _, err := database.BeginHostShareMutation(ctx, conflict, candidate, 1); !errors.Is(err, ErrOperationReceiptConflict) {
		t.Fatal(err)
	}
	stale := operation
	stale.ID = "stale"
	stale.RequestID = "stale"
	if _, err := database.BeginHostShareMutation(ctx, stale, candidate, 1); !errors.Is(err, ErrOperationReceiptConflict) {
		t.Fatal(err)
	}
	if _, err := database.OperationByRequestID(ctx, "stale"); err == nil {
		t.Fatal("failed fence left accepted operation")
	}
	laterCandidate := candidate
	laterCandidate.Revision++
	laterBody, err := json.Marshal(laterCandidate)
	if err != nil {
		t.Fatal(err)
	}
	later := operation
	later.ID = "later"
	later.RequestID = "later"
	later.Receipt = laterBody
	later.RequestSHA256 = []byte("later")
	if _, err := database.BeginHostShareMutation(ctx, later, laterCandidate, candidate.Revision); err != nil {
		t.Fatal("explicit revision-fenced rebind could not replace unresolved intent", err)
	}
	superseded, err := database.GetOperation(ctx, operation.ID)
	if err != nil || superseded.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) {
		t.Fatal(superseded, err)
	}
	candidate.OperationID = operation.ID
	if err := database.FinishHostShareMutation(ctx, operation, candidate); !errors.Is(err, ErrOperationReceiptConflict) {
		t.Fatal("superseded operation changed authority", err)
	}
	candidate = laterCandidate
	operation = later
	candidate.OperationID = operation.ID
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	operation.Receipt = []byte("terminal")
	if err := database.FinishHostShareMutation(ctx, operation, candidate); err != nil {
		t.Fatal(err)
	}
	current, err = database.GetHostShare(ctx, "share")
	if err != nil || current.Source != candidate.Source || !current.ReadOnly {
		t.Fatal(current, err)
	}
	pending := operation
	pending.ID, pending.RequestID, pending.State = "pending-before-removal", "pending-before-removal", int32(hostv1.OperationState_OPERATION_STATE_RUNNING)
	pendingCandidate := candidate
	pendingCandidate.Revision++
	if _, err := database.BeginHostShareMutation(ctx, pending, pendingCandidate, candidate.Revision); err != nil {
		t.Fatal(err)
	}
	if ids, err := database.CompleteEnvironmentHostShareRevocation(ctx, "service"); err != nil || len(ids) != 0 {
		t.Fatal("offline cleanup revoked effective integration", ids, err)
	}
	stillRunning, err := database.GetOperation(ctx, pending.ID)
	if err != nil || stillRunning.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
		t.Fatal("offline cleanup interrupted effective authority", stillRunning, err)
	}
	if _, err := database.BeginAgentIntegrationRemoval(ctx, "remove", owner.ID, owner.Revision); err != nil {
		t.Fatal(err)
	}
	interrupted, err := database.GetOperation(ctx, pending.ID)
	if err != nil || interrupted.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) {
		t.Fatal("removal orphaned pending share receipt", interrupted, err)
	}
	original, err := database.GetOperation(ctx, create.ID)
	if err != nil || original.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		t.Fatal("removal changed historical success", original, err)
	}
	current, err = database.GetHostShare(ctx, "share")
	if err != nil || current.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING || current.OperationID != "" || current.Source != candidate.Source {
		t.Fatal(current, err)
	}
	revokeCandidate := current
	revokeCandidate.Revision++
	revokeCandidate.State = hostv1.HostShareState_HOST_SHARE_STATE_REVOKED
	revokeCandidate.OperationID = "revoke-pending"
	frozen, err = json.Marshal(revokeCandidate)
	if err != nil {
		t.Fatal(err)
	}
	revoke := Operation{ID: revokeCandidate.OperationID, Kind: "host-share-revoke", RequestID: "revoke-pending", EnvironmentID: "service", RequestSHA256: []byte("revoke"), Receipt: frozen, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING)}
	if _, err := database.BeginHostShareMutation(ctx, revoke, revokeCandidate, current.Revision); err != nil {
		t.Fatal("operator could not revoke pending authority", err)
	}
	if err := database.FinishHostShareMutation(ctx, operation, candidate); !errors.Is(err, ErrOperationReceiptConflict) {
		t.Fatal("old operation overwrote the newer revocation fence", err)
	}
	revoke.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	if err := database.FinishHostShareMutation(ctx, revoke, revokeCandidate); err != nil {
		t.Fatal(err)
	}

}
