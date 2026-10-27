package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func TestLocalDisposalSurvivesRestartAndFencesOnlyItsConfirmedScope(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	registration := RemoteRegistration{ID: "A", Endpoint: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 5, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1, EnrollmentRequestID: "old-install-managed-enrollment"}
	a, err := database.CreateRemoteRegistration(ctx, registration)
	if err != nil {
		t.Fatal(err)
	}
	registration.ID, registration.EnrollmentRequestID = "B", "other-enrollment"
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	capacity := resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 4 << 30}
	environment := Environment{ID: "env-A", NamespaceID: "namespace-A", OwnerKind: OwnerRemoteRegistration, RegistrationID: "A", AllocationID: "allocation-A", AllocationGeneration: 1, AllocationState: 2, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1}
	for i := 0; i < 101; i++ {
		item := environment
		item.ID = fmt.Sprintf("env-A-%03d", i)
		item.NamespaceID = item.ID
		item.AllocationID = item.ID
		if _, err := database.BeginWorkloadAdmission(ctx, item, capacity); err != nil {
			t.Fatal(err)
		}
	}
	other := environment
	other.ID, other.NamespaceID, other.RegistrationID = "env-B", "namespace-B", "B"
	if _, err := database.BeginWorkloadAdmission(ctx, other, capacity); err != nil {
		t.Fatal(err)
	}
	local := environment
	local.AllocationState = 0
	local.ID, local.NamespaceID, local.OwnerKind, local.RegistrationID, local.AllocationID = "A", "namespace-local", OwnerLocal, "", ""
	if _, err := database.CreateEnvironment(ctx, local); err != nil {
		t.Fatal(err)
	}
	operation := Operation{ID: "dispose-A", RequestID: "confirm-A"}
	root := LocalDisposal{Kind: "registration", TargetID: a.ID, Revision: a.Revision}
	accepted, err := database.AcceptLocalDisposal(ctx, operation, root)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Stage != "isolated" {
		t.Fatalf("accepted=%+v", accepted)
	}
	root, err = database.GetLocalDisposal(ctx, "registration", "A")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := database.OperationByRequestID(ctx, "confirm-A")
	if err != nil || receipt.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
		t.Fatalf("restart receipt=%+v,%v", receipt, err)
	}
	if _, err := database.SetRemoteRegistrationState(ctx, "A", a.Revision+1, 1); err == nil {
		t.Fatal("disposed registration enabled")
	}
	registration.ID, registration.EnrollmentRequestID = "replacement-with-old-enrollment", "old-install-managed-enrollment"
	if _, err := database.CreateRemoteRegistration(ctx, registration); err == nil {
		t.Fatal("disposed enrollment request replayed")
	}
	if _, err := database.BeginWorkloadAdmission(ctx, environment, capacity); err == nil {
		t.Fatal("disposed registration admitted new allocation")
	}
	for {
		page, err := database.ListRemoteEnvironmentPage(ctx, "A", "", 32)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, item := range page {
			child, err := database.CaptureDisposalEnvironment(ctx, root, item)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.FinishDisposalEnvironment(ctx, child); err != nil {
				t.Fatal(err)
			}
			if _, err := database.BeginWorkloadAdmission(ctx, item, capacity); err == nil {
				t.Fatal("deleted environment replayed")
			}
		}
	}
	if err := database.FinishLocalDisposal(ctx, root); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetEnvironment(ctx, other.ID); err != nil {
		t.Fatal("other registration changed", err)
	}
	if _, err := database.GetEnvironment(ctx, local.ID); err != nil {
		t.Fatal("local environment changed", err)
	}
	registration.ID, registration.EnrollmentRequestID = "new-legitimate-registration", "new-legitimate-enrollment"
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal("fresh enrollment rejected", err)
	}
	replay, err := database.AcceptLocalDisposal(ctx, operation, root)
	if err != nil || replay.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		t.Fatalf("replay=%+v,%v", replay, err)
	}
	root.TargetID = "B"
	if _, err := database.AcceptLocalDisposal(ctx, operation, root); err == nil {
		t.Fatal("same request changed scope")
	}
	if _, err := database.GetEnvironment(ctx, "env-A-000"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("disposed mapping remains", err)
	}
}

func TestOwnerLocalDisposalDoesNotRevokeAnotherLocalEnvironment(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var target Environment
	for _, id := range []string{"target", "other"} {
		environment, err := database.CreateEnvironment(ctx, Environment{ID: id, NamespaceID: id, OwnerKind: OwnerLocal, PIDs: 128, DiskBytes: 1 << 30, EgressMode: 1})
		if err != nil {
			t.Fatal(err)
		}
		if id == "target" {
			target = environment
		}
		count := 1
		if id == "target" {
			count = 101
		}
		for i := 0; i < count; i++ {
			if err := database.CreateSession(ctx, Session{ID: fmt.Sprintf("%s-%03d", id, i), EnvironmentID: id, SocketPath: fmt.Sprintf("%s-%03d.sock", id, i), CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	root := LocalDisposal{Kind: "environment", TargetID: target.ID, NamespaceID: target.NamespaceID, Revision: target.PolicyRevision}
	if _, err := database.AcceptLocalDisposal(ctx, Operation{ID: "dispose-target", RequestID: "confirm-target"}, root); err != nil {
		t.Fatal(err)
	}
	root, err = database.GetLocalDisposal(ctx, root.Kind, root.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	other, err := database.GetSession(ctx, "other-000")
	if err != nil || other.RevokedAt != nil {
		t.Fatalf("other local authority changed: %+v,%v", other, err)
	}
	closed := 0
	for {
		ids, err := database.DisposalSessionPage(ctx, target.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) > 32 {
			t.Fatalf("unbounded page: %d", len(ids))
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if err := database.CloseDisposedSession(ctx, target.ID, id); err != nil {
				t.Fatal(err)
			}
			closed++
		}
	}
	if closed != 101 {
		t.Fatalf("closed=%d", closed)
	}
	if err := database.FinishDisposalEnvironment(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := database.FinishLocalDisposal(ctx, root); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetSession(ctx, "other-000"); err != nil {
		t.Fatal("other local Session removed", err)
	}
}

func TestAllocationDisposalKeepsRegistrationAndAdjacentAllocationAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	registration := RemoteRegistration{ID: "shared", Endpoint: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 5, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	capacity := resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 4 << 30}
	target := Environment{ID: "env-target", NamespaceID: "namespace-target", OwnerKind: OwnerRemoteRegistration, RegistrationID: "shared", AllocationID: "target", AllocationGeneration: 7, AllocationState: 2, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1}
	target, err = database.BeginWorkloadAdmission(ctx, target, capacity)
	if err != nil {
		t.Fatal(err)
	}
	neighbor := target
	neighbor.ID, neighbor.NamespaceID, neighbor.AllocationID = "env-neighbor", "namespace-neighbor", "neighbor"
	if _, err := database.BeginWorkloadAdmission(ctx, neighbor, capacity); err != nil {
		t.Fatal(err)
	}
	root := LocalDisposal{Kind: "environment", TargetID: target.ID, NamespaceID: target.NamespaceID, RegistrationID: target.RegistrationID, AllocationID: target.AllocationID, AllocationGeneration: target.AllocationGeneration, Revision: target.PolicyRevision}
	operation := Operation{ID: "delete-target", RequestID: "delete-request"}
	stale := root
	stale.AllocationGeneration--
	if _, err := database.AcceptAllocationDisposal(ctx, operation, stale); err == nil {
		t.Fatal("stale generation accepted")
	}
	if _, err := database.AcceptLocalDisposal(ctx, operation, root); err == nil {
		t.Fatal("operator widened remote authority")
	}
	accepted, err := database.AcceptAllocationDisposal(ctx, operation, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginWorkloadAdmission(ctx, target, capacity); err == nil {
		t.Fatal("disposed allocation recreated")
	}
	root, err = database.AllocationDisposal(ctx, "shared", "target", 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := database.AcceptAllocationDisposal(ctx, operation, root)
	if err != nil || replay.ID != accepted.ID {
		t.Fatalf("replay=%+v, %v", replay, err)
	}
	if err := database.FinishDisposalEnvironment(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := database.FinishLocalDisposal(ctx, root); err != nil {
		t.Fatal(err)
	}
	completed, err := database.AllocationDisposal(ctx, "shared", "target", 7)
	if err != nil || !completed.Completed {
		t.Fatalf("completed=%+v, %v", completed, err)
	}
	if _, err := database.GetRemoteRegistration(ctx, "shared"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetEnvironment(ctx, neighbor.ID); err != nil {
		t.Fatal("adjacent allocation lost:", err)
	}
	target.ID = "another-env"
	target.NamespaceID = "another-namespace"
	if _, err := database.BeginWorkloadAdmission(ctx, target, capacity); err == nil {
		t.Fatal("allocation recreated with new environment ID")
	}
	target.AllocationID = "new-target"
	if _, err := database.BeginWorkloadAdmission(ctx, target, capacity); err != nil {
		t.Fatal("new authorized allocation rejected:", err)
	}
}
