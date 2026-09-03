package store

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func TestTerminalAdmissionRecoveryNeverRevivesAllocation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", ProtocolVersion: "2", MaxEnvironments: 2,
		AggregateCPUMillis: 2000, AggregateMemoryBytes: 4 << 30, AggregatePIDs: 512, AggregateDiskBytes: 8 << 30,
		PerEnvCPUMillis: 2000, PerEnvMemoryBytes: 4 << 30, PerEnvPIDs: 512, PerEnvDiskBytes: 8 << 30,
		MaxEgressMode: 1, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}); err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, CPUWeight: 100, MemoryHighBytes: 768 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 2 << 30}
	environment, _, err := database.BeginWorkloadAdmission(ctx,
		Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)},
		WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: "registration", EnvironmentID: "environment", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 1000, CPUMaxMillis: 1500, MemoryHighBytes: 2 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AcquireRemoteLeaseProjection(ctx, RemoteLeaseProjection{LeaseID: "stale-lease", RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 6, EnvironmentID: environment.ID, WorkloadRef: "task", Generation: 1, ExpiresAt: time.Now().Add(time.Minute), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}, environment.PolicyRevision); err == nil {
		t.Fatal("stale allocation generation acquired a durable lease projection")
	}
	unchanged, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || unchanged.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) || unchanged.PolicyRevision != environment.PolicyRevision {
		t.Fatalf("stale projection mutated environment=%+v err=%v", unchanged, err)
	}
	environment, err = database.AcquireRemoteLeaseProjection(ctx, RemoteLeaseProjection{LeaseID: "lease", RegistrationID: "registration", AllocationID: "allocation", AllocationGeneration: 7, EnvironmentID: environment.ID, WorkloadRef: "task", Generation: 1, ExpiresAt: time.Now().Add(time.Minute), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}, environment.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	environment, err = database.FinishRemoteLeaseProjection(ctx, "registration", "lease", environment.ID, environment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
	obligation, loadErr := database.GetWorkloadReservation(ctx, "allocation", 7)
	if err != nil || loadErr != nil || obligation.Phase != WorkloadReservationReleaseRequired {
		t.Fatalf("automatic lease retirement obligation=%+v environment=%+v err=%v loadErr=%v", obligation, environment, err, loadErr)
	}
	receipt := []byte("exact-terminal-receipt")
	releasing, err := database.CommitGuestReservationForRelease(ctx, "allocation", 7, receipt)
	if err != nil || releasing.Phase != WorkloadReservationReleasing || !bytes.Equal(releasing.GuestReceipt, receipt) {
		t.Fatalf("terminal receipt=%+v err=%v", releasing, err)
	}
	current, err := database.GetEnvironment(ctx, environment.ID)
	if err != nil || current.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		t.Fatalf("terminal recovery revived allocation=%+v err=%v", current, err)
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation", 7, receipt); err == nil {
		t.Fatal("normal admission adopted a terminal release obligation")
	}
	admittedEnvironment, _, err := database.BeginWorkloadAdmission(ctx,
		Environment{ID: "environment-admitted", NamespaceID: "namespace-admitted", OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation-admitted", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY)},
		WorkloadReservation{AllocationID: "allocation-admitted", Generation: 8, RegistrationID: "registration", EnvironmentID: "environment-admitted", Resources: contract},
		resourcev2.Capacity{CPUGuaranteeMillis: 2000, CPUMaxMillis: 3000, MemoryHighBytes: 4 << 30, MemoryMaxBytes: 4 << 30, PIDMax: 512, WritableQuotaBytes: 8 << 30})
	if err != nil {
		t.Fatal(err)
	}
	admittedReceipt := []byte("admitted-receipt")
	if _, err := database.CommitGuestReservation(ctx, "allocation-admitted", 8, admittedReceipt); err != nil {
		t.Fatal(err)
	}
	admittedEnvironment, err = database.AcquireRemoteLeaseProjection(ctx, RemoteLeaseProjection{LeaseID: "lease-admitted", RegistrationID: "registration", AllocationID: "allocation-admitted", AllocationGeneration: 8, EnvironmentID: admittedEnvironment.ID, WorkloadRef: "task", Generation: 1, ExpiresAt: time.Now().Add(time.Minute), State: int32(hostv1.LeaseState_LEASE_STATE_ACTIVE)}, admittedEnvironment.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation-admitted", 8, admittedReceipt); err != nil {
		t.Fatalf("exact receipt replay while leased failed: %v", err)
	}
	stillLeased, err := database.GetEnvironment(ctx, admittedEnvironment.ID)
	if err != nil || stillLeased.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_LEASED) || stillLeased.PolicyRevision != admittedEnvironment.PolicyRevision {
		t.Fatalf("receipt replay regressed leased allocation=%+v err=%v", stillLeased, err)
	}
	admittedEnvironment, err = database.FinishRemoteLeaseProjection(ctx, "registration", "lease-admitted", admittedEnvironment.ID, admittedEnvironment.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
	admittedObligation, loadErr := database.GetWorkloadReservation(ctx, "allocation-admitted", 8)
	if err != nil || loadErr != nil || admittedEnvironment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) || admittedObligation.Phase != WorkloadReservationReleasing || !bytes.Equal(admittedObligation.GuestReceipt, admittedReceipt) {
		t.Fatalf("admitted automatic retirement=%+v obligation=%+v err=%v loadErr=%v", admittedEnvironment, admittedObligation, err, loadErr)
	}
}

func TestGuestReservationReceiptRetryIsExactAndAtomic(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "registration", Endpoint: "controller.example", DisplayName: "controller", ProtocolVersion: "2",
		MaxEnvironments: 2, AggregateCPUMillis: 2000, AggregateMemoryBytes: 4 << 30, AggregatePIDs: 512, AggregateDiskBytes: 16 << 30,
		PerEnvCPUMillis: 1500, PerEnvMemoryBytes: 2 << 30, PerEnvPIDs: 256, PerEnvDiskBytes: 8 << 30, MaxEgressMode: 1,
		MaxLeaseSeconds: 60, DisconnectGraceSeconds: 30, StopGraceSeconds: 30, State: 1, ConnectionState: 1,
		EnrollmentState: 2, ConnectorPhase: 1}); err != nil {
		t.Fatal(err)
	}
	contract := resourcev2.Contract{CPUGuaranteeMillis: 500, CPUMaxMillis: 1000, CPUWeight: 100,
		MemoryHighBytes: 768 << 20, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 4 << 30}
	capacity := resourcev2.Capacity{CPUGuaranteeMillis: 1000, CPUMaxMillis: 1500, MemoryHighBytes: 2 << 30, MemoryMaxBytes: 2 << 30, PIDMax: 256, WritableQuotaBytes: 8 << 30}
	environment, logical, err := database.BeginWorkloadAdmission(ctx,
		Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "workload", OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING)},
		WorkloadReservation{AllocationID: "allocation", Generation: 7, RegistrationID: "registration", EnvironmentID: "environment", Resources: contract}, capacity)
	if err != nil || logical.Phase != WorkloadReservationLogical || environment.AllocationState == 1 {
		t.Fatalf("logical admission=%+v environment=%+v err=%v", logical, environment, err)
	}
	receipt := []byte("exact-guest-receipt")
	first, err := database.CommitGuestReservation(ctx, "allocation", 7, receipt)
	if err != nil || first.Phase != WorkloadReservationAdmitted || !bytes.Equal(first.GuestReceipt, receipt) {
		t.Fatalf("first receipt commit=%+v err=%v", first, err)
	}
	// Simulate a lost Host response: retrying the same exact receipt must settle
	// the same row without double-accounting or creating another reservation.
	retry, err := database.CommitGuestReservation(ctx, "allocation", 7, receipt)
	if err != nil || retry.Phase != WorkloadReservationAdmitted || !bytes.Equal(retry.GuestReceipt, receipt) {
		t.Fatalf("receipt retry=%+v err=%v", retry, err)
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation", 7, []byte("conflict")); err == nil {
		t.Fatal("conflicting receipt replaced the exact Guest fact")
	}
	if _, err := database.CommitGuestReservation(ctx, "allocation", 6, receipt); err == nil {
		t.Fatal("stale generation committed a Guest reservation")
	}
	releasing, err := database.BeginWorkloadRelease(ctx, "allocation", 7, receipt)
	if err != nil || releasing.Phase != WorkloadReservationReleasing {
		t.Fatalf("release fence=%+v err=%v", releasing, err)
	}
	if _, err := database.BeginWorkloadRelease(ctx, "allocation", 7, receipt); err != nil {
		t.Fatalf("exact release retry failed: %v", err)
	}
	if _, err := database.BeginWorkloadRelease(ctx, "allocation", 6, receipt); err == nil {
		t.Fatal("stale generation adopted the release reservation")
	}
	pending, err := database.CommitGuestRelease(ctx, "allocation", 7, receipt)
	if err != nil || pending.Phase != WorkloadReservationReleasedPendingConfirm {
		t.Fatalf("released pending confirm=%+v err=%v", pending, err)
	}
	if _, err := database.CommitGuestRelease(ctx, "allocation", 7, receipt); err != nil {
		t.Fatalf("lost commit response was not idempotent: %v", err)
	}
	if _, err := database.CommitGuestRelease(ctx, "allocation", 7, []byte("conflict")); err == nil {
		t.Fatal("conflicting release receipt adopted pending confirmation")
	}
	current, err := database.GetEnvironment(ctx, "environment")
	if err != nil || current.AllocationState != 1 {
		t.Fatalf("admitted environment=%+v err=%v", current, err)
	}
	usage, err := database.WorkloadUsage(ctx, "registration")
	if err != nil || usage != (resourcev2.Capacity{}) {
		t.Fatalf("released pending confirmation still consumes admission=%+v err=%v", usage, err)
	}
}
