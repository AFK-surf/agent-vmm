//go:build darwin || linux

package appliance

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func updateDrainFixture(t *testing.T, frontends ...store.ApplianceFrontend) (*Owner, store.ApplianceInstallation) {
	t.Helper()
	return updateDrainFixtureAt(t, filepath.Join(t.TempDir(), "host.db"), frontends...)
}

func updateDrainFixtureAt(t *testing.T, path string, frontends ...store.ApplianceFrontend) (*Owner, store.ApplianceInstallation) {
	t.Helper()
	database, err := openTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	ctx := context.Background()
	manifestBytes, err := proto.Marshal(&appliancev1.ControllerApplianceManifest{SystemLimits: &hostv1.ResourceLimits{Pids: proto.Uint32(1), DiskBytes: proto.Uint64(1)}, RequestedCapacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: &hostv1.ResourceLimits{Pids: proto.Uint32(1), DiskBytes: proto.Uint64(1)}, MaxEgressMode: 1}, RequestedEgressMode: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "artifact", CanonicalHash: []byte("hash"), ProductID: "product", ImageDigest: "digest", ImageReference: "reference", Platform: "linux/arm64", Release: "1", Manifest: manifestBytes}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", State: 4, Revision: 1, RegistrationID: "registration", DataVolumeID: "volume", DataVolumeBytes: 1024}, store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1}, store.RemoteRegistration{ID: "registration", OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "installation", DisplayName: "private", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}, frontends, store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestOwnerWithWorkloads(t, &Owner{store: database}, testWorkloadState{workloadRunsRecovered: true})
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	admitTestLifecycle(t, controller)
	return controller, installation
}

func TestApplianceUpdateDrainFreezesAdmissionAndWaitsForExecutionEvidence(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		controller, installation := updateDrainFixture(t)
		seedApplianceUpdateWorkload(t, controller, installation.RegistrationID)
		database := controller.store
		server := controller
		if recovered {
			recoverTestWorkloads(t, controller)
		} else {
			controller.workloads.(*workload.Owner).ResetWorkloadRecovery()
		}
		if !recovered {
			releaseTestWorkloadExecution(t, controller, "workload", "unknown")
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- server.drainApplianceUpdate(ctx, installation) }()
		for {
			registration, err := database.GetRemoteRegistration(t.Context(), installation.RegistrationID)
			if err != nil {
				t.Fatal(err)
			}
			if registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
				break
			}
			select {
			case err := <-done:
				t.Fatalf("recovered=%t drain returned before freezing admission: %v", recovered, err)
			case <-t.Context().Done():
				t.Fatal("drain did not freeze admission")
			case <-time.After(time.Millisecond):
			}
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("recovered=%t drain=%v", recovered, err)
		}
		registration, err := database.GetRemoteRegistration(t.Context(), installation.RegistrationID)
		if err != nil || registration.State != 2 {
			t.Fatalf("admission not frozen: %+v %v", registration, err)
		}
		recoverTestWorkloads(t, controller)
		releaseTestWorkloadExecution(t, controller, "workload", "unknown")
		if err := server.drainApplianceUpdate(t.Context(), installation); err != nil {
			t.Fatal(err)
		}
		if testWorkloadInstance(controller, "workload", "main").InstanceID != "workload-instance" {
			t.Fatal("drain altered the workload")
		}
	}
}

func TestApplianceUpdateHostRestartResumesOriginalPhaseAndDeadline(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "running"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.db")
			controller, installation := updateDrainFixtureAt(t, path)
			seedApplianceUpdateWorkload(t, controller, installation.RegistrationID)
			deadline := time.Now().Add(time.Minute).UnixMilli()
			if expired {
				deadline = time.Now().Add(-time.Minute).UnixMilli()
			}
			request := &appliancev1.UpdateApplianceRequest{InstallationId: installation.ID, RequestId: "update", ExpectedRevision: installation.Revision, ArtifactId: "target", DeadlineUnixMillis: deadline}
			receipt, err := proto.Marshal(&appliancev1.ApplianceUpdateProgress{Request: request, SourceArtifactId: installation.ArtifactID, InstallationRevision: installation.Revision})
			if err != nil {
				t.Fatal(err)
			}
			operation, err := controller.store.CreateApplianceOperation(context.Background(), store.ApplianceOperation{ID: "update", InstallationID: installation.ID, RequestID: "update", Kind: 9, State: 1, Phase: 7, ExpectedRevision: installation.Revision, DeadlineUnixMillis: deadline, Receipt: receipt})
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.store.Close(); err != nil {
				t.Fatal(err)
			}
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			controller = newTestOwner(&Owner{store: database})
			controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
			admitTestLifecycle(t, controller)
			t.Cleanup(func() { database.Close() })
			seedApplianceUpdateWorkload(t, controller, installation.RegistrationID)
			if !expired {
				t.Cleanup(func() {
					_, err := controller.store.TransitionApplianceOperation(context.Background(), operation.ID, store.ApplianceOperationTransition{ExpectedState: 1, ExpectedPhase: 7, State: 4, Phase: 7, Receipt: receipt})
					if err != nil {
						t.Error(err)
					}
					recoverTestWorkloads(t, controller)
					setTestWorkloadState(t, controller, testWorkloadState{workloadRunsRecovered: true})
					unlock, lockErr := controller.applianceUpdates.Lock(context.Background(), operation.ID)
					if lockErr != nil {
						t.Fatal(lockErr)
					}

					unlock()
				})
			}
			if err := controller.RecoverInterruptedApplianceAdminOperations(context.Background()); err != nil {
				t.Fatal(err)
			}
			if expired {
				failed, err := controller.store.GetApplianceOperation(context.Background(), operation.ID)
				if err != nil || failed.State != 3 || failed.Phase != 7 || failed.DeadlineUnixMillis != deadline || string(failed.Receipt) != string(receipt) || failed.ErrorReason != "UPDATE_DEADLINE_EXPIRED" {
					t.Fatalf("expired restart changed phase/deadline/receipt: %+v err=%v", failed, err)
				}
				return
			}

			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			for {
				registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
				if err != nil {
					t.Fatal(err)
				}
				if registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("recovered update did not resume draining")
				case <-time.After(time.Millisecond):
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			grpcServer := grpc.NewServer(grpc.StreamInterceptor(auth.StreamInterceptor(auth.Identity{Role: auth.Operator})))
			appliancev1.RegisterApplianceAdminServiceServer(grpcServer, testApplianceServer{Owner: controller})
			go grpcServer.Serve(listener)
			defer grpcServer.Stop()
			connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			observer, cancel := context.WithCancel(ctx)
			defer cancel()
			watch, err := appliancev1.NewApplianceAdminServiceClient(connection).WatchApplianceOperation(observer, &appliancev1.WatchApplianceOperationRequest{InstallationId: installation.ID, OperationId: operation.ID})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = watch.Recv(); err != nil {
				t.Fatal(err)
			}
			cancel()
			running, err := controller.store.GetApplianceOperation(context.Background(), operation.ID)
			if err != nil || running.State != 1 || running.Phase != 7 || running.DeadlineUnixMillis != deadline || string(running.Receipt) != string(receipt) {
				t.Fatalf("restart or observer cancellation lost phase/deadline/receipt: %+v err=%v", running, err)
			}
		})
	}
}

func TestSealedApplianceUpdateRecoverRejectsReactivation(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	installation.State = 6
	installation, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute).UnixMilli()
	receipt, err := proto.Marshal(&appliancev1.ApplianceUpdateProgress{Request: &appliancev1.UpdateApplianceRequest{InstallationId: installation.ID, RequestId: "update", ArtifactId: "target", DeadlineUnixMillis: deadline}, SourceArtifactId: installation.ArtifactID, InstallationRevision: installation.Revision})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "update", InstallationID: installation.ID, RequestID: "update", Kind: 9, State: 3, Phase: 10, DeadlineUnixMillis: deadline, Receipt: receipt, ErrorReason: "TARGET_SEALED"})
	if err != nil {
		t.Fatal(err)
	}
	server := controller
	_, err = auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return server.RecoverApplianceUpdate(ctx, &appliancev1.RecoverApplianceUpdateRequest{InstallationId: installation.ID, OperationId: operation.ID, ExpectedUpdatedUnixNanos: operation.UpdatedAt.UnixNano(), DeadlineUnixMillis: deadline + 60000})
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("sealed recovery=%v", err)
	}
	current, _ := controller.store.GetApplianceOperation(ctx, operation.ID)
	if current.State != 3 || current.Phase != 10 || current.DeadlineUnixMillis != deadline || string(current.Receipt) != string(receipt) {
		t.Fatalf("sealed recovery mutated original operation: %+v", current)
	}
}

func TestPreparingUpdateFailurePreservesSourceControllerIdentity(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	installation.DesiredEnabled = true
	installation.CurrentBootID = "source-boot"
	installation.CurrentContainerInstance = "source-instance"
	installation, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := proto.Marshal(&appliancev1.ApplianceUpdateProgress{Request: &appliancev1.UpdateApplianceRequest{InstallationId: installation.ID, RequestId: "preparing-expired", ArtifactId: "target"}, SourceArtifactId: installation.ArtifactID, InstallationRevision: installation.Revision})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "preparing-expired", InstallationID: installation.ID, RequestID: "preparing-expired", Kind: 9, State: 1, Phase: 6, DeadlineUnixMillis: time.Now().Add(-time.Second).UnixMilli(), Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	controller.runApplianceUpdate(operation.ID)
	failed, err := controller.store.GetApplianceOperation(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != 3 || failed.Phase != 6 || string(failed.Receipt) != string(receipt) {
		t.Fatalf("preparing failure changed phase/receipt: %+v", failed)
	}
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current != installation {
		t.Fatalf("preparing failure changed the source: before=%+v after=%+v", installation, current)
	}
	currentGrant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(grantToProto(grant), grantToProto(currentGrant)) {
		t.Fatal("preparing failure changed the source grant")
	}
}

func TestGrantOnlyApplianceUpdatePlanAndIdempotency(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	controller.systemCapacity = store.Environment{PIDs: 10, DiskBytes: 10}
	withTestRemoteCapacity(controller, store.Environment{PIDs: 10, DiskBytes: 10})
	controller.manager = &vmm.Manager{}

	server := controller
	stored, err := controller.store.GetApplianceGrant(context.Background(), installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	grant := grantToProto(stored)
	grant.Revision = 999
	ctx := context.Background()
	operator := func(run func(context.Context) (any, error)) (any, error) {
		return auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(c context.Context, _ any) (any, error) { return run(c) })
	}
	request := &appliancev1.UpdateApplianceRequest{InstallationId: installation.ID, RequestId: "grant-only", ExpectedRevision: installation.Revision, ArtifactId: installation.ArtifactID, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ApprovedGrant: grant}
	plan := func(c context.Context) (any, error) {
		return server.PlanUpdate(c, &appliancev1.PlanUpdateRequest{InstallationId: request.InstallationId, RequestId: request.RequestId, ExpectedRevision: request.ExpectedRevision, ArtifactId: request.ArtifactId, DeadlineUnixMillis: request.DeadlineUnixMillis, ApprovedGrant: request.ApprovedGrant})
	}
	if _, err := operator(plan); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unchanged revision-only=%v", err)
	}
	grant.HostTcpEndpoints = []*appliancev1.HostTcpEndpoint{{Ip: "192.168.88.16", Port: 3000}}
	if _, err := operator(plan); err != nil {
		t.Fatalf("grant-only plan=%v", err)
	}
	_, _, err = controller.store.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "other-product", CanonicalHash: []byte("other-product"), ProductID: "other", Manifest: []byte("unused")}, store.ApplianceOperation{ID: "other-product-stage", RequestID: "other-product-stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	originalArtifact := request.ArtifactId
	request.ArtifactId = "other-product"
	if _, err := operator(plan); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-product=%v", err)
	}
	request.ArtifactId = originalArtifact
	submitted, err := operator(func(c context.Context) (any, error) { return server.UpdateAppliance(c, request) })
	if err != nil {
		t.Fatal(err)
	}
	again, err := operator(func(c context.Context) (any, error) { return server.UpdateAppliance(c, request) })
	if err != nil || again.(*appliancev1.UpdateApplianceResponse).GetOperation().GetOperationId() != submitted.(*appliancev1.UpdateApplianceResponse).GetOperation().GetOperationId() {
		t.Fatalf("replay=%v", err)
	}
	changed := proto.Clone(request).(*appliancev1.UpdateApplianceRequest)
	changed.ApprovedGrant.HostTcpEndpoints[0].Port = 3001
	if _, err := operator(func(c context.Context) (any, error) { return server.UpdateAppliance(c, changed) }); status.Code(err) != codes.Aborted {
		t.Fatalf("conflicting replay=%v", err)
	}
	// This fixture has no Guest; wait for the durable preparation failure before closing its store.
	for {
		op, err := controller.store.GetApplianceOperation(t.Context(), submitted.(*appliancev1.UpdateApplianceResponse).GetOperation().GetOperationId())
		if err != nil {
			t.Fatal(err)
		}
		if op.State != 1 {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal("update did not record preparation failure")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestEffectiveApplianceGrantIgnoresHostRevisionAndListOrder(t *testing.T) {
	original := &appliancev1.ApprovedGrant{Revision: 1, HostTcpEndpoints: []*appliancev1.HostTcpEndpoint{{Ip: "192.168.88.16", Port: 3000}, {Ip: "192.168.88.16", Port: 3001}}, Frontends: []*appliancev1.ControllerFrontend{{Name: "management"}, {Name: "webhook"}}}
	reordered := proto.Clone(original).(*appliancev1.ApprovedGrant)
	reordered.Revision = 20
	reordered.HostTcpEndpoints[0], reordered.HostTcpEndpoints[1] = reordered.HostTcpEndpoints[1], reordered.HostTcpEndpoints[0]
	reordered.Frontends[0], reordered.Frontends[1] = reordered.Frontends[1], reordered.Frontends[0]
	if !effectiveApplianceGrantsEqual(original, reordered) {
		t.Fatal("revision or order changed effective grant")
	}
	reordered.HostTcpEndpoints[0].Port = 3002
	if effectiveApplianceGrantsEqual(original, reordered) {
		t.Fatal("endpoint change was ignored")
	}
	if original.GetRevision() != 1 || original.HostTcpEndpoints[0].Port != 3000 || original.Frontends[0].Name != "management" {
		t.Fatal("comparison mutated original")
	}
}

func TestAppliancePreparationWaitsOnlyForExplicitResourceAdmission(t *testing.T) {
	for _, test := range []struct {
		code              codes.Code
		reason, dimension string
		wait              bool
	}{
		{codes.ResourceExhausted, "RESOURCE_CAPACITY_EXHAUSTED", "import_slot", true},
		{codes.ResourceExhausted, "RESOURCE_CAPACITY_EXHAUSTED", "storage_headroom", true},
		{codes.ResourceExhausted, "POOL_PER_ENVIRONMENT_LIMIT", "storage_headroom", false},
		{codes.ResourceExhausted, "RESOURCE_CAPACITY_EXHAUSTED", "writable", false},
		{codes.Unavailable, "RESOURCE_CAPACITY_EXHAUSTED", "import_slot", false},
	} {
		failure, err := status.New(test.code, "rejected").WithDetails(&errdetails.ErrorInfo{Reason: test.reason, Domain: hostv1.ErrorDomain, Metadata: map[string]string{"resource_dimension": test.dimension, "stage": map[string]string{"import_slot": "import_slot", "storage_headroom": "import_admission"}[test.dimension]}})
		if err != nil {
			t.Fatal(err)
		}
		if got := appliancePreparationCapacityWait(failure.Err()); got != test.wait {
			t.Fatalf("%+v wait=%t", test, got)
		}
	}
	if appliancePreparationCapacityWait(status.Error(codes.ResourceExhausted, "unknown")) {
		t.Fatal("untyped failure retried")
	}
}

type heldUpdateImportGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	held      atomic.Bool
	entered   chan string
	dimension string
}

func (guest *heldUpdateImportGuest) StageApplianceContent(stream grpc.ClientStreamingServer[vmmdv1.StageApplianceContentRequest, appliancev1.ApplianceContentStatus]) error {
	return stream.SendAndClose(&appliancev1.ApplianceContentStatus{Available: true})
}

func (guest *heldUpdateImportGuest) ImportImage(stream grpc.ClientStreamingServer[vmmdv1.ImportImageRequest, vmmdv1.Image]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	guest.entered <- request.GetHeader().GetRequestId()
	if guest.held.Load() {
		stage := "import_slot"
		if guest.dimension == "storage_headroom" {
			stage = "import_admission"
		}
		failure, err := status.New(codes.ResourceExhausted, "import capacity occupied").WithDetails(&errdetails.ErrorInfo{Reason: "RESOURCE_CAPACITY_EXHAUSTED", Domain: hostv1.ErrorDomain, Metadata: map[string]string{"resource_dimension": guest.dimension, "stage": stage}})
		if err != nil {
			return err
		}
		return failure.Err()
	}
	return stream.SendAndClose(&vmmdv1.Image{Digest: request.GetHeader().GetManifestDigest()})
}

func TestAppliancePreparationWaitResumesOriginalOperation(t *testing.T) {
	for _, outcome := range []string{"release", "headroom", "cancel", "stop"} {
		t.Run(outcome, func(t *testing.T) {
			controller, installation, artifact := stagedUpdateArchiveFixture(t)
			ctx := context.Background()
			artifact.CanonicalHash = []byte("target")
			artifact.ProductID = installation.ProductID
			_, _, err := controller.store.StageApplianceArtifact(ctx, artifact, store.ApplianceOperation{ID: "stage-target", RequestID: "stage-target", State: 2, Phase: 5})
			if err != nil {
				t.Fatal(err)
			}
			installation.DesiredEnabled = true
			installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
			installation.CurrentBootID = "source-boot"
			installation.CurrentContainerInstance = "source-instance"
			installation, err = controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Minute)
			progress := &appliancev1.ApplianceUpdateProgress{Request: &appliancev1.UpdateApplianceRequest{InstallationId: installation.ID, ArtifactId: artifact.ID, DeadlineUnixMillis: deadline.UnixMilli()}, SourceArtifactId: installation.ArtifactID, InstallationRevision: installation.Revision + 1}
			receipt, err := proto.Marshal(progress)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := controller.store.BeginApplianceStateMutation(ctx, installation, installation.Revision, store.ApplianceOperation{ID: "update-wait", InstallationID: installation.ID, RequestID: "update-wait", Kind: 9, State: 1, Phase: 6, DeadlineUnixMillis: deadline.UnixMilli(), Receipt: receipt})
			if err != nil {
				t.Fatal(err)
			}
			guest := &heldUpdateImportGuest{entered: make(chan string, 4), dimension: "import_slot"}
			if outcome == "headroom" {
				guest.dimension = "storage_headroom"
			}
			guest.held.Store(true)
			client := workloadGuestClient(t, guest)
			runCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := controller.prepareApplianceUpdate(runCtx, client, operation, installation, artifact, progress)
				done <- err
			}()
			var firstID string
			select {
			case firstID = <-guest.entered:
			case err := <-done:
				t.Fatalf("preparation ended before admission: %v", err)
			case <-runCtx.Done():
				t.Fatal("import did not reach held slot")
			}
			for {
				waiting, err := controller.store.GetApplianceOperation(ctx, operation.ID)
				if err != nil {
					t.Fatal(err)
				}
				if waiting.Stage == "waiting_for_resources" {
					break
				}
				if runCtx.Err() != nil {
					t.Fatal("capacity wait was not persisted")
				}
				time.Sleep(time.Millisecond)
			}
			old, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
			if err != nil || !old.DesiredEnabled || old.State != 3 || old.CurrentBootID != installation.CurrentBootID || old.CurrentContainerInstance != installation.CurrentContainerInstance || old.ArtifactID != installation.ArtifactID {
				t.Fatalf("held slot withdrew source: %+v %v", old, err)
			}
			if outcome == "release" || outcome == "headroom" {
				guest.held.Store(false)
			}
			if outcome == "cancel" {
				cancel()
			}
			if outcome == "stop" {
				_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(operatorCtx context.Context, _ any) (any, error) {
					return controller.setDesiredState(operatorCtx, applianceMutation{installation.ID, "stop", old.Revision, deadline.UnixMilli()}, old.Revision, false, appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP, "stop-pending")
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			resultErr := <-done
			current, err := controller.store.GetApplianceOperation(ctx, operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.DeadlineUnixMillis != deadline.UnixMilli() {
				t.Fatal("wait extended authorization")
			}
			switch outcome {
			case "release", "headroom":
				if resultErr != nil || current.ID != operation.ID || current.Phase != 7 {
					t.Fatalf("same update did not drain: %+v %v", current, resultErr)
				}
				if next := <-guest.entered; next != firstID {
					t.Fatalf("import identity changed: %s %s", firstID, next)
				}
			case "cancel":
				if !errors.Is(resultErr, context.Canceled) || current.Phase != 6 {
					t.Fatalf("cancellation: %+v %v", current, resultErr)
				}
				old, _ := controller.store.GetApplianceInstallation(ctx, installation.ID)
				if !old.DesiredEnabled || old.State != 3 {
					t.Fatal("cancellation withdrew source")
				}
			case "stop":
				if !errors.Is(resultErr, store.ErrOperationReceiptConflict) || current.State != 4 || current.Phase != 6 {
					t.Fatalf("stop: %+v %v", current, resultErr)
				}
				select {
				case <-guest.entered:
					t.Fatal("interrupted update retried import")
				default:
				}
			}
		})
	}
}

func seedApplianceUpdateWorkload(t *testing.T, controller *Owner, registrationID string) {
	t.Helper()
	setTestWorkloadState(t, controller, testWorkloadState{workloadRunsRecovered: true, workloadRuns: map[string]*testWorkloadRun{"workload": {registrationID: registrationID, instanceID: "workload-instance", executions: map[string]*testWorkloadExecution{"unknown": {}}}}})
}

func TestSeedOnlyUpdateKeepsSecretOutOfReceiptAndBindsReplay(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	controller.systemCapacity = store.Environment{PIDs: 10, DiskBytes: 10}
	withTestRemoteCapacity(controller, store.Environment{PIDs: 10, DiskBytes: 10})
	controller.manager = &vmm.Manager{}
	secrets, err := NewSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	controller.applianceSecrets = secrets
	stored, err := controller.store.GetApplianceGrant(t.Context(), installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	seed := bytes.Repeat([]byte{7}, 32)
	request := &appliancev1.UpdateApplianceRequest{InstallationId: installation.ID, RequestId: "seed-only", ExpectedRevision: installation.Revision, ArtifactId: installation.ArtifactID, ApprovedGrant: grantToProto(stored), DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), OpaqueBootstrapSeed: seed}
	submitted, err := controller.UpdateAppliance(t.Context(), proto.Clone(request).(*appliancev1.UpdateApplianceRequest))
	if err != nil {
		t.Fatal(err)
	}
	again, err := controller.UpdateAppliance(t.Context(), proto.Clone(request).(*appliancev1.UpdateApplianceRequest))
	if err != nil || again.GetOperation().GetOperationId() != submitted.GetOperation().GetOperationId() {
		t.Fatalf("replay: %v", err)
	}
	changed := proto.Clone(request).(*appliancev1.UpdateApplianceRequest)
	changed.OpaqueBootstrapSeed[0]++
	if _, err := controller.UpdateAppliance(t.Context(), changed); status.Code(err) != codes.Aborted {
		t.Fatalf("different seed replay: %v", err)
	}
	for {
		operation, err := controller.store.GetApplianceOperation(t.Context(), submitted.GetOperation().GetOperationId())
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(operation.Receipt, seed) {
			t.Fatal("operation receipt exposes bootstrap secret")
		}
		progress, err := updateProgress(operation)
		if err != nil {
			t.Fatal(err)
		}
		if progress.GetRequest().OpaqueBootstrapSeed != nil || progress.GetBootstrapId() == "" {
			t.Fatal("receipt does not reference protected seed")
		}
		retained, err := secrets.LoadBootstrap(t.Context(), installation.ID, progress.GetBootstrapId())
		if err != nil || !bytes.Equal(retained, seed) {
			t.Fatalf("protected seed: %v", err)
		}
		if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
			break
		}
		select {
		case <-t.Context().Done():
			t.Fatal("update did not finish")
		case <-time.After(time.Millisecond):
		}
	}
}
