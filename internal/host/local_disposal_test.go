//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLocalDisposalReturnsExactOwnerReceiptForLaterIntent(t *testing.T) {
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{
		ID: "disposed", Endpoint: "controller", State: 1, ConnectionState: 1, EnrollmentState: 2,
		ConnectorPhase: 1, EnrollmentRequestID: "disposed-enrollment", PerEnvPIDs: 128,
		PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{store: database})
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})))
	hostv1.RegisterOperatorServiceServer(server, &operatorServer{controller: controller})
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///operator", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	planRequest := &hostv1.PlanLocalDisposalRequest{TargetKind: hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION, TargetId: registration.ID}
	plan, err := client.PlanLocalDisposal(ctx, planRequest)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := client.ExecuteLocalDisposal(ctx, &hostv1.ExecuteLocalDisposalRequest{RequestId: "first-intent", Plan: plan.Plan, IrreversibleConfirmation: true})
	if err != nil {
		t.Fatal(err)
	}
	root, err := database.GetLocalDisposal(ctx, "registration", registration.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.cleanup.ReconcileLocalDisposal(ctx, root); err != nil {
		t.Fatal(err)
	}
	plan, err = client.PlanLocalDisposal(ctx, planRequest)
	if err != nil || plan.GetPlan().GetRevision() != registration.Revision+1 || plan.GetPlan().GetEnvironmentCount() != 0 {
		t.Fatalf("completed target plan=%v, %v", plan, err)
	}
	request := &hostv1.ExecuteLocalDisposalRequest{RequestId: "later-intent", Plan: plan.Plan, IrreversibleConfirmation: true}
	for attempt := 0; attempt < 2; attempt++ {
		replayed, err := client.ExecuteLocalDisposal(ctx, request)
		if err != nil || replayed.GetOperation().GetId() != accepted.GetOperation().GetId() ||
			replayed.GetOperation().GetRequestId() != "first-intent" ||
			replayed.GetOperation().GetKind() != "local-disposal" ||
			replayed.GetOperation().GetResource().GetEnvironmentId() != registration.ID ||
			replayed.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
			t.Fatalf("later intent attempt %d receipt=%v, %v", attempt, replayed, err)
		}
		// A lost reply must remain resumable if retained metadata changes afterward.
		if attempt == 0 {
			if _, err := database.UpdateRemotePool(ctx, registration, plan.Plan.Revision); err != nil {
				t.Fatal(err)
			}
		}
	}
	observed, err := client.GetOperation(ctx, &hostv1.GetOperationRequest{OperationId: accepted.GetOperation().GetId()})
	if err != nil || observed.GetOperation().GetRequestId() != "first-intent" || observed.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("original owner receipt=%v, %v", observed, err)
	}
	if _, err := database.OperationByRequestID(ctx, request.RequestId); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("later intent created a replacement receipt: %v", err)
	}
}
