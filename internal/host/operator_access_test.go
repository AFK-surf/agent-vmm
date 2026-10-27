//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPrepareAgentIntegrationAppliesDeclaredLimitsOnlyOnCreate(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server := &operatorServer{controller: newTestController(&Controller{store: database})}
	limits := &hostv1.ResourceLimits{Pids: proto.Uint32(128), DiskBytes: proto.Uint64(4 << 30)}
	request := &hostv1.PrepareAgentIntegrationRequest{RequestId: "prepare", AdapterId: "service", DisplayName: "Service",
		Scope: hostv1.IntegrationScope_INTEGRATION_SCOPE_MANUAL, ConfigTarget: "service://test", EnvironmentId: proto.String("environment"), Limits: limits}
	request.Limits = nil
	if _, err := callAsOperator(t, func(ctx context.Context) (*hostv1.PrepareAgentIntegrationResponse, error) {
		return server.PrepareAgentIntegration(ctx, request)
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing limits: %v", err)
	}
	if _, err := database.GetEnvironment(context.Background(), "environment"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("invalid prepare created environment: %v", err)
	}
	request.Limits = limits
	prepared, err := callAsOperator(t, func(ctx context.Context) (*hostv1.PrepareAgentIntegrationResponse, error) {
		return server.PrepareAgentIntegration(ctx, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	environment, err := database.GetEnvironment(context.Background(), prepared.GetIntegration().GetEnvironmentId())
	if err != nil || environment.PIDs != limits.GetPids() || environment.DiskBytes != limits.GetDiskBytes() {
		t.Fatalf("new environment does not have declared limits: %+v %v", environment, err)
	}
	for _, nextLimits := range []*hostv1.ResourceLimits{nil, {Pids: proto.Uint32(512), DiskBytes: proto.Uint64(16 << 30)}} {
		request.RequestId += "-next"
		request.Limits = nextLimits
		if _, err := callAsOperator(t, func(ctx context.Context) (*hostv1.PrepareAgentIntegrationResponse, error) {
			return server.PrepareAgentIntegration(ctx, request)
		}); err != nil {
			t.Fatal(err)
		}
		current, err := database.GetEnvironment(context.Background(), environment.ID)
		if err != nil || current != environment {
			t.Fatalf("prepare changed an existing environment policy: %+v %v", current, err)
		}
	}
}
