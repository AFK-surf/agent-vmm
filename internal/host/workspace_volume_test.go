//go:build darwin || linux

package host

import (
	"context"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkspaceVolumeRequiresCurrentNamespaceOwner(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateRemoteRegistration(ctx, observationTestRegistration("registration")); err != nil {
		t.Fatal(err)
	}
	environment := observationTestEnvironment("environment", "registration")
	if _, err := database.CreateEnvironment(ctx, environment); err != nil {
		t.Fatal(err)
	}
	if err := database.PutVolumeOwner(ctx, "workspace", environment.NamespaceID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, observationTestEnvironment("other-namespace", "registration")); err != nil {
		t.Fatal(err)
	}
	if err := database.PutVolumeOwner(ctx, "foreign", "other-namespace"); err != nil {
		t.Fatal(err)
	}
	server := &agentServer{controller: newTestController(&Controller{store: database})}
	identity := auth.Identity{EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID}
	for _, volume := range []string{"workspace", "foreign", "missing"} {
		err := server.requireWorkspaceVolume(ctx, identity, volume)
		if volume == "workspace" && err != nil {
			t.Fatalf("owned volume: %v", err)
		}
		if volume != "workspace" && status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unowned %s: %v", volume, err)
		}
	}
}

func TestWorkspaceGuestReadErrorKeepsCodeAndExistingDetails(t *testing.T) {
	for _, phase := range []string{"start", "receive"} {
		for _, code := range []codes.Code{codes.Unavailable, codes.NotFound, codes.Canceled} {
			original := status.Error(code, "private message /private/path")
			got := status.Convert(workspaceGuestReadError(original, phase))
			if got.Code() != code || got.Message() != "workspace guest relay failed" {
				t.Fatalf("unexpected fixed status: %v", got.Code())
			}
			info, ok := got.Details()[0].(*errdetails.ErrorInfo)
			if !ok || info.Reason != "WORKSPACE_GUEST_RELAY_FAILURE" || info.Domain != hostv1.ErrorDomain || info.Metadata["hop"] != "guest" || info.Metadata["phase"] != phase || len(info.Metadata) != 2 {
				t.Fatal("missing fixed relay attribution")
			}
		}
	}
	detailed, err := status.New(codes.Unavailable, "existing owner error").WithDetails(&errdetails.ErrorInfo{Reason: "LOCAL_TARGET_UNREADABLE", Domain: "agentvmm.afk.surf"})
	if err != nil {
		t.Fatal(err)
	}
	original := detailed.Err()
	if workspaceGuestReadError(original, "start") != original {
		t.Fatal("existing details were replaced")
	}
}
