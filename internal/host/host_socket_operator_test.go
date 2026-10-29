//go:build darwin || linux

package host

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOperatorApprovesAHostSocketForAnEnvironmentOrEveryOneAndRemovesIt(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "local", NamespaceID: "local", OwnerKind: store.OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: newTestController(&Controller{store: database, manager: &vmm.Manager{}})}
	dir, err := os.MkdirTemp("", "hs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "o.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	code := func(err error) codes.Code { return status.Code(err) }
	set := func(environment string, port uint32, path string) (*hostv1.SetHostSocketResponse, error) {
		return server.SetHostSocket(ctx, &hostv1.SetHostSocketRequest{RequestId: "r", EnvironmentId: environment, Port: port, Path: path})
	}
	for _, bad := range []struct {
		port uint32
		path string
	}{{7480, "relative.sock"}, {7480, dir + "/./o.sock"}, {53, socket}, {0, socket}, {70000, socket}} {
		if _, err := set("local", bad.port, bad.path); code(err) != codes.InvalidArgument {
			t.Fatalf("port %d path %q = %v", bad.port, bad.path, err)
		}
	}
	if _, err := set("missing", 7480, socket); code(err) != codes.NotFound {
		t.Fatalf("an environment that does not exist = %v", err)
	}
	got, err := set("local", 7480, socket)
	if err != nil || got.GetHostSocket().GetPort() != 7480 || got.GetHostSocket().GetPath() != socket {
		t.Fatalf("set = %v, %v", got, err)
	}
	// Every environment's, at a path whose socket is not there yet.
	if got, err := set("", 7481, plain+".later"); err != nil || got.GetHostSocket().GetEnvironmentId() != "" {
		t.Fatalf("every environment's = %v, %v", got, err)
	}
	listed, err := server.ListHostSockets(ctx, &hostv1.ListHostSocketsRequest{})
	if err != nil || len(listed.GetHostSockets()) != 2 || listed.GetHostSockets()[1].GetEnvironmentId() != "local" {
		t.Fatalf("list = %v, %v", listed, err)
	}
	if _, err := server.RemoveHostSocket(ctx, &hostv1.RemoveHostSocketRequest{RequestId: "r4", Port: 7481}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.RemoveHostSocket(ctx, &hostv1.RemoveHostSocketRequest{RequestId: "r2", EnvironmentId: "local", Port: 7480}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.RemoveHostSocket(ctx, &hostv1.RemoveHostSocketRequest{RequestId: "r3", EnvironmentId: "local", Port: 7480}); code(err) != codes.NotFound {
		t.Fatalf("removing again = %v", err)
	}
}
