//go:build linux

package control

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateEnvironmentAllowsMultilineValues(t *testing.T) {
	if err := validateEnvironment([]string{"CONFIG=first line\nsecond line\rthird"}); err != nil {
		t.Fatalf("multiline environment was rejected: %v", err)
	}
	if err := validateEnvironment([]string{"CONFIG=contains\x00nul"}); err == nil {
		t.Fatal("NUL environment value was accepted")
	}
}

func TestCreateContainerInvalidLogBoundsAreKnownBeforeRuntimeAccess(t *testing.T) {
	for _, limit := range []uint64{0, 64<<20 + 1} {
		runtime := &Runtime{logger: slog.Default().With("component", "guest-control")}
		_, err := runtime.CreateContainer(context.Background(), &vmmdv1.CreateContainerRequest{ActorNamespaceId: "namespace", ContainerId: "container", Image: "base", LogLimitBytes: limit})
		rejected := false
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Metadata["admission"] == "not_accepted" {
				rejected = true
			}
		}
		if status.Code(err) != codes.InvalidArgument || !rejected {
			t.Fatalf("limit=%d code=%v err=%v", limit, status.Code(err), err)
		}
	}
}

func TestCreateRollbackOutlivesCanceledRequest(t *testing.T) {
	for _, failure := range []error{nil, errors.New("snapshot cleanup failed")} {
		index := newRuntimeInventoryIndex()
		index.ensureNamespace("namespace")
		if err := index.addContainer("namespace", "container"); err != nil {
			t.Fatal(err)
		}
		runtime := &Runtime{inventory: index, logger: slog.Default().With("component", "guest-control")}
		ctx, cancel := context.WithCancel(runtimeContext(t.Context()))
		cancel()
		container := &rollbackContainer{failure: failure}
		if err := runtime.rollbackNewContainer(ctx, "namespace", "container", container); !errors.Is(err, failure) {
			t.Fatal(err)
		}
		ids, err := index.containerIDs("namespace", 10)
		if err != nil || !container.deleted || (len(ids) == 0) != (failure == nil) {
			t.Fatalf("rollback deleted=%v ids=%v err=%v", container.deleted, ids, err)
		}
	}
}

type rollbackContainer struct {
	containerd.Container
	failure error
	deleted bool
}

func (c *rollbackContainer) Delete(ctx context.Context, _ ...containerd.DeleteOpts) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("cleanup has no deadline")
	}
	if namespace, _ := namespaces.Namespace(ctx); namespace != runtimeContainerdNamespace {
		return errors.New("cleanup lost runtime namespace")
	}
	c.deleted = true
	return c.failure
}
