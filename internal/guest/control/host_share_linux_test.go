//go:build linux

package control

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestTypedMountLabelsAndRequestIdentity(t *testing.T) {
	owned := &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data"}
	shared := &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_HostShare{HostShare: "data"}, Destination: "/share", ReadOnly: true}
	body, err := encodeContainerMounts([]*vmmdv1.VolumeMount{owned, shared})
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := decodeContainerMounts(body)
	if err != nil || len(mounts) != 2 || !proto.Equal(mounts[0], owned) || !proto.Equal(mounts[1], shared) {
		t.Fatal(body, mounts, err)
	}
	request := &vmmdv1.CreateContainerRequest{ActorNamespaceId: "service", ContainerId: "web", Volumes: []*vmmdv1.VolumeMount{owned}}
	previous := containerRequestHash(request)
	request.Volumes = []*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_HostShare{HostShare: "data"}, Destination: "/data"}}
	if containerRequestHash(request) == previous {
		t.Fatal("typed source change reused creation identity")
	}
	previous = containerRequestHash(request)
	request.Volumes[0].ReadOnly = true
	if containerRequestHash(request) == previous {
		t.Fatal("access mode change reused creation identity")
	}
}

func TestHostShareProjectionCannotCrossNamespaceOrAmplifyWriteAccess(t *testing.T) {
	runtime := &Runtime{hostShares: map[string]*vmmdv1.HostShareProjection{"share": {ShareId: "share", ActorNamespaceId: "approved", ReadOnly: true, Revision: 1}}, logger: slog.Default().With("component", "guest-control")}
	for _, test := range []struct {
		name, namespace, id string
		readOnly            bool
	}{
		{"other environment", "other", "share", true},
		{"write amplification", "approved", "share", false},
		{"unknown grant", "approved", "missing", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtime.mountSource(test.namespace, &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_HostShare{HostShare: test.id}, ReadOnly: test.readOnly})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("unauthorized mount accepted: %v", err)
			}
		})
	}
	if _, err := runtime.mountSource("approved", &vmmdv1.VolumeMount{}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := runtime.applyHostShares(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.mountSource("approved", &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_HostShare{HostShare: "share"}, ReadOnly: true}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("withdrawn authority accepted: %v", err)
	}
}

func TestUnavailableShareDoesNotWithdrawAnotherEnvironmentProjection(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "ready"), 0700); err != nil {
		t.Fatal(err)
	}
	current, err := observeHostShareProjection(root, []*vmmdv1.HostShareProjection{{ShareId: "missing", ActorNamespaceId: "one", Revision: 1}, {ShareId: "ready", ActorNamespaceId: "other", Revision: 2}})
	if err != nil || len(current) != 1 || current["ready"].GetActorNamespaceId() != "other" || current["missing"] != nil {
		t.Fatalf("projection isolation: %v %v", current, err)
	}
	if err := os.Mkdir(filepath.Join(root, "missing"), 0700); err != nil {
		t.Fatal(err)
	}
	current, err = observeHostShareProjection(root, []*vmmdv1.HostShareProjection{{ShareId: "missing", ActorNamespaceId: "one", Revision: 1}, {ShareId: "ready", ActorNamespaceId: "other", Revision: 2}})
	if err != nil || len(current) != 2 {
		t.Fatalf("directory retry did not recover: %v %v", current, err)
	}
}
