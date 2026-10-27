//go:build linux

package control

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

func TestBuildInputFingerprintPreservesReceiptIdentity(t *testing.T) {
	header := &vmmdv1.BuildImageHeader{
		DockerfilePath: "docker/Dockerfile", Image: "example/image:tag", Target: proto.String("release"),
		BuildArgs: []*vmmdv1.BuildArgument{{Name: "A", Value: "B"}}, NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NAMESPACE_EGRESS,
		Secrets: []*vmmdv1.BuildSecret{{Id: "token"}}, BaseImageReference: "base:tag", BaseManifestDigest: "sha256:base", ContextSha256: "context",
	}
	legacy, err := proto.MarshalOptions{Deterministic: true}.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	header.RequestId, header.ActorNamespaceId, header.ExecutionId, header.ContainerId = "request", "owner", "execution", "container"
	header.ExpectedInstanceId, header.ExecutionDeadlineUnixNano, header.ControllerCacheScope = "instance", 123, "controller"
	input, err := proto.MarshalOptions{Deterministic: true}.Marshal(buildInputFingerprint(header))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(legacy, input) {
		t.Fatalf("fingerprint wire format changed: %x != %x", input, legacy)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(input)); got != "28119ca84e09fc109f61870e1e5f7d44b138a74591a2f96ab9b9dc5a373a45a5" {
		t.Fatalf("fingerprint = %s", got)
	}
}
