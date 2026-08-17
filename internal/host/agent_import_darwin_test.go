//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestImportImageHeaderInjectsSessionNamespaceAndPreservesContentCommitment(t *testing.T) {
	hash := make([]byte, 32)
	hash[0] = 7
	header := &hostv1.ImportImageHeader{RequestId: "request", ImageReference: "example.com/agent/pi:latest", ArchiveSha256: hash,
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: "linux/arm64", ArchiveSize: 4096}
	guest := importHeaderToGuest(header, "session-namespace")
	if guest.GetActorNamespaceId() != "session-namespace" || guest.GetRequestId() != header.GetRequestId() || guest.GetImageReference() != header.GetImageReference() ||
		guest.GetManifestDigest() != header.GetManifestDigest() || guest.GetPlatform() != "linux/arm64" || guest.GetArchiveSize() != header.GetArchiveSize() || guest.GetArchiveSha256()[0] != 7 {
		t.Fatalf("guest import header = %+v", guest)
	}
	header.ArchiveSha256[0] = 9
	if guest.GetArchiveSha256()[0] != 7 {
		t.Fatal("guest content commitment aliases the caller buffer")
	}
}
