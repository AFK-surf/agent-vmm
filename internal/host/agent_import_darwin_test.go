//go:build darwin

package host

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestImportImageHeaderInjectsSessionNamespaceAndPreservesContentCommitment(t *testing.T) {
	hash := make([]byte, 32)
	hash[0] = 7
	header := &hostv1.ImportImageHeader{RequestId: "request", ImageReference: "example.com/agent/pi:latest", ArchiveSha256: hash,
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: "linux/arm64", ArchiveSize: 4096,
		ArchiveUrl: "https://cue-release.example/runtime/sha256/archive"}
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

func TestValidateImportArchiveURL(t *testing.T) {
	if err := validateImportArchiveURL("https://cue-release.example/runtime/sha256/archive"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "http://cue-release.example/archive", "https://user@cue-release.example/archive", "https://cue-release.example/archive?token=mutable", "https://cue-release.example/archive#fragment"} {
		if err := validateImportArchiveURL(value); err == nil {
			t.Fatalf("validateImportArchiveURL(%q) succeeded", value)
		}
	}
}

func TestDownloadImportArchiveRequiresExactCommittedSize(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("archive"))
	}))
	defer server.Close()
	previousTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	defer func() { http.DefaultTransport = previousTransport }()
	var received strings.Builder
	if err := downloadImportArchive(context.Background(), server.URL, 7, func(chunk []byte) error {
		_, err := received.Write(chunk)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if received.String() != "archive" {
		t.Fatalf("downloaded %q", received.String())
	}
	if err := downloadImportArchive(context.Background(), server.URL, 6, func([]byte) error { return nil }); err == nil {
		t.Fatal("oversized archive succeeded")
	}
}
