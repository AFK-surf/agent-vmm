package appliancecontent

import (
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"strings"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/imageref"
	digest "github.com/opencontainers/go-digest"
)

const (
	MountRoot       = "/opt/agent-vmm/content"
	MaxArchiveBytes = 8 << 30
)

func ValidateReference(content *appliancev1.ApplianceContentReference) error {
	if content == nil || proto.Size(content) > 4096 || len(content.GetArchiveSha256()) != 32 || content.GetArchiveSize() == 0 || content.GetArchiveSize() > MaxArchiveBytes || content.GetPlatform() != "linux/arm64" {
		return errors.New("content requires bounded archive sha256/size and linux/arm64 platform")
	}
	switch content.GetRole() {
	case appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_HARNESS, appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_WORKLOAD:
	default:
		return errors.New("content role is invalid")
	}
	reference, err := imageref.NormalizeReference(content.GetImageReference())
	if err != nil || reference != content.GetImageReference() {
		return errors.New("content image reference must be canonical")
	}
	manifest, err := digest.Parse(content.GetImageManifestDigest())
	if err != nil || manifest.Algorithm() != digest.SHA256 || !strings.HasSuffix(reference, "@"+manifest.String()) {
		return errors.New("content image reference must bind its sha256 manifest digest")
	}
	return nil
}

func ArchiveName(content *appliancev1.ApplianceContentReference) string {
	return hex.EncodeToString(content.GetArchiveSha256()) + ".oci.tar"
}

func ValidateManifestReferences(manifest *appliancev1.ControllerApplianceManifest) error {
	refs := manifest.GetContentRefs()
	if proto.Size(&appliancev1.GetContentStatusRequest{ContentRefs: refs}) > 32<<10 {
		return errors.New("manifest content references exceed the frame limit")
	}
	seen := make(map[string]bool)
	controllers := 0
	for _, content := range refs {
		if err := ValidateReference(content); err != nil {
			return err
		}
		key := fmt.Sprintf("%d/%s", content.GetRole(), content.GetImageManifestDigest())
		if seen[key] {
			return errors.New("manifest repeats a role and digest authorization")
		}
		seen[key] = true
		if content.GetRole() == appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER {
			controllers++
			if controllers > 1 || content.GetImageManifestDigest() != manifest.GetImageManifestDigest() || content.GetImageReference() != manifest.GetImageReference() || content.GetPlatform() != manifest.GetPlatform() {
				return errors.New("controller content does not bind the manifest image")
			}
		}
	}
	return nil
}
