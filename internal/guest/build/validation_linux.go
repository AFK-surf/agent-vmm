//go:build linux

package build

import (
	"encoding/hex"
	"errors"
	"regexp"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/imageref"
	"github.com/opencontainers/go-digest"
)

var buildName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)

func validateHeader(header *vmmdv1.BuildImageHeader) error {
	if header == nil || header.GetRequestId() == "" || header.GetActorNamespaceId() == "" {
		return errors.New("build request and actor namespace are required")
	}
	if _, err := archivePath(header.GetDockerfilePath()); err != nil {
		return errors.New("invalid Dockerfile path")
	}
	if header.GetContextSha256() != "" {
		value, err := hex.DecodeString(header.GetContextSha256())
		if err != nil || len(value) != 32 || strings.ToLower(header.GetContextSha256()) != header.GetContextSha256() {
			return errors.New("build context requires canonical sha256")
		}
	}
	canonicalImage, err := imageref.NormalizeReference(header.GetImage())
	if err != nil {
		return errors.New("invalid image name")
	}
	header.Image = canonicalImage
	if header.GetBaseImageReference() != "" || header.GetBaseManifestDigest() != "" {
		reference, err := imageref.NormalizeReference(header.GetBaseImageReference())
		expected := digest.Digest(header.GetBaseManifestDigest())
		if err != nil || expected.Validate() != nil || expected.Algorithm() != digest.SHA256 {
			return errors.New("build base requires image reference and sha256 digest")
		}
		header.BaseImageReference = reference
	}
	if header.GetNetworkMode() != vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE && header.GetNetworkMode() != vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NAMESPACE_EGRESS {
		return errors.New("build network mode is required")
	}
	if len(header.GetBuildArgs()) > 128 || len(header.GetSecrets()) > 32 {
		return errors.New("too many build arguments or secrets")
	}
	seenArgs, seenSecrets := make(map[string]bool), make(map[string]bool)
	for _, argument := range header.GetBuildArgs() {
		if argument == nil || !buildName.MatchString(argument.GetName()) || seenArgs[argument.GetName()] || len(argument.GetValue()) > 1<<20 || strings.ContainsRune(argument.GetValue(), '\x00') {
			return errors.New("invalid build argument")
		}
		seenArgs[argument.GetName()] = true
	}
	for _, secret := range header.GetSecrets() {
		if secret == nil || !buildName.MatchString(secret.GetId()) || seenSecrets[secret.GetId()] {
			return errors.New("invalid build secret")
		}
		seenSecrets[secret.GetId()] = true
	}
	if header.Target != nil && (!buildName.MatchString(header.GetTarget()) || header.GetTarget() == "") {
		return errors.New("invalid build target")
	}
	return nil
}
