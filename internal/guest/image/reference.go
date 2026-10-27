package image

import "github.com/opencontainers/go-digest"

const MaterializedNamespace = "agent-vmm.runtime.v1"

func MaterializedReference(value digest.Digest) string {
	return "agent-vmm.internal/materialized@" + value.String()
}
