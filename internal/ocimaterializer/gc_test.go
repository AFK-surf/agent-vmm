package ocimaterializer

import (
	"testing"
	"time"
)

func TestSharedDigestIsReferencedGloballyAndDuplicateCandidateIsUnique(t *testing.T) {
	now := time.Now()
	facts := []ContentFact{{Digest: "shared", Namespace: "one", UpdatedAt: now}, {Digest: "shared", Namespace: "two", UpdatedAt: now.Add(time.Second)}, {Digest: "orphan", Namespace: "one", UpdatedAt: now}}
	candidates := SelectUnreferenced(facts, map[string]bool{"shared": true}, 10)
	if len(candidates) != 1 || candidates[0].Digest != "orphan" {
		t.Fatalf("candidates=%+v", candidates)
	}
	candidates = SelectUnreferenced(facts, nil, 10)
	if len(candidates) != 2 {
		t.Fatalf("duplicate global CAS candidate count=%d", len(candidates))
	}
}
