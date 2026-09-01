package ocimaterializer

import "testing"

func TestSnapshotReferenceClosureProtectsActiveAndRetainedParentsPerNamespace(t *testing.T) {
	parents := map[string]string{"one\x00active": "one\x00layer", "two\x00retained": "two\x00layer"}
	refs := SnapshotReferenceClosure(map[string]bool{"one\x00active": true, "two\x00retained": true}, parents)
	for _, key := range []string{"one\x00active", "one\x00layer", "two\x00retained", "two\x00layer"} {
		if !refs[key] {
			t.Fatalf("snapshot %q lost its actual reference", key)
		}
	}
	if refs["three\x00orphan"] {
		t.Fatal("unreferenced snapshot was protected by a mirror fact")
	}
}
