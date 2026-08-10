package image

import "testing"

func TestNormalizeReferenceForPullBuildAndRun(t *testing.T) {
	tests := map[string]string{
		"postgres:16-alpine":        "docker.io/library/postgres:16-alpine",
		"pgvector/pgvector:pg16":    "docker.io/pgvector/pgvector:pg16",
		"registry.example/app:edge": "registry.example/app:edge",
	}
	for input, want := range tests {
		got, err := NormalizeReference(input)
		if err != nil || got != want {
			t.Fatalf("NormalizeReference(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
}
