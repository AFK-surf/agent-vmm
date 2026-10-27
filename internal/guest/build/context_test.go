package build

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractContext(t *testing.T) {
	tests := []struct {
		name    string
		headers []*tar.Header
		valid   bool
	}{
		{name: "regular tree", valid: true, headers: []*tar.Header{{Name: "Dockerfile", Mode: 0o644, Size: 12, Typeflag: tar.TypeReg}, {Name: "bin", Mode: 0o755, Typeflag: tar.TypeDir}, {Name: "bin/tool", Mode: 0o755, Size: 4, Typeflag: tar.TypeReg}, {Name: "tool", Linkname: "bin/tool", Typeflag: tar.TypeSymlink}}},
		{name: "absolute", headers: []*tar.Header{{Name: "/escape", Typeflag: tar.TypeReg}}},
		{name: "parent", headers: []*tar.Header{{Name: "../escape", Typeflag: tar.TypeReg}}},
		{name: "escaping symlink", headers: []*tar.Header{{Name: "link", Linkname: "../escape", Typeflag: tar.TypeSymlink}}},
		{name: "physical parent escape", headers: []*tar.Header{{Name: "a", Linkname: ".", Typeflag: tar.TypeSymlink}, {Name: "a/b", Linkname: "../x", Typeflag: tar.TypeSymlink}}},
		{name: "hardlink", valid: true, headers: []*tar.Header{{Name: "source", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg}, {Name: "linked", Linkname: "source", Typeflag: tar.TypeLink}}},
		{name: "escaping hardlink", headers: []*tar.Header{{Name: "linked", Linkname: "../escape", Typeflag: tar.TypeLink}}},
		{name: "device", headers: []*tar.Header{{Name: "device", Typeflag: tar.TypeChar}}},
		{name: "fifo", headers: []*tar.Header{{Name: "fifo", Typeflag: tar.TypeFifo}}},
		{name: "socket", headers: []*tar.Header{{Name: "socket", Typeflag: 's'}}},
		{name: "large file", headers: []*tar.Header{{Name: "large", Size: MaxFileBytes + 1, Typeflag: tar.TypeReg}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			writer := tar.NewWriter(&buffer)
			for _, header := range test.headers {
				if err := writer.WriteHeader(header); err != nil {
					t.Fatal(err)
				}
				if header.Size > 0 && header.Size <= MaxFileBytes {
					_, _ = writer.Write(bytes.Repeat([]byte("x"), int(header.Size)))
				}
			}
			_ = writer.Close()
			root := t.TempDir()
			err := ExtractContext(&buffer, root)
			if (err == nil) != test.valid {
				t.Fatalf("ExtractContext() = %v, valid = %v", err, test.valid)
			}
			if test.name == "regular tree" {
				data, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
				if err != nil || string(data) != strings.Repeat("x", 12) {
					t.Fatalf("Dockerfile = %q, %v", data, err)
				}
			}
		})
	}
}

func FuzzExtractContext(f *testing.F) {
	f.Add([]byte("not a tar"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_ = ExtractContext(bytes.NewReader(data), t.TempDir())
	})
}
