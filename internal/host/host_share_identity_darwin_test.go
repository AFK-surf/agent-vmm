//go:build darwin

package host

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostShareSourceIdentityRejectsPathReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	file, identity, err := openHostShareSource(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if identity.FileID == 0 || identity.FileSystemUUID == "" {
		t.Fatalf("missing identity: %+v", identity)
	}
	if err := os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if replacement, err := reopenHostShareSource(identity); err == nil {
		replacement.Close()
		t.Fatal("replacement directory accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".original", path); err != nil {
		t.Fatal(err)
	}
	restored, err := reopenHostShareSource(identity)
	if err != nil {
		t.Fatal(err)
	}
	restored.Close()
}

func TestHostShareVolumePolicyRequiresInternalLocalFixedStorage(t *testing.T) {
	for _, test := range []struct {
		name                                  string
		internal, local, removable, ejectable bool
		allowed                               bool
	}{
		{"internal", true, true, false, false, true},
		{"external", false, true, false, false, false},
		{"network", true, false, false, false, false},
		{"removable", true, true, true, false, false},
		{"ejectable", true, true, false, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := hostShareVolumeSupported(test.internal, test.local, test.removable, test.ejectable); got != test.allowed {
				t.Fatalf("volume allowed = %t", got)
			}
		})
	}
}
