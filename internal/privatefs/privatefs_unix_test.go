//go:build darwin || linux

package privatefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateFiles(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(string) error
		valid  bool
	}{
		{"private", func(string) error { return nil }, true},
		{"public", func(p string) error { return os.Chmod(p, 0o644) }, false},
		{"hardlink", func(p string) error { return os.Link(p, p+".link") }, false},
		{"symlink", func(p string) error {
			if err := os.Rename(p, p+".target"); err != nil {
				return err
			}
			return os.Symlink(p+".target", p)
		}, false},
		{"oversize", func(p string) error { return os.WriteFile(p, []byte("long secret"), 0o600) }, false},
		{"empty", func(p string) error { return os.Truncate(p, 0) }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key")
			if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := test.change(path); err != nil {
				t.Fatal(err)
			}
			data, err := ReadFile(path, 1, 6)
			if (err == nil) != test.valid {
				t.Fatalf("ReadFile = %q, %v", data, err)
			}
		})
	}
}

func TestPrivateDirectory(t *testing.T) {
	for _, mode := range []os.FileMode{0o700, 0o755} {
		path := filepath.Join(t.TempDir(), "private")
		if err := os.Mkdir(path, mode); err != nil {
			t.Fatal(err)
		}
		if err := EnsureDirectory(path); (err == nil) != (mode == 0o700) {
			t.Fatalf("mode %o: %v", mode, err)
		}
	}
	path := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(t.TempDir(), path); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirectory(path); err == nil {
		t.Fatal("accepted symlink")
	}
}

func TestWritableAncestor(t *testing.T) {
	for _, mode := range []os.FileMode{0o770, os.ModeSticky | 0o777} {
		parent := t.TempDir()
		if err := os.Chmod(parent, mode); err != nil {
			t.Fatal(err)
		}
		if err := ValidateAncestors(filepath.Join(parent, "source"), os.Geteuid()); err == nil {
			t.Fatalf("accepted mode %s", mode)
		}
	}
}
