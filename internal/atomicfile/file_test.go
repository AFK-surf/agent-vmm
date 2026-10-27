package atomicfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestPublish(t *testing.T) {
	for _, once := range []bool{false, true} {
		t.Run(map[bool]string{false: "replace", true: "once"}[once], func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "state")
			publish := ReplaceFile
			if once {
				publish = PublishOnce
			}
			if err := publish(path, []byte("first"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := publish(path, []byte("first"), 0600); err != nil {
				t.Fatal(err)
			}
			err := publish(path, []byte("second"), 0600)
			want := "second"
			if once {
				want = "first"
				if !errors.Is(err, fs.ErrExist) {
					t.Fatalf("conflict = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != want {
				t.Fatalf("data = %q, %v", data, err)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 1 {
				t.Fatalf("temporary files = %v, %v", entries, err)
			}
			directory := filepath.Join(root, "directory")
			if err := os.Mkdir(directory, 0700); err != nil {
				t.Fatal(err)
			}
			if err := publish(directory, []byte("x"), 0600); err == nil {
				t.Fatal("replaced directory")
			}
		})
	}
}
