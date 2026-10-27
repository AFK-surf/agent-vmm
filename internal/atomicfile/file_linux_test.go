//go:build linux

package atomicfile

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWriteFailurePreservesPublication(t *testing.T) {
	if root := os.Getenv("AGENT_VMM_ATOMIC_FAILURE_DIRECTORY"); root != "" {
		signal.Ignore(syscall.SIGXFSZ)
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 2, Max: 2}); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			name  string
			write func(string, []byte, os.FileMode) error
		}{{"state", ReplaceFile}, {"new", PublishOnce}} {
			err := test.write(filepath.Join(root, test.name), []byte("complete payload"), 0600)
			if !errors.Is(err, syscall.EFBIG) && !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("kernel write failure = %v", err)
			}
		}
		return
	}
	root := t.TempDir()
	path := filepath.Join(root, "state")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestWriteFailurePreservesPublication$", "-test.v")
	command.Env = append(os.Environ(), "AGENT_VMM_ATOMIC_FAILURE_DIRECTORY="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("write failure helper: %v\n%s", err, output)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "original" {
		t.Fatalf("failed replacement changed the published file: %q, %v", data, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "state" {
		t.Fatalf("failed publication left a target or temporary file: %v, %v", entries, err)
	}
}
