//go:build linux

package build

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeFailurePreservesBoundedFatalMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "errors.json")
	createError := errors.New("client sync EOF")
	for _, test := range []struct{ log, want string }{
		{`{"msg":"runtime info","level":"info"}{"msg":"creating loader: mount cgroup filesystem: operation not permitted","level":"error"}`, "creating loader: mount cgroup filesystem: operation not permitted"},
		{`{"msg":"new runtime fatal not known to this application","level":"error"}`, "new runtime fatal not known to this application"},
		{`not JSON`, "client sync EOF"},
	} {
		if err := os.WriteFile(path, []byte(test.log), 0o600); err != nil {
			t.Fatal(err)
		}
		err := runtimeFailure(path, createError)
		if !errors.Is(err, createError) || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("startup failure: %v", err)
		}
	}
	if err := os.WriteFile(path, []byte(`{"msg":"`+strings.Repeat("x", 4096)+`","level":"error"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtimeFailure(path, createError); len(err.Error()) > 2200 {
		t.Fatalf("unbounded fatal message: %d", len(err.Error()))
	}
	output := new(buildOutput)
	_, _ = output.Write([]byte("buildctl final failure\n"))
	failure := runtimeFailure(path, output.failure(1))
	var exit *ExitError
	if !errors.As(failure, &exit) || !strings.Contains(failure.Error(), "buildctl final failure") || !strings.Contains(failure.Error(), "runsc fatal:") {
		t.Fatalf("exit lost BuildKit type/output or runsc error: %v", failure)
	}
}

func TestStartupRuntimeUsesPrivateNonDebugLog(t *testing.T) {
	if _, err := os.Stat("/etc/containerd/runsc-build.toml"); err != nil {
		t.Skip("Guest runtime configuration is required")
	}
	workspace := &Workspace{root: t.TempDir(), secretRoot: t.TempDir()}
	if err := os.WriteFile(filepath.Join(workspace.secretRoot, "startup"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	options, path, err := workspace.startupRuntimeOptions()
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := os.ReadFile(options.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(configuration, []byte(`debug = "false"`)) || !bytes.Contains(configuration, []byte(`overlay2 = "root:memory"`)) || bytes.Contains(configuration, []byte("debug-log")) {
		t.Fatalf("runtime options not private non-debug config")
	}
	for name, mode := range map[string]os.FileMode{filepath.Dir(path): 0o700, path: 0o600} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("log permissions: %v, %v", info, err)
		}
	}
}
