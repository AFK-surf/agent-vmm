package launchdservice

import (
	"context"
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

func TestRenderHostPlist(t *testing.T) {
	t.Parallel()
	for _, daemon := range []bool{false, true} {
		data := RenderHostPlist(HostParams{Home: "/Users/a<&", App: "/Users/a<&/Host.app", User: "a<&", Group: "staff", Daemon: daemon, DisablePairing: true})
		decoder := xml.NewDecoder(strings.NewReader(string(data)))
		for {
			_, err := decoder.Token()
			if err != nil {
				if err.Error() != "EOF" {
					t.Fatal(err)
				}
				break
			}
		}
		for _, required := range []string{bundle.HostBundleIdentifier, "a&lt;&amp;", "--state-size", "--bundle", "--instance", "--serial-log", "--operator-socket", "--remote-compute-socket", "--database", "--session-directory", "--data-worker", "--disable-personal-mesh-pairing"} {
			if !strings.Contains(string(data), required) {
				t.Fatalf("missing %q", required)
			}
		}
		if strings.Contains(string(data), "__") {
			t.Fatalf("unresolved template: %s", data)
		}
		if daemon && (!strings.Contains(string(data), "<key>UserName</key>") || !strings.Contains(string(data), "/Library/Logs/AgentVMM/host.stderr.log")) {
			t.Fatalf("daemon=%s", data)
		}
	}
}

func TestStateDiskReleaseAcrossRemoval(t *testing.T) {
	t.Parallel()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "state.raw")
	assertBusy := func(want bool) {
		t.Helper()
		owners, err := DefaultSystem().StateDiskOpenOwners(context.Background(), path)
		if err != nil || (len(owners) > 0) != want {
			t.Fatalf("owners=%v want=%v err=%v", owners, want, err)
		}
	}
	assertBusy(false)
	disk, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	assertBusy(true)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	assertBusy(true)
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	assertBusy(false)
}
