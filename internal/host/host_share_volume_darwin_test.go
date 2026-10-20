//go:build darwin && platform_spike

package host

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostShareRejectsDiskImageSource(t *testing.T) {
	work := t.TempDir()
	image := filepath.Join(work, "source.dmg")
	mountpoint := filepath.Join(work, "mount")
	run := func(args ...string) {
		t.Helper()
		if output, err := exec.Command("hdiutil", args...).CombinedOutput(); err != nil {
			t.Fatalf("hdiutil %v: %v %s", args, err, output)
		}
	}
	run("create", "-size", "64m", "-fs", "APFS", "-volname", "AgentVMMStaticSharePolicy", image)
	run("attach", "-nobrowse", "-mountpoint", mountpoint, image)
	defer run("detach", mountpoint)
	source := filepath.Join(mountpoint, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	file, _, err := openHostShareSource(source)
	if err == nil {
		file.Close()
		t.Fatal("disk image accepted as an internal fixed host share source")
	}
	if !strings.Contains(err.Error(), "internal local volume") {
		t.Fatalf("source rejected without actual volume policy observation: %v", err)
	}
	t.Logf("mounted APFS source rejected: %v", err)
}
