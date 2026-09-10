//go:build linux

package bootstrap

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

func TestWorkloadFilesystemFullPreservesControlCommits(t *testing.T) {
	if os.Getenv("AGENT_VMM_PRIVILEGED_QUOTA_TEST") != "1" {
		t.Skip("requires disposable privileged loop devices")
	}
	root := t.TempDir()
	backing := filepath.Join(root, "state.raw")
	f, err := os.Create(backing)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(9 << 30); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("losetup", "--find", "--show", "--partscan", backing).CombinedOutput()
	if err != nil {
		t.Fatalf("attach test disk: %v: %s", err, out)
	}
	device := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if out, err := exec.Command("losetup", "--detach", device).CombinedOutput(); err != nil {
			t.Errorf("detach: %v: %s", err, out)
		}
	})
	if err := initializeStateDisk(device); err != nil {
		t.Fatal(err)
	}
	control, pool := filepath.Join(root, "control"), filepath.Join(root, "pool")
	for i, target := range []string{control, pool} {
		if err := os.Mkdir(target, 0700); err != nil {
			t.Fatal(err)
		}
		if err := unix.Mount(partitionDevice(device, i+1), target, "ext4", 0, "prjquota"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := unix.Unmount(target, 0); err != nil {
				t.Error(err)
			}
		})
	}
	db, err := sql.Open("sqlite", filepath.Join(control, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE state(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	fill, err := os.Create(filepath.Join(pool, "workload"))
	if err != nil {
		t.Fatal(err)
	}
	defer fill.Close()
	block := make([]byte, 1<<20)
	for {
		_, err := fill.Write(block)
		if errors.Is(err, unix.ENOSPC) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO state VALUES ('stopped')"); err != nil {
		t.Fatalf("control WAL on full pool: %v", err)
	}
	staging := filepath.Join(control, "recovery.tmp")
	if err := os.WriteFile(staging, []byte("recoverable"), 0600); err != nil {
		t.Fatal(err)
	}
	committed := filepath.Join(control, "recovery")
	if err := os.Rename(staging, committed); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(control)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Syncfs(int(dir.Fd())); err != nil {
		t.Fatal(err)
	}
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRow("SELECT value FROM state").Scan(&value); err != nil || value != "stopped" {
		t.Fatalf("durable control state=%q: %v", value, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", filepath.Join(control, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow("SELECT value FROM state").Scan(&value); err != nil || value != "stopped" {
		t.Fatalf("reopened control state=%q: %v", value, err)
	}
}
