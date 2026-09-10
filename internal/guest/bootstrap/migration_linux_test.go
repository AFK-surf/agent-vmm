//go:build linux

package bootstrap

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"github.com/containerd/containerd/v2/core/metadata"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func TestMigrateLegacyStatePreservesOwnedDataAndSplitsControl(t *testing.T) {
	if os.Getenv("AGENT_VMM_PRIVILEGED_MIGRATION_TEST") != "1" {
		t.Skip("requires disposable privileged loop devices")
	}
	root := t.TempDir()
	legacyBacking := filepath.Join(root, "legacy.raw")
	targetBacking := filepath.Join(root, "target.raw")
	for _, path := range []string{legacyBacking, targetBacking} {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Truncate(10 << 30); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	legacy := attachMigrationLoop(t, legacyBacking)
	target := attachMigrationLoop(t, targetBacking)
	if output, err := exec.Command("mkfs.ext4", stateFilesystemFormatArgs(legacy)...).CombinedOutput(); err != nil {
		t.Fatalf("format legacy: %v: %s", err, output)
	}
	legacyMount := filepath.Join(root, "legacy-mount")
	if err := os.Mkdir(legacyMount, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(legacy, legacyMount, "ext4", 0, "prjquota"); err != nil {
		t.Fatal(err)
	}
	for path, value := range map[string]string{
		"containerd/io.containerd.content.v1.content/blobs/value":   "shared-content",
		"containerd/io.containerd.snapshotter.v1.overlayfs/upper/x": "private-upper",
		"volumes/workload-a/home/account":                           "workload-account",
		"volumes/system.appliance/private/key":                      "system-key",
		"network/resolv/workload-a.conf":                            "workload-resolver",
		"network/resolv/system.appliance.conf":                      "system-resolver",
	} {
		full := filepath.Join(legacyMount, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(value), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	writeLegacyContainerdMetadata(t, legacyMount)
	workloadVolume := filepath.Join(legacyMount, "volumes/workload-a")
	if err := guestquota.Assign(workloadVolume, 10001); err != nil {
		t.Fatal(err)
	}
	if err := guestquota.Assign(filepath.Join(workloadVolume, "home/account"), 10001); err != nil {
		t.Fatal(err)
	}
	if err := guestquota.SetLimit(legacy, 10001, 64<<20); err != nil {
		t.Fatal(err)
	}
	if err := unix.Lsetxattr(filepath.Join(workloadVolume, "home/account"), "user.agent-vmm-test", []byte("preserved"), 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("account", filepath.Join(workloadVolume, "home/current")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(workloadVolume, "home/account"), filepath.Join(workloadVolume, "home/account-link")); err != nil {
		t.Fatal(err)
	}
	sparse, err := os.Create(filepath.Join(workloadVolume, "home/sparse"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sparse.Truncate(16 << 20); err != nil {
		sparse.Close()
		t.Fatal(err)
	}
	if _, err := sparse.WriteAt([]byte("tail"), 16<<20-4); err != nil {
		sparse.Close()
		t.Fatal(err)
	}
	if err := sparse.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(workloadVolume, "home/events"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := unix.Unmount(legacyMount, 0); err != nil {
		t.Fatal(err)
	}
	legacyDigest := migrationTestFileDigest(t, legacyBacking)
	report, err := MigrateLegacyState(legacy, target)
	if err != nil {
		t.Fatal(err)
	}
	if report.Entries < 7 || report.LogicalBytes == 0 {
		t.Fatalf("migration report = %+v", report)
	}
	if migrationTestFileDigest(t, legacyBacking) != legacyDigest {
		t.Fatal("read-only legacy disk changed")
	}
	controlMount, poolMount := filepath.Join(root, "control"), filepath.Join(root, "pool")
	for index, mount := range []string{controlMount, poolMount} {
		if err := os.Mkdir(mount, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := unix.Mount(partitionDevice(target, index+1), mount, "ext4", 0, "prjquota"); err != nil {
			t.Fatal(err)
		}
		defer unix.Unmount(mount, 0)
	}
	wants := map[string]string{
		filepath.Join(controlMount, "volumes/system.appliance/private/key"): "system-key",
		filepath.Join(controlMount, "network/resolv/system.appliance.conf"): "system-resolver",
		filepath.Join(poolMount, "containerd/content/blobs/value"):          "shared-content",
		filepath.Join(poolMount, "containerd/overlayfs/upper/x"):            "private-upper",
		filepath.Join(poolMount, "volumes/workload-a/home/account"):         "workload-account",
		filepath.Join(poolMount, "network/resolv/workload-a.conf"):          "workload-resolver",
	}
	for path, want := range wants {
		value, err := os.ReadFile(path)
		if err != nil || string(value) != want {
			t.Fatalf("%s = %q, %v", path, value, err)
		}
	}
	if value, err := os.ReadFile(filepath.Join(controlMount, namespaceconfig.LegacyStateMarker)); err != nil || string(value) != "v1\n" {
		t.Fatalf("migration marker=%q err=%v", value, err)
	}
	metadataPath := filepath.Join(controlMount, "containerd", "io.containerd.metadata.v1.bolt", "meta.db")
	database, err := bolt.Open(metadataPath, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var legacyLabels map[string]string
	if err := database.View(func(tx *bolt.Tx) error {
		var labelsErr error
		legacyLabels, labelsErr = metadata.NewNamespaceStore(tx).Labels(context.Background(), "workload-a")
		return labelsErr
	}); err != nil {
		t.Fatal(err)
	}
	converted, changed, err := namespaceconfig.ConvertLegacy(legacyLabels)
	if err != nil || !changed {
		t.Fatalf("legacy labels changed=%t err=%v", changed, err)
	}
	projectID, namespace, err := namespaceconfig.Decode("workload-a", converted)
	if err != nil || projectID != 10001 || namespace.GetLimits().GetDiskBytes() != 8_000_000_000 {
		t.Fatalf("migrated namespace project=%d namespace=%+v err=%v", projectID, namespace, err)
	}
	value := make([]byte, 32)
	size, err := unix.Lgetxattr(filepath.Join(poolMount, "volumes/workload-a/home/account"), "user.agent-vmm-test", value)
	if err != nil || string(value[:size]) != "preserved" {
		t.Fatalf("xattr = %q, %v", value[:size], err)
	}
	if link, err := os.Readlink(filepath.Join(poolMount, "volumes/workload-a/home/current")); err != nil || link != "account" {
		t.Fatalf("symlink = %q, %v", link, err)
	}
	account, err := os.Stat(filepath.Join(poolMount, "volumes/workload-a/home/account"))
	if err != nil {
		t.Fatal(err)
	}
	accountLink, err := os.Stat(filepath.Join(poolMount, "volumes/workload-a/home/account-link"))
	if err != nil || !os.SameFile(account, accountLink) {
		t.Fatalf("hard link was not preserved: %v", err)
	}
	sparseInfo, err := os.Stat(filepath.Join(poolMount, "volumes/workload-a/home/sparse"))
	if err != nil || sparseInfo.Size() != 16<<20 {
		t.Fatalf("sparse file = %v, %v", sparseInfo, err)
	}
	if stat := sparseInfo.Sys().(*syscall.Stat_t); stat.Blocks*512 >= sparseInfo.Size() {
		t.Fatalf("sparse extents were materialized: blocks=%d size=%d", stat.Blocks, sparseInfo.Size())
	}
	fifo, err := os.Lstat(filepath.Join(poolMount, "volumes/workload-a/home/events"))
	if err != nil || fifo.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO = %v, %v", fifo, err)
	}
	if project, err := guestquota.ProjectID(filepath.Join(poolMount, "volumes/workload-a/home/account")); err != nil || project != 10001 {
		t.Fatalf("project = %d, %v", project, err)
	}
}

func writeLegacyContainerdMetadata(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "containerd", "io.containerd.metadata.v1.bolt", "meta.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	labels := map[string]string{
		namespaceconfig.LabelPrefix + "project-id":                 "10001",
		namespaceconfig.LabelPrefix + "subnet":                     "10.64.1.0/24",
		namespaceconfig.LabelPrefix + "egress":                     "1",
		namespaceconfig.LegacyReservationPrefix + "allocation":     "allocation-a",
		namespaceconfig.LegacyReservationPrefix + "generation":     "7",
		namespaceconfig.LegacyReservationPrefix + "cpu-guarantee":  "1000",
		namespaceconfig.LegacyReservationPrefix + "cpu-max":        "7000",
		namespaceconfig.LegacyReservationPrefix + "cpu-weight":     "100",
		namespaceconfig.LegacyReservationPrefix + "memory-high":    "8000000000",
		namespaceconfig.LegacyReservationPrefix + "memory-max":     "9000000000",
		namespaceconfig.LegacyReservationPrefix + "pid-max":        "2048",
		namespaceconfig.LegacyReservationPrefix + "writable-quota": "8000000000",
	}
	if err := database.Update(func(tx *bolt.Tx) error {
		return metadata.NewNamespaceStore(tx).Create(context.Background(), "workload-a", labels)
	}); err != nil {
		t.Fatal(err)
	}
}

func migrationTestFileDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func attachMigrationLoop(t *testing.T, backing string) string {
	t.Helper()
	output, err := exec.Command("losetup", "--find", "--show", "--partscan", backing).CombinedOutput()
	if err != nil {
		t.Fatalf("attach %s: %v: %s", backing, err, output)
	}
	device := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if output, err := exec.Command("losetup", "--detach", device).CombinedOutput(); err != nil {
			t.Errorf("detach %s: %v: %s", device, err, output)
		}
	})
	return device
}
