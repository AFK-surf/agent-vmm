//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"golang.org/x/sys/unix"
)

func TestPrivateProjectUsageCoversAllPoolProjectsAndPreservesUnknown(t *testing.T) {
	calls := 0
	next := func(_ string, cursor uint32) (uint32, guestquota.State, error) {
		calls++
		if cursor >= namespaceProjectBase+70 {
			return applianceProjectBase, guestquota.State{UsageBytes: 999}, nil
		}
		return cursor, guestquota.State{UsageBytes: 4096}, nil
	}
	got, err := privateProjectUsage(context.Background(), "pool", next)
	if err != nil || got != 70*4096 || calls != 71 {
		t.Fatalf("private=%d calls=%d err=%v", got, calls, err)
	}
	_, err = privateProjectUsage(context.Background(), "pool", func(string, uint32) (uint32, guestquota.State, error) { return 0, guestquota.State{}, unix.EOPNOTSUPP })
	if !errors.Is(err, unix.EOPNOTSUPP) {
		t.Fatalf("unknown quota became zero: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := privateProjectUsage(ctx, "pool", next); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDiskObservationReadsCachedSampleAndExpiresIt(t *testing.T) {
	zero, images, cache, private := uint64(0), uint64(7), uint64(5), uint64(3)
	r := &Runtime{diskSampleWake: make(chan struct{}, 1), diskSample: &vmmdv1.DiskAccounting{UsedBytes: 15, ImageRuntimeBytes: &images, BuildCacheBytes: &cache, PrivateBytes: &private, OtherBytes: &zero, BreakdownSampledAtUnixNano: time.Now().UnixNano()}}
	for i := 0; i < 100; i++ {
		got, err := r.observedDiskAccounting()
		if err != nil || got.GetImageRuntimeBytes() != 7 || got.OtherBytes == nil {
			t.Fatalf("cached read=%v %v", got, err)
		}
		got.UsedBytes = 99
	}
	if len(r.diskSampleWake) != 1 {
		t.Fatal("sample requests did not coalesce")
	}
	r.diskSample.BreakdownSampledAtUnixNano = time.Now().Add(-16 * time.Second).UnixNano()
	if got, err := r.observedDiskAccounting(); err == nil || got != nil {
		t.Fatalf("expired sample=%v %v", got, err)
	}
}

func TestDiskCategoriesUsePhysicalProjectUsage(t *testing.T) {
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable ext4 project-quota mount")
	}
	root, err := os.MkdirTemp(stateRoot, "disk-categories-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	for i, project := range []uint32{ociProjectID, cacheProjectID, namespaceProjectBase + 900, namespaceProjectBase + 901} {
		directory := filepath.Join(root, []string{"images", "cache", "private-a", "private-b"}[i])
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		if err := assignProject(directory, project); err != nil {
			t.Fatal(err)
		}
		if err := setProjectQuota(device, project, 64<<20); err != nil {
			t.Fatal(err)
		}
		file, err := os.Create(filepath.Join(directory, "payload"))
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Fallocate(int(file.Fd()), 0, 0, int64(i+1)<<20); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	r := &Runtime{stateDevice: device}
	sample, err := r.sampleDiskAccounting(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sample.ImageRuntimeBytes == nil || sample.BuildCacheBytes == nil || sample.PrivateBytes == nil || sample.OtherBytes == nil {
		images, e1 := projectUsage(device, ociProjectID)
		cache, e2 := projectUsage(device, cacheProjectID)
		private, e3 := privateProjectUsage(context.Background(), device, guestquota.Next)
		t.Fatalf("missing categories: %v, images %d %v cache %d %v private %d %v", sample, images, e1, cache, e2, private, e3)
	}
	if sample.GetImageRuntimeBytes() < 1<<20 || sample.GetBuildCacheBytes() < 2<<20 || sample.GetPrivateBytes() < 7<<20 {
		t.Fatalf("project usage=%v", sample)
	}
	if sample.GetImageRuntimeBytes()+sample.GetBuildCacheBytes()+sample.GetPrivateBytes()+sample.GetOtherBytes() != sample.UsedBytes {
		t.Fatalf("partition does not equal used: %v", sample)
	}
	reservationRoot := filepath.Join(stateRoot, "import-reservations")
	if err := os.MkdirAll(reservationRoot, 0700); err != nil {
		t.Fatal(err)
	}
	reservationFile, err := os.CreateTemp(reservationRoot, "category-hold-")
	if err != nil {
		t.Fatal(err)
	}
	reservationPath := reservationFile.Name()
	reservationFile.Close()
	os.Remove(reservationPath)
	defer os.Remove(reservationPath)
	defer os.Remove(reservationPath + ".hold")
	if err := writeImportReservation(reservationPath, reservationPath+".hold", 8<<20); err != nil {
		t.Fatal(err)
	}
	withHold, err := r.sampleDiskAccounting(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	holds, _, err := importReservationSummary(reservationRoot)
	if err != nil {
		t.Fatal(err)
	}
	var stats unix.Statfs_t
	if err := unix.Statfs(stateRoot, &stats); err != nil {
		t.Fatal(err)
	}
	if withHold.UsedBytes+holds != (stats.Blocks-stats.Bfree)*uint64(stats.Bsize) {
		t.Fatalf("hold counted as usage: %v holds=%d", withHold, holds)
	}
	if withHold.ImportReservationBytes < 8<<20 {
		t.Fatalf("hold lost from reservation: %v", withHold)
	}
	creditLimit := withHold.GetBuildCacheBytes() + (16 << 20)
	if err := persistBuilderQuota(reservationPath+".writer", creditLimit); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(reservationPath + ".writer")
	withCredit, err := r.sampleDiskAccounting(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if withCredit.ImportReservationBytes != holds+(16<<20) || withCredit.GetBuildCacheBytes() != withHold.GetBuildCacheBytes() {
		t.Fatalf("unused builder quota became physical usage: %v", withCredit)
	}

	// No namespace client is present. Pool project quotas alone supply private
	// bytes, including projects beyond any environment page.
}
