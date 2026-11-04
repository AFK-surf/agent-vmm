//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"math/bits"
	"path/filepath"
	"runtime"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const diskSampleInterval = 5 * time.Second

func (r *Runtime) startDiskSampler() {
	ctx, cancel := context.WithCancel(context.Background())
	r.diskSampleCancel, r.diskSampleDone, r.diskSampleWake = cancel, make(chan struct{}), make(chan struct{}, 1)
	go func() {
		defer close(r.diskSampleDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.diskSampleWake:
			}
			sample, err := r.sampleDiskAccounting(ctx)
			r.diskSampleMu.Lock()
			r.diskSample, r.diskSampleErr = sample, err
			r.diskSampleMu.Unlock()
			timer := time.NewTimer(diskSampleInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	r.wakeDiskSampler()
}

func (r *Runtime) wakeDiskSampler() {
	if r.diskSampleWake != nil {
		select {
		case r.diskSampleWake <- struct{}{}:
		default:
		}
	}
}

func (r *Runtime) stopDiskSampler() {
	if r.diskSampleCancel != nil {
		r.diskSampleCancel()
		<-r.diskSampleDone
	}
}

// Dashboard reads never enumerate namespaces or use admission commitments.
func (r *Runtime) observedDiskAccounting() (*vmmdv1.DiskAccounting, error) {
	r.wakeDiskSampler()
	r.diskSampleMu.Lock()
	defer r.diskSampleMu.Unlock()
	if r.diskSample != nil && time.Since(time.Unix(0, r.diskSample.BreakdownSampledAtUnixNano)) <= 3*diskSampleInterval {
		return proto.Clone(r.diskSample).(*vmmdv1.DiskAccounting), nil
	}
	return nil, errors.Join(errors.New("disk sample is unavailable"), r.diskSampleErr)
}

func (r *Runtime) sampleDiskAccounting(ctx context.Context) (*vmmdv1.DiskAccounting, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(stateRoot, &stats); err != nil {
		return nil, err
	}
	holds, _, err := importReservationSummary(filepath.Join(stateRoot, "import-reservations"))
	if err != nil {
		return nil, err
	}
	limit, err := activeBuilderQuotaLimit(filepath.Join(stateRoot, "import-reservations"))
	if err != nil {
		return nil, err
	}
	used := (stats.Blocks - stats.Bfree) * uint64(stats.Bsize)
	if holds > used {
		return nil, errors.New("import holds exceed physical disk usage")
	}
	credit := uint64(0)
	cache, cacheErr := projectUsage(r.stateDevice, cacheProjectID)
	if limit != 0 && cacheErr != nil {
		return nil, cacheErr
	}
	if limit > cache {
		credit = limit - cache
	}
	reservations, carry := bits.Add64(holds, credit, 0)
	if carry != 0 {
		return nil, errors.New("writer reservation accounting overflow")
	}
	result := &vmmdv1.DiskAccounting{
		UsedBytes: used - holds, CapacityBytes: used + stats.Bavail*uint64(stats.Bsize), ImportReservationBytes: reservations,
		SystemReserveBytes: guestdisk.DefaultEmergencyBytes, MetadataReserveBytes: guestdisk.DefaultMetadataBytes,
	}
	defer func() { result.BreakdownSampledAtUnixNano = time.Now().UnixNano() }()
	images, imageErr := projectUsage(r.stateDevice, ociProjectID)
	private, privateErr := privateProjectUsage(ctx, r.stateDevice, guestquota.Next)
	if cacheErr != nil || imageErr != nil || privateErr != nil {
		return result, nil
	}
	// Holds use project zero and are excluded from physical used bytes. Unused
	// builder quota credit is a reservation, never physical category usage.
	if images <= result.UsedBytes && cache <= result.UsedBytes-images && private <= result.UsedBytes-images-cache {
		other := result.UsedBytes - images - cache - private
		result.ImageRuntimeBytes, result.BuildCacheBytes, result.PrivateBytes, result.OtherBytes = &images, &cache, &private, &other
	}
	return result, nil
}

func privateProjectUsage(ctx context.Context, device string, next func(string, uint32) (uint32, guestquota.State, error)) (uint64, error) {
	var total uint64
	cursor := namespaceProjectBase
	for count := 0; ; count++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id, state, err := next(device, cursor)
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return 0, err
		}
		if id < cursor {
			return 0, errors.New("project quota cursor moved backward")
		}
		if id >= applianceProjectBase {
			return total, nil
		}
		value, carry := bits.Add64(total, state.UsageBytes, 0)
		if carry != 0 {
			return 0, errors.New("private project usage overflow")
		}
		total = value
		cursor = id + 1
		if count%maxGCDeletes == maxGCDeletes-1 {
			runtime.Gosched()
		}
	}
}
