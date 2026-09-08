//go:build linux

package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"golang.org/x/sys/unix"
)

const reservationLabelPrefix = labelPrefix + "reservation-"

func contractFromGuest(value *vmmdv1.WorkloadResourcesV2) resourcev2.Contract {
	return resourcev2.Contract{CPUGuaranteeMillis: uint64(value.GetCpuGuaranteeMillis()), CPUMaxMillis: uint64(value.GetCpuMaxMillis()),
		CPUWeight: uint64(value.GetCpuWeight()), MemoryHighBytes: value.GetMemoryHighBytes(), MemoryMaxBytes: value.GetMemoryMaxBytes(),
		PIDMax: uint64(value.GetPidMax()), WritableQuotaBytes: value.GetWritableQuotaBytes()}
}

func (r *Runtime) ReserveWorkloadResources(ctx context.Context, request *vmmdv1.ReserveWorkloadResourcesRequest) (*vmmdv1.WorkloadResourceReceipt, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	if err := validateID("allocation_id", request.GetAllocationId()); err != nil {
		return nil, err
	}
	if err := validateID("namespace_id", request.GetNamespaceId()); err != nil {
		return nil, err
	}
	contract := contractFromGuest(request.GetResources())
	if request.GetGeneration() == 0 || request.GetEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED || contract.Validate(8000) != nil {
		return nil, errors.New("complete resource-v2 reservation is required")
	}
	digest := contract.Digest(request.GetAllocationId(), request.GetGeneration(), request.GetNamespaceId())
	unlock := r.lockNamespace(request.GetNamespaceId())
	defer unlock()
	releaseIdentity := &vmmdv1.ReleaseWorkloadResourcesRequest{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: digest[:]}
	if terminal, err := loadReleaseReceipt(releaseIdentity); err != nil {
		return nil, err
	} else if terminal != nil {
		return nil, errors.New("exact Guest reservation is already terminal")
	}
	labels, exists, err := r.workloadReservationLabels(ctx, request.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	if exists {
		stored, decodeErr := reservationFromLabels(request.GetNamespaceId(), labels)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if stored.GetAllocationId() != request.GetAllocationId() || stored.GetGeneration() != request.GetGeneration() ||
			!bytes.Equal(stored.GetContractSha256(), digest[:]) {
			return nil, errors.New("reservation identity conflicts with durable Guest state")
		}
		if err := r.enforceWorkloadReservation(ctx, request, labels); err != nil {
			return nil, err
		}
		return stored, nil
	}
	var lastSnapshot guestdisk.Snapshot
	loadSnapshot := func(transaction *guestdisk.Transaction) (guestdisk.Snapshot, error) {
		snapshot, err := r.diskAdmissionSnapshot(ctx, true, transaction)
		if err == nil || errors.Is(err, guestdisk.ErrCapacityUnavailable) {
			lastSnapshot = snapshot
		}
		return snapshot, err
	}
	commit := func(transaction *guestdisk.Transaction) error {
		projectID, subnet, err := r.allocateNamespace(ctx, request.GetNamespaceId())
		if err != nil {
			return err
		}
		labels = workloadReservationLabels(request, projectID, subnet, digest[:])
		if err := r.guardOCIMetadataGrowthLockedWithCommit(ctx, transaction, contract.WritableQuotaBytes, func() error { return r.client.NamespaceService().Create(ctx, request.GetNamespaceId(), labels) }); err != nil {
			return err
		}
		return r.enforceWorkloadReservation(ctx, request, labels)
	}
	err = r.diskBudget.AdmitQuota(loadSnapshot, contract.WritableQuotaBytes, commit)
	if errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		if _, gcErr := r.reclaimOCI(ctx, maxGCDeletes); gcErr != nil {
			return nil, gcErr
		}
		err = r.diskBudget.AdmitQuota(loadSnapshot, contract.WritableQuotaBytes, commit)
	}
	if errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		available, _ := lastSnapshot.AvailableBytes()
		return nil, resourceCapacityExhaustedWithBytes(resourcev2.CapacityStorageHeadroom, "workload_reservation", "Guest storage headroom is unavailable", available, contract.WritableQuotaBytes)
	}
	if err != nil {
		return nil, err
	}
	return reservationFromLabels(request.GetNamespaceId(), labels)
}

func (r *Runtime) enforceWorkloadReservation(ctx context.Context, request *vmmdv1.ReserveWorkloadResourcesRequest, labels map[string]string) error {
	projectID64, err := strconv.ParseUint(labels[labelPrefix+"project-id"], 10, 32)
	if err != nil {
		return err
	}
	projectID := uint32(projectID64)
	for _, directory := range []string{volumePath(request.GetNamespaceId(), ""), workspacePath(request.GetNamespaceId()),
		stateRoot + "/logs/" + request.GetNamespaceId(), stateRoot + "/build-cache/" + request.GetNamespaceId(), stateRoot + "/build-tmp/" + request.GetNamespaceId()} {
		if err := ensureDirectory(directory, 0o700); err != nil {
			return err
		}
		if err := assignProject(directory, projectID); err != nil {
			return err
		}
	}
	if err := setProjectQuota(r.stateDevice, projectID, request.GetResources().GetWritableQuotaBytes()); err != nil {
		return err
	}
	if err := applyWorkloadCgroup(request.GetNamespaceId(), request.GetResources()); err != nil {
		return err
	}
	return r.network.EnsureNamespace(request.GetNamespaceId(), labels[labelPrefix+"subnet"], request.GetEgressMode())
}

func (r *Runtime) ReleaseWorkloadResources(ctx context.Context, request *vmmdv1.ReleaseWorkloadResourcesRequest) (*vmmdv1.WorkloadResourceReceipt, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	if err := validateID("allocation_id", request.GetAllocationId()); err != nil {
		return nil, err
	}
	if err := validateID("namespace_id", request.GetNamespaceId()); err != nil {
		return nil, err
	}
	if request.GetGeneration() == 0 || len(request.GetContractSha256()) != 32 {
		return nil, errors.New("complete exact release identity is required")
	}
	unlock := r.lockNamespace(request.GetNamespaceId())
	defer unlock()
	stored, err := loadReleaseReceipt(request)
	if err != nil {
		return nil, err
	}
	labels, exists, err := r.workloadReservationLabels(ctx, request.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	if !exists {
		if stored == nil {
			return nil, errors.New("exact Guest reservation does not exist")
		}
		if stored.State != "released" {
			stored.State = "released"
			if err := persistReleaseReceipt(request, stored); err != nil {
				return nil, err
			}
		}
		return requestReceipt(request), nil
	}
	receipt, err := reservationFromLabels(request.GetNamespaceId(), labels)
	if err != nil {
		return nil, err
	}
	if receipt.GetAllocationId() != request.GetAllocationId() || receipt.GetGeneration() != request.GetGeneration() || !bytes.Equal(receipt.GetContractSha256(), request.GetContractSha256()) {
		return nil, errors.New("release does not match exact Guest reservation")
	}
	if stored == nil {
		stored = &durableReleaseReceipt{AllocationID: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceID: request.GetNamespaceId(), ContractSHA256: hex.EncodeToString(request.GetContractSha256()), State: "releasing"}
		if err := persistReleaseReceipt(request, stored); err != nil {
			return nil, err
		}
	}
	if err := r.deleteNamespaceLocked(ctx, request.GetNamespaceId()); err != nil {
		return nil, err
	}
	stored.State = "released"
	if err := persistReleaseReceipt(request, stored); err != nil {
		return nil, err
	}
	return receipt, nil
}

func (r *Runtime) ConfirmWorkloadResourceRelease(request *vmmdv1.ConfirmWorkloadResourceReleaseRequest) (*vmmdv1.WorkloadResourceReceipt, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	if err := validateID("allocation_id", request.GetAllocationId()); err != nil {
		return nil, err
	}
	if err := validateID("namespace_id", request.GetNamespaceId()); err != nil {
		return nil, err
	}
	if request.GetGeneration() == 0 || len(request.GetContractSha256()) != 32 {
		return nil, errors.New("complete exact release confirmation identity is required")
	}
	release := &vmmdv1.ReleaseWorkloadResourcesRequest{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: request.GetContractSha256()}
	unlock := r.lockNamespace(request.GetNamespaceId())
	defer unlock()
	stored, err := loadReleaseReceipt(release)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return requestReceipt(release), nil
	}
	if stored.State != "released" {
		return nil, errors.New("Guest release has not reached its durable tombstone")
	}
	if err := removeReleaseReceipt(release); err != nil {
		return nil, err
	}
	return requestReceipt(release), nil
}

func removeReleaseReceipt(request *vmmdv1.ReleaseWorkloadResourcesRequest) error {
	return removeReleaseReceiptAt(stateRoot, request)
}

func removeReleaseReceiptAt(root string, request *vmmdv1.ReleaseWorkloadResourcesRequest) error {
	path := releaseReceiptPathAt(root, request)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

type durableReleaseReceipt struct {
	AllocationID   string `json:"allocation_id"`
	Generation     uint64 `json:"generation"`
	NamespaceID    string `json:"namespace_id"`
	ContractSHA256 string `json:"contract_sha256"`
	State          string `json:"state"`
	UpdatedAt      int64  `json:"updated_at"`
}

func requestReceipt(request *vmmdv1.ReleaseWorkloadResourcesRequest) *vmmdv1.WorkloadResourceReceipt {
	return &vmmdv1.WorkloadResourceReceipt{AllocationId: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceId: request.GetNamespaceId(), ContractSha256: append([]byte(nil), request.GetContractSha256()...)}
}

func releaseReceiptPath(request *vmmdv1.ReleaseWorkloadResourcesRequest) string {
	return releaseReceiptPathAt(stateRoot, request)
}

func releaseReceiptPathAt(root string, request *vmmdv1.ReleaseWorkloadResourcesRequest) string {
	identity := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", request.GetAllocationId(), request.GetGeneration(), request.GetNamespaceId())))
	return filepath.Join(root, "reservation-receipts", hex.EncodeToString(identity[:])+".json")
}

func loadReleaseReceipt(request *vmmdv1.ReleaseWorkloadResourcesRequest) (*durableReleaseReceipt, error) {
	data, err := os.ReadFile(releaseReceiptPath(request))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var value durableReleaseReceipt
	if json.Unmarshal(data, &value) != nil || value.AllocationID != request.GetAllocationId() || value.Generation != request.GetGeneration() || value.NamespaceID != request.GetNamespaceId() || value.ContractSHA256 != hex.EncodeToString(request.GetContractSha256()) || (value.State != "releasing" && value.State != "released") {
		return nil, errors.New("durable release receipt conflicts with exact identity")
	}
	return &value, nil
}

func persistReleaseReceipt(request *vmmdv1.ReleaseWorkloadResourcesRequest, value *durableReleaseReceipt) error {
	return persistReleaseReceiptAt(stateRoot, request, value)
}

func persistReleaseReceiptAt(root string, request *vmmdv1.ReleaseWorkloadResourcesRequest, value *durableReleaseReceipt) error {
	path := releaseReceiptPathAt(root, request)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	value.UpdatedAt = time.Now().UnixNano()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".receipt-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (r *Runtime) workloadReservationLabels(ctx context.Context, namespaceID string) (map[string]string, bool, error) {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, name := range names {
		if name == namespaceID {
			labels, err := r.client.NamespaceService().Labels(ctx, name)
			return labels, true, err
		}
	}
	return nil, false, nil
}

func workloadReservationLabels(request *vmmdv1.ReserveWorkloadResourcesRequest, projectID uint32, subnet string, digest []byte) map[string]string {
	resources := request.GetResources()
	return map[string]string{
		labelPrefix + "project-id": strconv.FormatUint(uint64(projectID), 10), labelPrefix + "subnet": subnet,
		labelPrefix + "egress":                strconv.FormatInt(int64(request.GetEgressMode()), 10),
		reservationLabelPrefix + "allocation": request.GetAllocationId(), reservationLabelPrefix + "generation": strconv.FormatUint(request.GetGeneration(), 10),
		reservationLabelPrefix + "digest": fmt.Sprintf("%x", digest), reservationLabelPrefix + "cpu-guarantee": strconv.FormatUint(uint64(resources.GetCpuGuaranteeMillis()), 10),
		reservationLabelPrefix + "cpu-max": strconv.FormatUint(uint64(resources.GetCpuMaxMillis()), 10), reservationLabelPrefix + "cpu-weight": strconv.FormatUint(uint64(resources.GetCpuWeight()), 10),
		reservationLabelPrefix + "memory-high": strconv.FormatUint(resources.GetMemoryHighBytes(), 10), reservationLabelPrefix + "memory-max": strconv.FormatUint(resources.GetMemoryMaxBytes(), 10),
		reservationLabelPrefix + "pid-max": strconv.FormatUint(uint64(resources.GetPidMax()), 10), reservationLabelPrefix + "writable-quota": strconv.FormatUint(resources.GetWritableQuotaBytes(), 10),
	}
}

func reservationFromLabels(namespaceID string, labels map[string]string) (*vmmdv1.WorkloadResourceReceipt, error) {
	parse := func(name string, bits int) (uint64, error) {
		value, err := strconv.ParseUint(labels[reservationLabelPrefix+name], 10, bits)
		if err != nil {
			return 0, fmt.Errorf("Guest reservation %s is invalid", name)
		}
		return value, nil
	}
	generation, err := strconv.ParseUint(labels[reservationLabelPrefix+"generation"], 10, 64)
	if err != nil {
		return nil, errors.New("Guest reservation generation is invalid")
	}
	digest, err := hex.DecodeString(labels[reservationLabelPrefix+"digest"])
	if err != nil || len(digest) != 32 {
		return nil, errors.New("Guest reservation digest is invalid")
	}
	cpuGuarantee, err := parse("cpu-guarantee", 32)
	if err != nil {
		return nil, err
	}
	cpuMax, err := parse("cpu-max", 32)
	if err != nil {
		return nil, err
	}
	cpuWeight, err := parse("cpu-weight", 32)
	if err != nil {
		return nil, err
	}
	memoryHigh, err := parse("memory-high", 64)
	if err != nil {
		return nil, err
	}
	memoryMax, err := parse("memory-max", 64)
	if err != nil {
		return nil, err
	}
	pidMax, err := parse("pid-max", 32)
	if err != nil {
		return nil, err
	}
	writableQuota, err := parse("writable-quota", 64)
	if err != nil {
		return nil, err
	}
	allocationID := labels[reservationLabelPrefix+"allocation"]
	contract := resourcev2.Contract{CPUGuaranteeMillis: cpuGuarantee, CPUMaxMillis: cpuMax, CPUWeight: cpuWeight,
		MemoryHighBytes: memoryHigh, MemoryMaxBytes: memoryMax, PIDMax: pidMax, WritableQuotaBytes: writableQuota}
	expected := contract.Digest(allocationID, generation, namespaceID)
	if !bytes.Equal(digest, expected[:]) {
		return nil, errors.New("Guest reservation contract digest is invalid")
	}
	return &vmmdv1.WorkloadResourceReceipt{AllocationId: allocationID, Generation: generation, NamespaceId: namespaceID, ContractSha256: digest}, nil
}

func (r *Runtime) guestDiskSnapshot(ctx context.Context) (guestdisk.Snapshot, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(stateRoot, &stats); err != nil {
		return guestdisk.Snapshot{}, err
	}
	used := (stats.Blocks - stats.Bfree) * uint64(stats.Bsize)
	capacity := used + stats.Bavail*uint64(stats.Bsize)
	var headroom uint64
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	if len(names) > maxGCNamespaces {
		return guestdisk.Snapshot{}, errors.New("Guest disk accounting namespace bound exceeded")
	}
	for _, name := range names {
		if name == runtimeContainerdNamespace {
			continue
		}
		labels, err := r.client.NamespaceService().Labels(ctx, name)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
		quota, project, parseErr := accountableNamespaceQuota(name, labels)
		if parseErr != nil {
			return guestdisk.Snapshot{}, parseErr
		}
		usage, err := projectUsage(r.stateDevice, project)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
		if quota > usage {
			var carry uint64
			headroom, carry = bits.Add64(headroom, quota-usage, 0)
			if carry != 0 {
				return guestdisk.Snapshot{}, errors.New("Guest writable quota accounting overflow")
			}
		}
	}
	imports, _, err := importReservationSummary(filepath.Join(stateRoot, "import-reservations"))
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	if imports > used {
		return guestdisk.Snapshot{}, errors.New("Guest import reservation exceeds physical usage")
	}
	return guestdisk.Snapshot{CapacityBytes: capacity, UsedBytes: used - imports, WritableHeadroomBytes: headroom, ImportReservationBytes: imports,
		EmergencyBytes: guestdisk.DefaultEmergencyBytes, MetadataGrowthBytes: guestdisk.DefaultMetadataBytes, AllocationUnitBytes: uint64(stats.Bsize)}, nil
}

func accountableNamespaceQuota(name string, labels map[string]string) (uint64, uint32, error) {
	quotaText := labels[reservationLabelPrefix+"writable-quota"]
	if quotaText == "" {
		quotaText = labels[labelPrefix+"disk"]
	}
	quota, err := strconv.ParseUint(quotaText, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("namespace %q has no accountable writable quota: %w", name, err)
	}
	project, err := strconv.ParseUint(labels[labelPrefix+"project-id"], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("namespace %q has no accountable project: %w", name, err)
	}
	return quota, uint32(project), nil
}

func (r *Runtime) diskAdmissionSnapshot(ctx context.Context, cold bool, transaction *guestdisk.Transaction) (guestdisk.Snapshot, error) {
	snapshot, err := r.guestDiskSnapshot(ctx)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	if snapshot.Pressure() >= guestdisk.PressureGC {
		if _, err := r.reclaimOCIWithMaterializerTransaction(ctx, maxGCDeletes, false, transaction); err != nil {
			return guestdisk.Snapshot{}, err
		}
		snapshot, err = r.guestDiskSnapshot(ctx)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
	}
	if snapshot.Pressure() == guestdisk.PressureCritical || cold && snapshot.Pressure() >= guestdisk.PressureRejectCold {
		return snapshot, guestdisk.ErrCapacityUnavailable
	}
	return snapshot, nil
}

func importReservationSummary(directory string) (uint64, uint32, error) {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	var total uint64
	var count uint32
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".hold" {
			continue
		}
		if count == ^uint32(0) {
			return 0, 0, errors.New("Guest import reservation count overflow")
		}
		count++
		info, err := entry.Info()
		if err != nil {
			return 0, 0, err
		}
		value := uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
		var carry uint64
		total, carry = bits.Add64(total, value, 0)
		if carry != 0 {
			return 0, 0, errors.New("Guest import reservation accounting overflow")
		}
	}
	return total, count, nil
}
