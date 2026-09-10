//go:build linux

package bootstrap

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"golang.org/x/sys/unix"
)

const (
	legacyMigrationMount  = "/run/agent-vmm-migration/legacy"
	controlMigrationMount = "/run/agent-vmm-migration/control"
	poolMigrationMount    = "/run/agent-vmm-migration/workload"
	maxMigrationEntries   = 2_000_000
	maxMigrationFileBytes = int64(1 << 40)
)

type MigrationReport struct {
	Entries      uint64
	LogicalBytes uint64
}

type migrationTarget struct {
	root            string
	device          string
	preserveProject bool
}

type migrationCopier struct {
	report    MigrationReport
	hardlinks map[migrationInode]string
}

type migrationInode struct {
	device uint64
	inode  uint64
	target string
}

func MigrateLegacyState(legacyDevice, targetDevice string) (report MigrationReport, result error) {
	if legacyDevice == "" || targetDevice == "" || legacyDevice == targetDevice {
		return report, errors.New("legacy and target state devices must be distinct")
	}
	root := filepath.Dir(legacyMigrationMount)
	for _, path := range []string{root, legacyMigrationMount, controlMigrationMount, poolMigrationMount} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return report, err
		}
	}
	if err := unix.Mount(legacyDevice, legacyMigrationMount, "ext4", unix.MS_RDONLY|unix.MS_NOATIME, "noload,prjquota"); err != nil {
		return report, fmt.Errorf("mount legacy state read-only: %w", err)
	}
	defer func() { result = errors.Join(result, unix.Unmount(legacyMigrationMount, 0)) }()
	if err := initializeStateDisk(targetDevice); err != nil {
		return report, err
	}
	controlDevice, poolDevice := partitionDevice(targetDevice, 1), partitionDevice(targetDevice, 2)
	for _, filesystem := range []struct{ device, mount string }{{controlDevice, controlMigrationMount}, {poolDevice, poolMigrationMount}} {
		if err := unix.Mount(filesystem.device, filesystem.mount, "ext4", unix.MS_NOATIME, "prjquota"); err != nil {
			return report, fmt.Errorf("mount migration target %s: %w", filesystem.mount, err)
		}
		defer func(target string) { result = errors.Join(result, unix.Unmount(target, 0)) }(filesystem.mount)
	}
	copier := &migrationCopier{hardlinks: make(map[migrationInode]string)}
	entries, err := os.ReadDir(legacyMigrationMount)
	if err != nil {
		return report, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if migrationDiscardedRoot(name) {
			continue
		}
		if name == "containerd" {
			if err := copier.copyContainerd(controlDevice, poolDevice); err != nil {
				return report, err
			}
			continue
		}
		if name == "cni" {
			if err := copier.copy(filepath.Join(legacyMigrationMount, name), migrationTarget{root: filepath.Join(controlMigrationMount, name), device: controlDevice}); err != nil {
				return report, err
			}
			continue
		}
		if name == "network" {
			if err := copier.copyNetwork(controlDevice, poolDevice); err != nil {
				return report, err
			}
			continue
		}
		if migrationNamespaceRoot(name) {
			if err := copier.copyNamespaceRoot(name, controlDevice, poolDevice); err != nil {
				return report, err
			}
			continue
		}
		if err := copier.copy(filepath.Join(legacyMigrationMount, name), migrationTarget{root: filepath.Join(poolMigrationMount, name), device: poolDevice, preserveProject: true}); err != nil {
			return report, err
		}
	}
	if err := os.WriteFile(filepath.Join(controlMigrationMount, namespaceconfig.LegacyStateMarker), []byte("v1\n"), 0o600); err != nil {
		return report, fmt.Errorf("write legacy state migration marker: %w", err)
	}
	for _, path := range []string{controlMigrationMount, poolMigrationMount} {
		file, err := os.Open(path)
		if err != nil {
			return report, err
		}
		err = unix.Syncfs(int(file.Fd()))
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return report, errors.Join(err, closeErr)
		}
	}
	return copier.report, nil
}

func migrationDiscardedRoot(name string) bool {
	switch name {
	case "lost+found", "tmp", "imports", "import-reservations", "containerd-bootstrap-reservation", "containerd-bootstrap-reservation.hold":
		return true
	default:
		return false
	}
}

func migrationNamespaceRoot(name string) bool {
	switch name {
	case "volumes", "workspaces", "logs":
		return true
	default:
		return false
	}
}

func (copier *migrationCopier) copyNetwork(controlDevice, poolDevice string) error {
	source := filepath.Join(legacyMigrationMount, "network")
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != "resolv" {
			if err := copier.copy(filepath.Join(source, entry.Name()), migrationTarget{root: filepath.Join(poolMigrationMount, "network", entry.Name()), device: poolDevice, preserveProject: true}); err != nil {
				return err
			}
			continue
		}
		resolvers, err := os.ReadDir(filepath.Join(source, "resolv"))
		if err != nil {
			return err
		}
		for _, resolver := range resolvers {
			target := migrationTarget{root: filepath.Join(poolMigrationMount, "network", "resolv", resolver.Name()), device: poolDevice, preserveProject: true}
			if strings.HasPrefix(resolver.Name(), "system.") {
				target = migrationTarget{root: filepath.Join(controlMigrationMount, "network", "resolv", resolver.Name()), device: controlDevice, preserveProject: true}
			}
			if err := copier.copy(filepath.Join(source, "resolv", resolver.Name()), target); err != nil {
				return err
			}
		}
	}
	return nil
}

func (copier *migrationCopier) copyContainerd(controlDevice, poolDevice string) error {
	source := filepath.Join(legacyMigrationMount, "containerd")
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		target := migrationTarget{root: filepath.Join(controlMigrationMount, "containerd", name), device: controlDevice}
		switch name {
		case "io.containerd.content.v1.content":
			target = migrationTarget{root: filepath.Join(poolMigrationMount, "containerd", "content"), preserveProject: true}
		case "io.containerd.snapshotter.v1.overlayfs":
			target = migrationTarget{root: filepath.Join(poolMigrationMount, "containerd", "overlayfs"), preserveProject: true}
		case "io.containerd.snapshotter.v1.native":
			target = migrationTarget{root: filepath.Join(controlMigrationMount, "containerd", "native"), preserveProject: true}
		}
		if strings.HasPrefix(target.root, poolMigrationMount) {
			target.device = poolDevice
		}
		if err := copier.copy(filepath.Join(source, name), target); err != nil {
			return err
		}
	}
	return nil
}

func (copier *migrationCopier) copyNamespaceRoot(name, controlDevice, poolDevice string) error {
	source := filepath.Join(legacyMigrationMount, name)
	entries, err := os.ReadDir(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		target := migrationTarget{root: filepath.Join(poolMigrationMount, name, entry.Name()), device: poolDevice, preserveProject: true}
		if strings.HasPrefix(entry.Name(), "system.") {
			target = migrationTarget{root: filepath.Join(controlMigrationMount, name, entry.Name()), device: controlDevice, preserveProject: true}
		}
		if err := copier.copy(filepath.Join(source, entry.Name()), target); err != nil {
			return err
		}
	}
	return nil
}

func (copier *migrationCopier) copy(source string, target migrationTarget) error {
	if copier.report.Entries >= maxMigrationEntries {
		return errors.New("legacy state exceeds migration entry bound")
	}
	if err := os.MkdirAll(filepath.Dir(target.root), 0o700); err != nil {
		return err
	}
	copier.report.Entries++
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("legacy state inode metadata is unavailable")
	}
	mode := info.Mode()
	switch {
	case mode.IsDir():
		if err := os.Mkdir(target.root, 0o700); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			child := target
			child.root = filepath.Join(target.root, entry.Name())
			if err := copier.copy(filepath.Join(source, entry.Name()), child); err != nil {
				return err
			}
		}
	case mode.IsRegular():
		if info.Size() < 0 || info.Size() > maxMigrationFileBytes || uint64(info.Size()) > ^uint64(0)-copier.report.LogicalBytes {
			return fmt.Errorf("legacy regular file %s exceeds migration bound", source)
		}
		copier.report.LogicalBytes += uint64(info.Size())
		key := migrationInode{device: uint64(stat.Dev), inode: stat.Ino, target: target.device}
		if existing := copier.hardlinks[key]; existing != "" {
			if err := os.Link(existing, target.root); err != nil {
				return err
			}
			return nil
		}
		if err := copier.copyRegular(source, target.root, info.Size()); err != nil {
			return err
		}
		copier.hardlinks[key] = target.root
	case mode&os.ModeSymlink != 0:
		value, err := os.Readlink(source)
		if err != nil {
			return err
		}
		if err := os.Symlink(value, target.root); err != nil {
			return err
		}
	case mode&os.ModeNamedPipe != 0:
		if err := unix.Mkfifo(target.root, uint32(stat.Mode&0o7777)); err != nil {
			return err
		}
	case mode&os.ModeSocket != 0:
		return fmt.Errorf("legacy state contains live socket %s", source)
	case mode&os.ModeDevice != 0:
		if err := unix.Mknod(target.root, stat.Mode, int(stat.Rdev)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("legacy state contains unsupported inode %s", source)
	}
	if err := preserveMigrationMetadata(source, target.root, info, stat); err != nil {
		return err
	}
	if target.preserveProject && (mode.IsDir() || mode.IsRegular()) {
		projectID, err := guestquota.ProjectID(source)
		if err != nil {
			return err
		}
		if projectID != 0 {
			if err := guestquota.Assign(target.root, projectID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (copier *migrationCopier) copyRegular(source, target string, size int64) error {
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	targetFile, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		_ = targetFile.Close()
		if !complete {
			_ = os.Remove(target)
		}
	}()
	if err := targetFile.Truncate(size); err != nil {
		return err
	}
	offset := int64(0)
	for offset < size {
		data, err := unix.Seek(int(sourceFile.Fd()), offset, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if err != nil {
			return err
		}
		hole, err := unix.Seek(int(sourceFile.Fd()), data, unix.SEEK_HOLE)
		if err != nil {
			return err
		}
		if _, err := sourceFile.Seek(data, io.SeekStart); err != nil {
			return err
		}
		if _, err := targetFile.Seek(data, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(targetFile, sourceFile, hole-data); err != nil {
			return err
		}
		offset = hole
	}
	if err := targetFile.Sync(); err != nil {
		return err
	}
	if err := targetFile.Close(); err != nil {
		return err
	}
	sourceDigest, err := migrationFileDigest(source)
	if err != nil {
		return err
	}
	targetDigest, err := migrationFileDigest(target)
	if err != nil {
		return err
	}
	if sourceDigest != targetDigest {
		return fmt.Errorf("copied file verification failed for %s", source)
	}
	complete = true
	return nil
}

func migrationFileDigest(path string) ([32]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [32]byte{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return [32]byte{}, err
	}
	hash := sha256.New()
	var encoded [16]byte
	binary.LittleEndian.PutUint64(encoded[:8], uint64(info.Size()))
	hash.Write(encoded[:8])
	for offset := int64(0); offset < info.Size(); {
		data, err := unix.Seek(int(file.Fd()), offset, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if err != nil {
			return [32]byte{}, err
		}
		hole, err := unix.Seek(int(file.Fd()), data, unix.SEEK_HOLE)
		if err != nil {
			return [32]byte{}, err
		}
		binary.LittleEndian.PutUint64(encoded[:8], uint64(data))
		binary.LittleEndian.PutUint64(encoded[8:], uint64(hole-data))
		hash.Write(encoded[:])
		if _, err := file.Seek(data, io.SeekStart); err != nil {
			return [32]byte{}, err
		}
		if _, err := io.CopyN(hash, file, hole-data); err != nil {
			return [32]byte{}, err
		}
		offset = hole
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func preserveMigrationMetadata(source, target string, info os.FileInfo, stat *syscall.Stat_t) error {
	if err := unix.Fchownat(unix.AT_FDCWD, target, int(stat.Uid), int(stat.Gid), unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		if err := os.Chmod(target, info.Mode().Perm()|info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky)); err != nil {
			return err
		}
	}
	if err := copyMigrationXattrs(source, target); err != nil {
		return err
	}
	times := []unix.Timespec{unix.NsecToTimespec(stat.Atim.Nano()), unix.NsecToTimespec(stat.Mtim.Nano())}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, target, times, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	return nil
}

func copyMigrationXattrs(source, target string) error {
	size, err := unix.Llistxattr(source, nil)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	names := make([]byte, size)
	size, err = unix.Llistxattr(source, names)
	if err != nil {
		return err
	}
	values := strings.Split(strings.TrimRight(string(names[:size]), "\x00"), "\x00")
	sort.Strings(values)
	for _, name := range values {
		valueSize, err := unix.Lgetxattr(source, name, nil)
		if err != nil {
			return err
		}
		value := make([]byte, valueSize)
		valueSize, err = unix.Lgetxattr(source, name, value)
		if err != nil {
			return err
		}
		if err := unix.Lsetxattr(target, name, value[:valueSize], 0); err != nil {
			return err
		}
	}
	return nil
}

func migrationRequested(parameters map[string]string) bool {
	return parameters["agent.state_migrate"] == "1"
}
