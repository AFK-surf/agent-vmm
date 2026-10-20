//go:build linux

package quota

import (
	"fmt"
	"math/bits"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	fsIOCFSGetXAttr    = 0x801c581f
	fsIOCFSSetXAttr    = 0x401c5820
	fsXFlagProjInherit = 0x00000200
	quotaSetQuota      = 0x800008
	quotaGetQuota      = 0x800007
	projectQuota       = 2
	quotaValidLimits   = 1
)

type fsXAttr struct {
	XFlags     uint32
	ExtSize    uint32
	Nextents   uint32
	ProjectID  uint32
	CowExtSize uint32
	Pad        [8]byte
}

type State struct {
	UsageBytes     uint64
	HardLimitBytes uint64
}

func Usage(device string, projectID uint32) (uint64, error) {
	state, err := Read(device, projectID)
	return state.UsageBytes, err
}

func Read(device string, projectID uint32) (State, error) {
	devicePointer, err := unix.BytePtrFromString(device)
	if err != nil {
		return State{}, err
	}
	quota := quotaBlock{}
	command := uintptr(quotaGetQuota<<8 | projectQuota)
	_, _, errno := unix.Syscall6(unix.SYS_QUOTACTL, command, uintptr(unsafe.Pointer(devicePointer)), uintptr(projectID), uintptr(unsafe.Pointer(&quota)), 0, 0)
	if errno != 0 {
		return State{}, fmt.Errorf("read project quota %d: %w", projectID, errno)
	}
	high, limit := bits.Mul64(quota.BlockHardLimit, 1024)
	if high != 0 {
		return State{}, fmt.Errorf("project quota %d limit overflows byte accounting", projectID)
	}
	return State{UsageBytes: quota.CurrentSpace, HardLimitBytes: limit}, nil
}

type quotaBlock struct {
	BlockHardLimit uint64
	BlockSoftLimit uint64
	CurrentSpace   uint64
	InodeHardLimit uint64
	InodeSoftLimit uint64
	CurrentInodes  uint64
	BlockTime      uint64
	InodeTime      uint64
	Valid          uint32
	Padding        uint32
}

func Assign(path string, projectID uint32) error {
	file, err := openProjectInode(path)
	if err != nil {
		return err
	}
	defer file.Close()
	attributes := fsXAttr{}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), fsIOCFSGetXAttr, uintptr(unsafe.Pointer(&attributes))); errno != 0 {
		return fmt.Errorf("get project for %s: %w", path, errno)
	}
	attributes.ProjectID = projectID
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		attributes.XFlags |= fsXFlagProjInherit
	}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), fsIOCFSSetXAttr, uintptr(unsafe.Pointer(&attributes))); errno != 0 {
		return fmt.Errorf("set project for %s: %w", path, errno)
	}
	return nil
}

func ProjectID(path string) (uint32, error) {
	file, err := openProjectInode(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	attributes := fsXAttr{}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), fsIOCFSGetXAttr, uintptr(unsafe.Pointer(&attributes))); errno != 0 {
		return 0, fmt.Errorf("get project for %s: %w", path, errno)
	}
	return attributes.ProjectID, nil
}

func openProjectInode(path string) (*os.File, error) {
	fd, err := unix.Openat2(unix.AT_FDCWD, path, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, err
	}
	if kind := stat.Mode & unix.S_IFMT; kind != unix.S_IFDIR && kind != unix.S_IFREG {
		return nil, fmt.Errorf("project quota requires a directory or regular file: %s", path)
	}
	// Reopen the pinned inode, not the mutable pathname. O_PATH never opens
	// a device or waits for a FIFO writer before the type check.
	return os.Open(fmt.Sprintf("/proc/self/fd/%d", fd))
}

func SetLimit(device string, projectID uint32, bytes uint64) error {
	devicePointer, err := unix.BytePtrFromString(device)
	if err != nil {
		return err
	}
	blocks := bytes / 1024
	if bytes%1024 != 0 {
		blocks++
	}
	quota := quotaBlock{BlockHardLimit: blocks, BlockSoftLimit: blocks, Valid: quotaValidLimits}
	command := uintptr(quotaSetQuota<<8 | projectQuota)
	_, _, errno := unix.Syscall6(unix.SYS_QUOTACTL, command, uintptr(unsafe.Pointer(devicePointer)), uintptr(projectID), uintptr(unsafe.Pointer(&quota)), 0, 0)
	if errno != 0 {
		return fmt.Errorf("set project quota %d: %w", projectID, errno)
	}
	return nil
}
