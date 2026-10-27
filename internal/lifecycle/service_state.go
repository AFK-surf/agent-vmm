//go:build darwin || linux

package lifecycle

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"golang.org/x/sys/unix"
)

type operationFileOwner struct {
	uid    int
	gid    int
	fchown func(int, int, int) error
}

func lifecycleOperationOwner(value paths) *operationFileOwner {
	return lifecycleOperationOwnerFor(value, os.Geteuid(), unix.Fchown)
}

func lifecycleOperationOwnerFor(value paths, effectiveUID int, fchown func(int, int, int) error) *operationFileOwner {
	if value.service.Mode != serviceModeDaemon || effectiveUID != 0 {
		return nil
	}
	return &operationFileOwner{uid: value.service.UID, gid: value.service.GID, fchown: fchown}
}

func openExistingServiceStateDirectory(path string, owner *operationFileOwner) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), path)
	if err := applyOperationFileOwner(directory, owner); err != nil {
		directory.Close()
		return nil, err
	}
	return directory, nil
}

func openExistingStateDirectoryAt(parent *os.File, name string) (*os.File, error) {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return nil, errors.New("state directory name is invalid")
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func openOrCreateStateDirectoryAt(parent *os.File, name string, owner *operationFileOwner) (*os.File, error) {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return nil, errors.New("state directory name is invalid")
	}
	created := false
	if err := unix.Mkdirat(int(parent.Fd()), name, 0o700); err == nil {
		created = true
	} else if !errors.Is(err, unix.EEXIST) {
		return nil, err
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), name)
	if created {
		if err := directory.Chmod(0o700); err != nil {
			directory.Close()
			return nil, err
		}
	}
	if err := applyOperationFileOwner(directory, owner); err != nil {
		directory.Close()
		return nil, err
	}
	if created {
		if err := parent.Sync(); err != nil {
			directory.Close()
			return nil, err
		}
	}
	return directory, nil
}

func openInstallOperationDirectory(value paths, operationID string, owner *operationFileOwner) (*os.File, error) {
	if !operationIDPattern.MatchString(operationID) {
		return nil, errors.New("install operation id is invalid")
	}
	root, err := openApplicationDataDirectory(value, owner)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	operations, err := openOrCreateStateDirectoryAt(root, "install-operations", owner)
	if err != nil {
		return nil, err
	}
	defer operations.Close()
	return openOrCreateStateDirectoryAt(operations, operationID, owner)
}

func openInstallCompletionDirectory(value paths, owner *operationFileOwner) (*os.File, error) {
	root, err := openApplicationDataDirectory(value, owner)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return openOrCreateStateDirectoryAt(root, "install-completions", owner)
}

func openExistingInstallOperationDirectory(value paths, operationID string) (*os.File, error) {
	if !operationIDPattern.MatchString(operationID) {
		return nil, errors.New("install operation id is invalid")
	}
	root, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	operations, err := openExistingStateDirectoryAt(root, "install-operations")
	if err != nil {
		return nil, err
	}
	defer operations.Close()
	return openExistingStateDirectoryAt(operations, operationID)
}

func readInstallOperationState(value paths, operationID, name string, limit int64) ([]byte, error) {
	directory, err := openExistingInstallOperationDirectory(value, operationID)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	return readStateFileAt(directory, name, limit)
}

func readInstallCompletionState(value paths, operationID string, limit int64) ([]byte, error) {
	root, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	directory, err := openExistingStateDirectoryAt(root, "install-completions")
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	return readStateFileAt(directory, operationID+".json", limit)
}

func readApplicationDataState(value paths, name string, limit int64) ([]byte, error) {
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	return readStateFileAt(directory, name, limit)
}

func removeInstallOperationState(value paths, operationID string) error {
	if !operationIDPattern.MatchString(operationID) {
		return errors.New("install operation id is invalid")
	}
	root, err := openExistingApplicationDataDirectory(value)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	operations, err := openExistingStateDirectoryAt(root, "install-operations")
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer operations.Close()
	directory, err := openExistingStateDirectoryAt(operations, operationID)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	names, err := directory.Readdirnames(-1)
	if err != nil {
		directory.Close()
		return err
	}
	for _, name := range names {
		if filepath.Base(name) != name || name == "." || name == ".." {
			directory.Close()
			return errors.New("install operation contains an invalid entry")
		}
		if err := unix.Unlinkat(int(directory.Fd()), name, 0); err != nil {
			directory.Close()
			return fmt.Errorf("remove install operation entry %s: %w", name, err)
		}
	}
	if err := directory.Close(); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(operations.Fd()), operationID, unix.AT_REMOVEDIR); err != nil {
		return err
	}
	return operations.Sync()
}

func openStateFileAt(directory *os.File, name string, flags int, mode os.FileMode, owner *operationFileOwner) (*os.File, error) {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return nil, errors.New("state file name is invalid")
	}
	secureFlags := flags | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd := -1
	var err error
	if flags&unix.O_CREAT != 0 && flags&unix.O_EXCL == 0 {
		fd, err = unix.Openat(int(directory.Fd()), name, secureFlags|unix.O_EXCL, uint32(mode.Perm()))
		if errors.Is(err, unix.EEXIST) {
			fd, err = unix.Openat(int(directory.Fd()), name, secureFlags&^unix.O_CREAT, uint32(mode.Perm()))
		}
	} else {
		fd, err = unix.Openat(int(directory.Fd()), name, secureFlags, uint32(mode.Perm()))
	}
	if err != nil {
		return nil, fmt.Errorf("openat %s: %w", name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if err := requireRegularStateFile(file); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return nil, fmt.Errorf("chmod %s: %w", name, err)
	}
	if err := applyOperationFileOwner(file, owner); err != nil {
		file.Close()
		return nil, fmt.Errorf("fchown %s: %w", name, err)
	}
	return file, nil
}

func requireRegularStateFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("lifecycle state is not a regular file")
	}
	return nil
}

func atomicWriteStateFileAt(directory *os.File, name string, data []byte, mode os.FileMode, owner *operationFileOwner) error {
	uid, gid := -1, -1
	if owner != nil {
		uid, gid = owner.uid, owner.gid
	}
	return atomicfile.ReplaceFileAt(directory, name, data, mode, uid, gid)
}

func handoffExistingStateFileAt(directory *os.File, name string, owner *operationFileOwner) error {
	if owner == nil {
		return nil
	}
	file, err := openStateFileAt(directory, name, unix.O_RDWR, 0o600, owner)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	return file.Close()
}

func readStateFileNoFollow(path string, limit int64) ([]byte, error) {
	directory, err := openExistingServiceStateDirectory(filepath.Dir(path), nil)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	return readStateFileAt(directory, filepath.Base(path), limit)
}

func readStateFileAt(directory *os.File, name string, limit int64) ([]byte, error) {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return nil, errors.New("state file name is invalid")
	}
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("lifecycle state is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("lifecycle state exceeds %d bytes", limit)
	}
	return data, nil
}

func applyOperationFileOwner(file *os.File, owner *operationFileOwner) error {
	if owner == nil {
		return nil
	}
	return owner.fchown(int(file.Fd()), owner.uid, owner.gid)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".agent-vmm-*.plist")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
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
	return os.Rename(temporaryPath, path)
}
