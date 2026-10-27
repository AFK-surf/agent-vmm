//go:build darwin || linux

package atomicfile

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// ReplaceFileAt publishes through an open directory and sets ownership before publication.
func ReplaceFileAt(directory *os.File, name string, data []byte, perm fs.FileMode, uid, gid int) error {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return errors.New("invalid publication filename")
	}
	var temporary *os.File
	var temporaryName string
	for attempt := 0; attempt < 16; attempt++ {
		var suffix [8]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			return err
		}
		temporaryName = ".agent-vmm-" + hex.EncodeToString(suffix[:])
		fd, err := unix.Openat(int(directory.Fd()), temporaryName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(perm.Perm()))
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return err
		}
		temporary = os.NewFile(uintptr(fd), temporaryName)
		break
	}
	if temporary == nil {
		return errors.New("create publication file: name space exhausted")
	}
	defer unix.Unlinkat(int(directory.Fd()), temporaryName, 0)
	if err := temporary.Chmod(perm); err != nil {
		temporary.Close()
		return err
	}
	if uid >= 0 || gid >= 0 {
		if err := temporary.Chown(uid, gid); err != nil {
			temporary.Close()
			return err
		}
	}
	count, err := temporary.Write(data)
	if err == nil && count != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = temporary.Sync()
	}
	if err = errors.Join(err, temporary.Close()); err != nil {
		return err
	}
	if err := unix.Renameat(int(directory.Fd()), temporaryName, int(directory.Fd()), name); err != nil {
		return err
	}
	return directory.Sync()
}
