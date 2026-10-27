//go:build linux

package control

import (
	"os"

	"golang.org/x/sys/unix"
)

func openBeneath(root *os.File, relative string, flags int) (*os.File, error) {
	if flags&unix.O_PATH == 0 {
		flags |= unix.O_NONBLOCK
	}
	// O_PATH does not open device contents and rejects O_NONBLOCK in openat2.
	fd, err := unix.Openat2(int(root.Fd()), relative, &unix.OpenHow{
		Flags:   uint64(flags | unix.O_CLOEXEC),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), relative), nil
}
