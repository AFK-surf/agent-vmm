//go:build darwin || linux

package host

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"golang.org/x/sys/unix"
)

func openHostShareSource(path string) (*os.File, store.HostShareSource, error) {
	if !filepath.IsAbs(path) {
		return nil, store.HostShareSource{}, fmt.Errorf("source must be an absolute directory path")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, store.HostShareSource{}, err
	}
	fd, err := unix.Open(canonical, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, store.HostShareSource{}, err
	}
	file := os.NewFile(uintptr(fd), canonical)
	identity, err := identifyHostShareSource(file)
	if err != nil {
		file.Close()
		return nil, store.HostShareSource{}, err
	}
	return file, identity, nil
}

func reopenHostShareSource(expected store.HostShareSource) (*os.File, error) {
	file, observed, err := openHostShareSource(expected.Path)
	if err != nil {
		return nil, err
	}
	if observed != expected {
		file.Close()
		return nil, fmt.Errorf("source directory identity changed; explicitly rebind the share to approve another directory")
	}
	return file, nil
}
