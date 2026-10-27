//go:build darwin || linux

package hostshare

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func openHostShareSource(path string) (*os.File, store.HostShareSource, error) {
	if !filepath.IsAbs(path) {
		return nil, store.HostShareSource{}, fmt.Errorf("source must be an absolute directory path")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, store.HostShareSource{}, err
	}
	if runtime.GOOS == "darwin" {
		if err := privatefs.ValidateAncestors(canonical, os.Geteuid()); err != nil {
			return nil, store.HostShareSource{}, status.Error(codes.FailedPrecondition, err.Error())
		}
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
