//go:build linux

package quota

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProjectQuotaRejectsSymlinkPaths(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(root, "link"), filepath.Join(root, "link", "file")} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if file, err := openProjectInode(path); !errors.Is(err, unix.ELOOP) {
				if file != nil {
					file.Close()
				}
				t.Fatalf("quota path open error=%v, want ELOOP", err)
			}
		})
	}
	for _, path := range []string{real, filepath.Join(real, "file")} {
		file, err := openProjectInode(path)
		if err != nil {
			t.Fatal(err)
		}
		file.Close()
	}
}

func TestProjectQuotaRejectsSpecialInodes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "fifo")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(root, "socket")
	socket, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	for _, path := range []string{path, socketPath, "/dev/null"} {
		if file, err := openProjectInode(path); err == nil {
			file.Close()
			t.Fatalf("quota open accepted special inode %s", path)
		}
	}
}

func TestProjectQuotaOpenPinsInodeDuringPathReplacement(t *testing.T) {
	root := t.TempDir()
	path, replacement := filepath.Join(root, "file"), filepath.Join(root, "replacement")
	if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/null", replacement); err != nil {
		t.Fatal(err)
	}
	var worker sync.WaitGroup
	stop := make(chan struct{})
	worker.Add(1)
	go func() {
		defer worker.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := unix.Renameat2(unix.AT_FDCWD, path, unix.AT_FDCWD, replacement, unix.RENAME_EXCHANGE); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	defer func() { close(stop); worker.Wait() }()
	for i := 0; i < 1000; i++ {
		file, err := openProjectInode(path)
		if errors.Is(err, unix.ELOOP) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		info, err := file.Stat()
		file.Close()
		if err != nil || !info.Mode().IsRegular() || info.Size() != 5 {
			t.Fatalf("opened replacement instead of owned inode: info=%v err=%v", info, err)
		}
	}
}
