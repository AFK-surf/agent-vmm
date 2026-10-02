//go:build linux

package main

import "golang.org/x/sys/unix"

func atomicSwapApp(source, target string) error {
	return unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_EXCHANGE)
}

func atomicPublishApp(source, target string) error {
	return unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE)
}
