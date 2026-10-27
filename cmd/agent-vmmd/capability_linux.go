//go:build linux

package main

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func dropQuotaBypass() error {
	if _, _, err := syscall.AllThreadsSyscall(syscall.SYS_PRCTL, unix.PR_CAPBSET_DROP, unix.CAP_SYS_RESOURCE, 0); err != 0 {
		return err
	}
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var data [2]unix.CapUserData
	if err := unix.Capget(&header, &data[0]); err != nil {
		return err
	}
	mask := ^uint32(1 << (unix.CAP_SYS_RESOURCE % 32))
	entry := &data[unix.CAP_SYS_RESOURCE/32]
	entry.Effective &= mask
	entry.Permitted &= mask
	entry.Inheritable &= mask
	if _, _, err := syscall.AllThreadsSyscall(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(&header)), uintptr(unsafe.Pointer(&data[0])), 0); err != 0 {
		return err
	}
	if _, _, err := syscall.AllThreadsSyscall(syscall.SYS_PRCTL, unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0); err != 0 {
		return err
	}
	return nil
}
