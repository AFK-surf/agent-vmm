//go:build linux

package control

import (
	"log"
	"syscall"
)

func (r *Runtime) logSystemImageStorage(stage string) {
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(stateRoot, &filesystem); err != nil {
		log.Printf("system image storage: stage=%s pool_statfs_error=%v", stage, err)
		return
	}
	used, err := projectUsage(r.stateDevice, ociProjectID)
	log.Printf("system image storage: stage=%s pool_capacity_bytes=%d pool_available_bytes=%d image_project_used_bytes=%d project_error=%v", stage,
		filesystem.Blocks*uint64(filesystem.Bsize), filesystem.Bavail*uint64(filesystem.Bsize), used, err)
}
