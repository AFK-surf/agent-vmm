//go:build linux

package control

import (
	"syscall"
)

func (r *Runtime) logSystemImageStorage(stage string) {
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(stateRoot, &filesystem); err != nil {
		r.logger.Warn("system image storage observation failed", "stage", stage, "error", err)
		return
	}
	used, err := projectUsage(r.stateDevice, ociProjectID)
	r.logger.Info("system image storage", "stage", stage, "pool_capacity_bytes", filesystem.Blocks*uint64(filesystem.Bsize), "pool_available_bytes", filesystem.Bavail*uint64(filesystem.Bsize), "image_project_used_bytes", used, "error", err)
}
