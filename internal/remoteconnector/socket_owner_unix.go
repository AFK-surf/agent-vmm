//go:build unix

package remoteconnector

import (
	"fmt"
	"os"
	"syscall"
)

func checkSocketOwner(info os.FileInfo, label string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		return fmt.Errorf("%s has unsafe owner", label)
	}
	return nil
}
