//go:build darwin && keychain && cgo

package hostapp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

func isGUIHost(ctx context.Context) bool {
	job := fmt.Sprintf("gui/%d/%s", os.Getuid(), bundle.HostBundleIdentifier)
	output, err := exec.CommandContext(ctx, "/bin/launchctl", "print", job).Output()
	if err != nil {
		return false
	}
	for line := range strings.SplitSeq(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "pid" && fields[1] == "=" {
			return fields[2] == strconv.Itoa(os.Getpid())
		}
	}
	return false
}
