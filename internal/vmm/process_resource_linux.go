package vmm

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/vmmprocess"
)

func currentProcessMemoryBytes() (uint64, error) { return residentBytes("self") }

// childProcessResources reports the VMM process, which runs the Guest outside
// the Host process and can outlive it.
func (manager *Manager) childProcessResources() (uint64, uint32, error) {
	manager.mu.Lock()
	client, _ := manager.machine.(*vmmprocess.Client)
	manager.mu.Unlock()
	if client == nil || client.PID() == 0 {
		return 0, 0, nil
	}
	memory, err := residentBytes(strconv.Itoa(client.PID()))
	if err != nil {
		return 0, 1, errors.New("VMM process memory is unavailable")
	}
	return memory, 1, nil
}

func residentBytes(process string) (uint64, error) {
	data, err := os.ReadFile("/proc/" + process + "/statm")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, errors.New("process memory is unavailable")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return pages * uint64(os.Getpagesize()), nil
}
