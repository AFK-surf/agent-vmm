//go:build linux && !cgo

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"golang.org/x/sys/unix"
)

func TestQuotaBypassDropsOnEveryThreadAndExec(t *testing.T) {
	root := os.Getenv("AGENT_VMM_QUOTA_TEST_ROOT")
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if root == "" || device == "" {
		t.Skip("requires a disposable ext4 project-quota mount")
	}
	if os.Getenv("AGENT_VMM_QUOTA_CAPABILITY_HELPER") == "1" {
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 1048576, Max: 1048576}); err != nil {
			t.Fatal(err)
		}
		ready, inspect := make(chan struct{}, 16), make(chan struct{})
		results := make(chan error, 16)
		for range 16 {
			go func() {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
				ready <- struct{}{}
				<-inspect
				data, err := os.ReadFile(fmt.Sprintf("/proc/self/task/%d/status", unix.Gettid()))
				if err == nil {
					err = quotaCapabilityAbsent(string(data))
				}
				results <- err
			}()
		}
		for range 16 {
			<-ready
		}
		if err := dropQuotaBypass(); err != nil {
			t.Fatal(err)
		}
		close(inspect)
		for range 16 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		var limit unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil || limit.Cur != 1048576 {
			t.Fatalf("NOFILE after capability removal: %+v, %v", limit, err)
		}
		output, err := exec.Command("cat", "/proc/self/status").CombinedOutput()
		if err != nil {
			t.Fatalf("inspect child: %v: %s", err, output)
		}
		if err := quotaCapabilityAbsent(string(output)); err != nil {
			t.Fatal(err)
		}
		file, err := os.Create(filepath.Join(root, "payload"))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := unix.Fallocate(int(file.Fd()), 0, 0, 16<<20); !errors.Is(err, unix.EDQUOT) {
			t.Fatalf("project quota bypass after capability removal: %v", err)
		}
		return
	}
	root, err := os.MkdirTemp(root, "capability-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	const project = 19999
	if err := guestquota.Assign(root, project); err != nil {
		t.Fatal(err)
	}
	if err := guestquota.SetLimit(device, project, 8<<20); err != nil {
		t.Fatal(err)
	}
	defer guestquota.SetLimit(device, project, 0)
	command := exec.Command(os.Args[0], "-test.run=^TestQuotaBypassDropsOnEveryThreadAndExec$", "-test.v")
	command.Env = append(os.Environ(), "AGENT_VMM_QUOTA_CAPABILITY_HELPER=1", "AGENT_VMM_QUOTA_TEST_ROOT="+root)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("capability helper: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}

func quotaCapabilityAbsent(status string) error {
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "CapEff:", "CapPrm:", "CapInh:", "CapBnd:", "CapAmb:":
			value, err := strconv.ParseUint(fields[1], 16, 64)
			if err != nil || value&(1<<unix.CAP_SYS_RESOURCE) != 0 {
				return fmt.Errorf("quota bypass capability remains: %s", line)
			}
		}
	}
	return nil
}
