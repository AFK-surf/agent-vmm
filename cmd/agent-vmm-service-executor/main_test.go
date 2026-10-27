//go:build darwin

package main

import (
	"context"
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/launchdservice"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActionsRejectArguments(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		action string
		args   []string
	}{{"start", []string{"--job", "host"}}, {"shell", nil}, {"install", []string{"--path", "/tmp/x"}}} {
		if err := parseAction(test.action, test.args); err == nil {
			t.Fatalf("accepted %+v", test)
		}
	}
	if err := parseAction("start", nil); err != nil {
		t.Fatal(err)
	}
}

func TestHostDefinitionUsesConfiguredIdentity(t *testing.T) {
	t.Parallel()
	config := ownerConfig{User: "service-user", Group: "staff", UID: 501, GID: 20, Home: "/Users/service-user"}
	job := resolveHostJob(config)
	if job.label != hostLabel || job.program != "/Users/service-user/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/MacOS/agent-vmm-host" {
		t.Fatalf("job=%+v", job)
	}
	definition := string(renderPlist(config))
	for _, expected := range []string{"<string>--disable-personal-mesh-pairing</string>", "<string>service-user</string>", "/Library/Logs/AgentVMM/host.stderr.log"} {
		if !strings.Contains(definition, expected) {
			t.Fatalf("missing %q", expected)
		}
	}
}

func TestServiceProgramRejectsCrossIdentityAndWritableAncestors(t *testing.T) {
	home := t.TempDir()
	program := filepath.Join(home, "bin", "runner")
	if err := os.MkdirAll(filepath.Dir(program), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(program, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	config := ownerConfig{UID: os.Getuid(), Home: home}
	if err := validateServiceProgram(config, program); err != nil {
		t.Fatalf("valid service path: %v", err)
	}
	config.UID++
	if err := validateServiceProgram(config, program); err == nil {
		t.Fatal("cross-identity program was accepted")
	}
	config.UID = os.Getuid()
	if err := os.Chmod(filepath.Dir(program), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := validateServiceProgram(config, program); err == nil {
		t.Fatal("world-writable program ancestor was accepted")
	}
}

func TestStartDoesNotRestartRunningJob(t *testing.T) {
	t.Parallel()
	owner := executor{system: launchdservice.System{
		Launchctl: func(_ context.Context, args ...string) error {
			if args[0] != "print" {
				t.Fatalf("restarted: %v", args)
			}
			return nil
		},
		ServicePID:     func(context.Context, string) (int, error) { return 42, nil },
		ProcessRunning: func(int) bool { return true },
	}}
	if err := owner.startJob(fixedJob{label: hostLabel}); err != nil {
		t.Fatal(err)
	}
}

func TestStartKickstartsLoadedStoppedJob(t *testing.T) {
	t.Parallel()
	var commands []string
	owner := executor{system: launchdservice.System{
		Launchctl: func(_ context.Context, args ...string) error {
			commands = append(commands, strings.Join(args, " "))
			return nil
		},
		ServicePID: func(context.Context, string) (int, error) { return 0, nil },
	}}
	if err := owner.startJob(fixedJob{label: hostLabel}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || commands[1] != "kickstart system/"+hostLabel {
		t.Fatalf("commands=%v", commands)
	}
}

func TestHostStopRequiresReleasedDiskAndConfirmsExit(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		owners []int
		err    error
	}{{name: "released"}, {name: "attached", owners: []int{42}}, {name: "inspection failed", err: errors.New("inspection")}} {
		t.Run(test.name, func(t *testing.T) {
			var commands []string
			checks := 0
			owner := executor{system: launchdservice.System{
				Launchctl: func(_ context.Context, args ...string) error {
					commands = append(commands, strings.Join(args, " "))
					return nil
				},
				ServicePID:          func(context.Context, string) (int, error) { return 42, nil },
				ProcessRunning:      func(int) bool { return false },
				StateDiskOpenOwners: func(context.Context, string) ([]int, error) { checks++; return test.owners, test.err },
			}}
			err := owner.stopJob(ownerConfig{Home: "/Users/service"}, fixedJob{label: hostLabel})
			wantFailure := test.err != nil || len(test.owners) > 0
			if (err != nil) != wantFailure {
				t.Fatalf("err=%v", err)
			}
			if wantFailure && len(commands) != 1 || !wantFailure && (len(commands) != 2 || checks != 2) {
				t.Fatalf("commands=%v checks=%d", commands, checks)
			}
		})
	}
}

func TestInvalidProgramStopsBeforeLaunchctl(t *testing.T) {
	t.Parallel()
	owner := executor{system: launchdservice.System{Launchctl: func(context.Context, ...string) error { t.Fatal("called launchctl"); return nil }}}
	config := ownerConfig{UID: os.Getuid() + 1, Home: t.TempDir()}
	job := fixedJob{program: filepath.Join(config.Home, "missing")}
	if err := owner.installJob(config, job); err == nil {
		t.Fatal("accepted missing program")
	}
	if err := owner.startConfiguredJob(config, job); err == nil {
		t.Fatal("accepted missing program")
	}
}
