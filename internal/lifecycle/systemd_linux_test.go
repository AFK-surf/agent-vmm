package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func testPaths(root string) paths {
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 65534, 65534
	}
	value := pathsUnder(root, filepath.Join(root, "source"), serviceConfig{Version: 1, Mode: serviceModeAgent, User: "test", Group: "test",
		UID: uid, GID: gid, ReleaseID: releaseID})
	if err := os.MkdirAll(value.logs, 0o700); err != nil {
		panic(err)
	}
	return value
}

func writeTestRelease(t *testing.T, directory, release string) string {
	t.Helper()
	contents := map[string]string{"agent-vmm": "host " + release, "agent-vmm-data": "worker", "guest/manifest.json": `{"release":"` + release + `"}`}
	manifest := releaseManifest{Version: 1, ReleaseID: release}
	for path, content := range contents {
		if err := os.MkdirAll(filepath.Join(directory, filepath.Dir(path)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, path), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(content))
		manifest.Files = append(manifest.Files, releaseFile{Path: path, Size: int64(len(content)), SHA256: hex.EncodeToString(digest[:]), Executable: !strings.HasPrefix(path, "guest/")})
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, ReleaseManifestName), data, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func TestRenderUnitRunsTheHostAsTheServiceUserAndKeepsTheVMM(t *testing.T) {
	value := pathsUnder("", "/opt/agent-vmm/abc", serviceConfig{User: "agent-vmm", Group: "agent-vmm"})
	unit, err := renderUnit(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"User=agent-vmm", "Group=agent-vmm", "KillMode=process", "Restart=on-failure", "TimeoutStopSec=90",
		"ExecStart=/opt/agent-vmm/current/agent-vmm host --bundle /opt/agent-vmm/current/guest --instance /var/lib/agent-vmm/instance " +
			"--serial-log /var/lib/agent-vmm/logs/serial.log --operator-socket /var/lib/agent-vmm/operator.sock " +
			"--remote-compute-socket /var/lib/agent-vmm/remote-compute.sock --database /var/lib/agent-vmm/host.sqlite " +
			"--session-directory /var/lib/agent-vmm/sessions --data-worker /opt/agent-vmm/current/agent-vmm-data --disable-personal-mesh-pairing",
		"StandardError=append:/var/log/agent-vmm/host.stderr.log", "WantedBy=multi-user.target",
	} {
		if !strings.Contains(string(unit), "\n"+line+"\n") {
			t.Fatalf("unit lacks %q:\n%s", line, unit)
		}
	}
}

func TestInstallReleasePublishesCompleteRelease(t *testing.T) {
	value := testPaths(t.TempDir())
	contentID := writeTestRelease(t, value.source, "release-a")
	if err := os.MkdirAll(filepath.Join(value.installRoot, ".install-"+contentID+"-interrupted", "guest"), 0o755); err != nil {
		t.Fatal(err)
	}
	installed, err := installRelease(value, value.source)
	if err != nil || installed != contentID {
		t.Fatalf("installRelease() = %q, %v; want %q", installed, err, contentID)
	}
	if err := switchCurrent(value, installed); err != nil {
		t.Fatal(err)
	}
	if release, err := ReleaseID(value.app); err != nil || release != "release-a" || currentContentID(value) != contentID {
		t.Fatalf("current release = %q, %v, content %q", release, err, currentContentID(value))
	}
	if info, err := os.Stat(filepath.Join(value.app, "agent-vmm")); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("installed executable = %v, %v", info, err)
	}
	if data, err := os.ReadFile(filepath.Join(value.app, "guest", "manifest.json")); err != nil || string(data) != `{"release":"release-a"}` {
		t.Fatalf("installed guest manifest = %q, %v", data, err)
	}

	for name, damage := range map[string]func(string){
		"extra":   func(source string) { os.WriteFile(filepath.Join(source, "guest", "extra"), nil, 0o644) },
		"missing": func(source string) { os.Remove(filepath.Join(source, "guest", "manifest.json")) },
		"link":    func(source string) { os.Symlink("agent-vmm", filepath.Join(source, "guest", "link")) },
	} {
		t.Run(name, func(t *testing.T) {
			value := testPaths(t.TempDir())
			writeTestRelease(t, value.source, "release-b")
			damage(value.source)
			if _, err := installRelease(value, value.source); err == nil {
				t.Fatal("damaged release was installed")
			}
			if entries, _ := os.ReadDir(value.installRoot); len(entries) != 0 {
				t.Fatalf("install root retains %v", entries)
			}
		})
	}
}

func TestLinuxUpdateSwitchesCurrentAndPreservesState(t *testing.T) {
	value := testPaths(t.TempDir())
	oldID := writeTestRelease(t, value.source, "release-a")
	if _, err := installRelease(value, value.source); err != nil {
		t.Fatal(err)
	}
	if err := switchCurrent(value, oldID); err != nil {
		t.Fatal(err)
	}
	stateDisk := filepath.Join(value.applicationData, "instance", "state.raw")
	if err := os.MkdirAll(filepath.Dir(stateDisk), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{stateDisk, filepath.Join(value.applicationData, "host.sqlite")} {
		if err := os.WriteFile(path, []byte("state"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := os.Stat(stateDisk)
	prepared := filepath.Join(value.installRoot, ".request-1")
	newID := writeTestRelease(t, prepared, "release-b")

	var steps []string
	actions := linuxUpdateActions(value, "release-b")
	actions.drain = func(paths) error { steps = append(steps, "drain:"+currentContentID(value)); return nil }
	actions.repair = func(paths) error { steps = append(steps, "repair:"+currentContentID(value)); return nil }
	actions.startVM = func(context.Context, paths, string) error { steps = append(steps, "start"); return nil }
	operation, err := runUpdateLifecycleOperation(value, "request-1", "release-b", func() error {
		return updateApplicationWith(value, prepared, "release-b", "request-1", actions)
	})
	if err != nil || operation.Outcome != "succeeded" || operation.Stage != "settled" {
		t.Fatalf("update = %+v, %v", operation, err)
	}
	if want := []string{"drain:" + oldID, "repair:" + newID, "start"}; strings.Join(steps, ",") != strings.Join(want, ",") {
		t.Fatalf("steps = %v, want %v", steps, want)
	}
	if release, _ := ReleaseID(value.app); release != "release-b" {
		t.Fatalf("current release = %q", release)
	}
	for _, retired := range []string{prepared, filepath.Join(value.installRoot, oldID)} {
		if _, err := os.Lstat(retired); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s remains after update: %v", retired, err)
		}
	}
	after, err := os.Stat(stateDisk)
	if err != nil || after.Sys().(*syscall.Stat_t).Ino != before.Sys().(*syscall.Stat_t).Ino {
		t.Fatalf("state disk changed identity: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(value.applicationData, "host.sqlite")); err != nil || string(data) != "state" {
		t.Fatalf("database = %q, %v", data, err)
	}
	if err := validatePreparedUpdate(value, filepath.Join(value.installRoot, newID)); err == nil {
		t.Fatal("an installed release was accepted as a prepared update")
	}
}

func TestReconcileServiceStartsTheUnitOnlyWhenItChanges(t *testing.T) {
	value := testPaths(t.TempDir())
	writeTestRelease(t, value.source, "release-a")
	show := "LoadState=not-found\nActiveState=inactive\nMainPID=0\n"
	var calls []string
	originalOutput := commandOutput
	t.Cleanup(func() { commandOutput = originalOutput })
	commandOutput = func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		call := name + " " + strings.Join(arguments, " ")
		if strings.HasPrefix(call, "systemctl show") {
			return []byte(show), nil
		}
		calls = append(calls, call)
		return nil, nil
	}
	if err := reconcileService(context.Background(), value, false); err != nil {
		t.Fatal(err)
	}
	want := "systemctl daemon-reload,systemctl enable agent-vmm-host.service,systemctl start agent-vmm-host.service"
	if strings.Join(calls, ",") != want {
		t.Fatalf("calls = %v", calls)
	}
	if !value.hostInstalled() {
		t.Fatal("installed Host is not observed")
	}
	if config, err := readServiceConfig(value.serviceConfigFile); err != nil || !config.DisablePairing {
		t.Fatalf("service config = %+v, %v", config, err)
	}
	calls = nil
	show = "LoadState=loaded\nActiveState=active\nMainPID=42\nExecMainCode=0\nExecMainStatus=0\n"
	if err := reconcileService(context.Background(), value, false); err != nil || len(calls) != 0 {
		t.Fatalf("current service was reloaded: calls=%v err=%v", calls, err)
	}
}

func TestParseSystemdServiceObservation(t *testing.T) {
	facts, err := parseSystemdServiceObservation([]byte("LoadState=loaded\nActiveState=activating\nMainPID=0\nExecMainCode=1\nExecMainStatus=3\n"))
	if err != nil || !facts.Loaded || facts.Running || facts.LastExitStatus == nil || *facts.LastExitStatus != 3 {
		t.Fatalf("facts = %+v, %v", facts, err)
	}
	if _, err := parseSystemdServiceObservation([]byte("LoadState=not-found\nActiveState=inactive\nMainPID=0\n")); !errors.Is(err, errHostServiceNotFound) {
		t.Fatalf("missing unit error = %v", err)
	}
}

func TestReconcileServiceDrainsOnlyForChangedRelease(t *testing.T) {
	for _, test := range []struct {
		name               string
		changed, failDrain bool
	}{{name: "same release"}, {name: "changed release", changed: true}, {name: "drain refused", changed: true, failDrain: true}} {
		t.Run(test.name, func(t *testing.T) {
			value := testPaths(t.TempDir())
			oldID := writeTestRelease(t, value.source, "release-a")
			if _, err := installRelease(value, value.source); err != nil {
				t.Fatal(err)
			}
			if err := switchCurrent(value, oldID); err != nil {
				t.Fatal(err)
			}
			if test.changed {
				writeTestRelease(t, value.source, "release-b")
			}
			drainCalls := 0
			value.owner = &serviceOwner{drainVM: func(context.Context, paths, string) error {
				drainCalls++
				if currentContentID(value) != oldID {
					t.Fatal("release switched before VM drain")
				}
				if test.failDrain {
					return errors.New("drain refused")
				}
				return nil
			}}
			previous := commandOutput
			defer func() { commandOutput = previous }()
			var stops int
			commandOutput = func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "systemctl" && args[0] == "show" {
					return []byte("LoadState=loaded\nActiveState=active\nMainPID=42\n"), nil
				}
				if name == "systemctl" && args[0] == "stop" {
					stops++
				}
				return nil, nil
			}
			err := reconcileService(context.Background(), value, false)
			if test.failDrain {
				if err == nil || currentContentID(value) != oldID || stops != 0 {
					t.Fatalf("refused drain changed service: current=%s stops=%d err=%v", currentContentID(value), stops, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantDrain := 0
			if test.changed {
				wantDrain = 1
			}
			if drainCalls != wantDrain || stops != 1 {
				t.Fatalf("drains=%d stops=%d", drainCalls, stops)
			}
		})
	}
}

func TestRemoveServiceStopsGuestBeforeRemovingHost(t *testing.T) {
	for _, test := range []struct {
		name               string
		refuseStop, locked bool
	}{
		{name: "stopped Guest"},
		{name: "stop refused", refuseStop: true},
		{name: "writer remains", locked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := testPaths(t.TempDir())
			writeTestRelease(t, value.source, "release-a")
			installed, err := installRelease(value, value.source)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{value.unit, filepath.Join(value.applicationData, "instance", "state.raw"), filepath.Join(value.applicationData, "credentials")} {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			lock, err := os.OpenFile(filepath.Join(value.applicationData, "instance", "vmm.lock"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			if err := unix.FcntlFlock(lock.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{Type: unix.F_WRLCK}); err != nil {
				t.Fatal(err)
			}
			guestRunning, hostRunning, stopped := true, true, false
			value.owner = &serviceOwner{drainVM: func(context.Context, paths, string) error {
				stopped = true
				if test.refuseStop {
					return errors.New("stop refused")
				}
				guestRunning = false
				if !test.locked {
					return lock.Close()
				}
				return nil
			}}
			previous := commandOutput
			defer func() { commandOutput = previous }()
			commandOutput = func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "systemctl" {
					t.Fatalf("unexpected command %s", name)
				}
				if args[0] == "show" {
					if hostRunning {
						return []byte("LoadState=loaded\nActiveState=active\nMainPID=42\n"), nil
					}
					return []byte("LoadState=loaded\nActiveState=inactive\nMainPID=0\n"), nil
				}
				if guestRunning {
					return nil, errors.New("Host service changed before explicit Guest stop")
				}
				if args[0] == "stop" || args[0] == "disable" {
					hostRunning = false
				}
				return nil, nil
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			err = removeService(ctx, value, test.refuseStop)
			if !stopped {
				t.Fatal("uninstall did not request explicit Guest stop")
			}
			if test.refuseStop || test.locked {
				if err == nil {
					t.Fatal("uninstall succeeded without confirmed writer exit")
				}
				for _, path := range []string{value.unit, filepath.Join(value.installRoot, installed, "agent-vmm"), filepath.Join(value.applicationData, "instance", "state.raw"), filepath.Join(value.applicationData, "credentials")} {
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("failed stop removed %s: %v", path, err)
					}
				}
				if test.refuseStop && !hostRunning {
					t.Fatal("refused VM stop removed the running Host")
				}
				return
			}
			if err != nil || hostRunning {
				t.Fatalf("uninstall error=%v Host running=%t", err, hostRunning)
			}
			for _, path := range []string{value.unit, value.installRoot} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("uninstall retained %s: %v", path, err)
				}
			}
			for _, name := range []string{"instance/state.raw", "credentials"} {
				if data, err := os.ReadFile(filepath.Join(value.applicationData, name)); err != nil || string(data) != "retained" {
					t.Fatalf("uninstall changed %s: %q %v", name, data, err)
				}
			}
		})
	}
}
