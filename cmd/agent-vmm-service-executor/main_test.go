//go:build darwin

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseJobRejectsForgedTargetsAndArguments(t *testing.T) {
	for _, test := range [][]string{{"start"}, {"start", "--job", "other"}, {"start", "--job", "host", "--path", "/tmp/x"}, {"shell", "--job", "host"}} {
		if _, err := parseJob(test[0], test[1:]); err == nil {
			t.Fatalf("accepted %v", test)
		}
	}
}

func TestHostDefinitionMatchesLifecycleCurrentDeclaration(t *testing.T) {
	config := ownerConfig{User: "service-user", Group: "staff", UID: 501, GID: 20, Home: "/Users/service-user"}
	job, _ := resolveJob(config, "host")
	template, err := os.ReadFile(filepath.Join("..", "..", "build", "host", "com.afk-surf.agent-vmm-host.plist.template"))
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.ReplaceAll(string(template), "__HOME__", config.Home)
	expected = strings.ReplaceAll(expected, "__APP__", "/Users/service-user/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app")
	expected = strings.Replace(expected, "  </array>", "  \t\t<string>--disable-personal-mesh-pairing</string>\n\t</array>", 1)
	expected = strings.Replace(expected, "<string>Interactive</string>", "<string>Background</string>", 1)
	expected = strings.Replace(expected, "</dict>\n</plist>", "\t<key>UserName</key>\n\t<string>service-user</string>\n\t<key>GroupName</key>\n\t<string>staff</string>\n</dict>\n</plist>", 1)
	if actual := string(renderPlist(config, job)); actual != expected {
		t.Fatalf("executor Host definition drifted from lifecycle declaration\nactual:\n%s\nexpected:\n%s", actual, expected)
	}
}

func TestHostDefinitionDisablesPersonalMeshPairing(t *testing.T) {
	config := ownerConfig{User: "service-user", Group: "staff", UID: 501, GID: 20, Home: "/Users/service-user"}
	job, _ := resolveJob(config, "host")
	definition := string(renderPlist(config, job))
	if strings.Count(definition, "<string>--disable-personal-mesh-pairing</string>") != 1 {
		t.Fatalf("managed Host definition must disable personal mesh pairing: %s", definition)
	}
}

func TestResolveJobsUsesOnlyConfiguredIdentityAndFixedPaths(t *testing.T) {
	config := ownerConfig{User: "service-user", Group: "staff", UID: 501, GID: 20, Home: "/Users/service-user"}
	host, _ := resolveJob(config, "host")
	runner, _ := resolveJob(config, "runner")
	if host.label != hostLabel || host.program != "/Users/service-user/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/MacOS/agent-vmm-host" {
		t.Fatalf("host=%+v", host)
	}
	if runner.label != runnerLabel || runner.program != "/Users/service-user/.bridge-for-teams/bin/bft-runner" || len(runner.arguments) != 0 {
		t.Fatalf("runner=%+v", runner)
	}
}

func TestRunnerDefinitionRetainsApprovedContinuityPolicy(t *testing.T) {
	config := ownerConfig{User: "svc<&", Group: "staff", UID: 501, GID: 20, Home: "/Users/svc"}
	job, _ := resolveJob(config, "runner")
	definition := string(renderPlist(config, job))
	for _, expected := range []string{"<key>UserName</key><string>svc&lt;&amp;</string>", "<key>GroupName</key><string>staff</string>", runnerLabel, "/Users/svc/.bridge-for-teams/bin/bft-runner", "<key>KeepAlive</key><true/>", "<key>NumberOfFiles</key><integer>65536</integer>", "<key>NumberOfFiles</key><integer>524288</integer>", "<key>EnvironmentVariables</key>", fixedRunnerPath(config.Home), "/Users/svc/.bridge-for-teams/state/runner.out.log", "<key>WorkingDirectory</key><string>/Users/svc/.bridge-for-teams</string>"} {
		if !strings.Contains(definition, expected) {
			t.Fatalf("missing %q in %s", expected, definition)
		}
	}
	if strings.Contains(definition, "<string>root</string>") {
		t.Fatalf("unsafe definition=%s", definition)
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
	oldObserve, oldRun := observeLaunchctl, runLaunchctl
	defer func() { observeLaunchctl, runLaunchctl = oldObserve, oldRun }()
	observeLaunchctl = func(string) (bool, bool, int, error) { return true, true, 42, nil }
	calls := 0
	runLaunchctl = func(...string) error { calls++; return nil }
	if err := startJob(fixedJob{label: hostLabel}); err != nil || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestStartKickstartsLoadedStoppedJob(t *testing.T) {
	oldObserve, oldRun := observeLaunchctl, runLaunchctl
	defer func() { observeLaunchctl, runLaunchctl = oldObserve, oldRun }()
	observeLaunchctl = func(string) (bool, bool, int, error) { return true, false, 0, nil }
	var arguments []string
	runLaunchctl = func(args ...string) error {
		arguments = append([]string(nil), args...)
		return nil
	}
	if err := startJob(fixedJob{label: hostLabel}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(arguments, " ") != "kickstart system/"+hostLabel {
		t.Fatalf("arguments=%v", arguments)
	}
}

func TestHostStopRequiresReleasedStateDiskBeforeBootout(t *testing.T) {
	oldObserve, oldRun, oldBusy := observeLaunchctl, runLaunchctl, stateDiskBusy
	defer func() { observeLaunchctl, runLaunchctl, stateDiskBusy = oldObserve, oldRun, oldBusy }()
	observeLaunchctl = func(string) (bool, bool, int, error) { return true, true, 42, nil }
	stateDiskBusy = func(string) (bool, error) { return true, nil }
	calls := 0
	runLaunchctl = func(...string) error { calls++; return nil }
	err := stopJob(ownerConfig{Home: "/Users/service"}, fixedJob{name: "host", label: hostLabel})
	if err == nil || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestInstallRejectsInvalidProgramBeforeLaunchctl(t *testing.T) {
	oldObserve := observeLaunchctl
	defer func() { observeLaunchctl = oldObserve }()
	calls := 0
	observeLaunchctl = func(string) (bool, bool, int, error) { calls++; return false, false, 0, nil }
	err := installJob(ownerConfig{UID: os.Getuid() + 1, Home: t.TempDir()}, fixedJob{name: "runner", label: runnerLabel, program: "/tmp/forged"})
	if err == nil || calls != 0 {
		t.Fatalf("err=%v launchctl calls=%d", err, calls)
	}
}

func TestStartRejectsReplacedProgramBeforeLaunchctl(t *testing.T) {
	oldObserve := observeLaunchctl
	defer func() { observeLaunchctl = oldObserve }()
	calls := 0
	observeLaunchctl = func(string) (bool, bool, int, error) { calls++; return true, false, 0, nil }
	home := t.TempDir()
	program := filepath.Join(home, "bin", "runner")
	if err := os.MkdirAll(filepath.Dir(program), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(program, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(program), 0o777); err != nil {
		t.Fatal(err)
	}
	err := startConfiguredJob(ownerConfig{UID: os.Getuid(), Home: home}, fixedJob{name: "runner", label: runnerLabel, program: program})
	if err == nil || calls != 0 {
		t.Fatalf("err=%v launchctl calls=%d", err, calls)
	}
}

func TestHostStopBootsOutOnlyAfterReleaseAndConfirmsExit(t *testing.T) {
	oldObserve, oldRun, oldBusy, oldAlive := observeLaunchctl, runLaunchctl, stateDiskBusy, processAlive
	defer func() {
		observeLaunchctl, runLaunchctl, stateDiskBusy, processAlive = oldObserve, oldRun, oldBusy, oldAlive
	}()
	observeLaunchctl = func(string) (bool, bool, int, error) { return true, true, 42, nil }
	checks := 0
	stateDiskBusy = func(string) (bool, error) { checks++; return false, nil }
	processAlive = func(int) bool { return false }
	var arguments []string
	runLaunchctl = func(args ...string) error { arguments = append([]string(nil), args...); return nil }
	if err := stopJob(ownerConfig{Home: "/Users/service"}, fixedJob{name: "host", label: hostLabel}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(arguments, " ") != "bootout system/"+hostLabel || checks != 2 {
		t.Fatalf("args=%v checks=%d", arguments, checks)
	}
}
