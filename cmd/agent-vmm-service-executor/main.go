//go:build darwin

// agent-vmm-service-executor is an administrator-invoked, one-shot launchd
// adapter. It never executes the service programs and accepts no paths,
// templates, environment variables, labels, or numeric identities.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	configPath  = "/Library/Application Support/AgentVMM/service-owner.json"
	hostLabel   = "com.afk-surf.agent-vmm-host"
	runnerLabel = "com.bridgeforteams.runner"
)

type ownerConfig struct {
	User  string `json:"user"`
	Group string `json:"group"`
	UID   int    `json:"uid"`
	GID   int    `json:"gid"`
	Home  string `json:"home"`
}
type fixedJob struct {
	name                                  string
	label, program, plist, stdout, stderr string
	arguments                             []string
}

var runLaunchctl = func(args ...string) error {
	command := exec.Command("/bin/launchctl", args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}
var observeLaunchctl = func(label string) (bool, bool, int, error) {
	output, err := exec.Command("/bin/launchctl", "print", "system/"+label).CombinedOutput()
	if err != nil {
		text := string(output)
		if strings.Contains(text, "Could not find service") || strings.Contains(text, "not found") {
			return false, false, 0, nil
		}
		return false, false, 0, err
	}
	text := string(output)
	running := strings.Contains(text, "state = running")
	pid := 0
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "pid = ") {
			pid, _ = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "pid = ")))
		}
	}
	return true, running, pid, nil
}
var stateDiskBusy = func(path string) (bool, error) {
	command := exec.Command("/usr/sbin/lsof", "-Fn", "--", path)
	output, err := command.Output()
	if err == nil {
		return len(output) != 0, nil
	}
	if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 && len(exit.Stderr) == 0 {
		return false, nil
	}
	return false, err
}
var processAlive = func(pid int) bool { err := unix.Kill(pid, 0); return err == nil || errors.Is(err, unix.EPERM) }

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if os.Geteuid() != 0 {
		return errors.New("administrator access is required")
	}
	if len(args) == 0 {
		return errors.New("usage: agent-vmm-service-executor configure --service-user USER | install|start|stop|remove|status --job host|runner")
	}
	if err := validateProtectedExecutable(os.Executable); err != nil {
		return err
	}
	if args[0] == "configure" {
		return configure(args[1:])
	}
	config, err := readConfig(configPath)
	if err != nil {
		return err
	}
	jobName, err := parseJob(args[0], args[1:])
	if err != nil {
		return err
	}
	job, err := resolveJob(config, jobName)
	if err != nil {
		return err
	}
	switch args[0] {
	case "install":
		return installJob(config, job)
	case "start":
		return startConfiguredJob(config, job)
	case "stop":
		return stopJob(config, job)
	case "remove":
		if err := stopJob(config, job); err != nil {
			return err
		}
		err := os.Remove(job.plist)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	case "status":
		return runLaunchctl("print", "system/"+job.label)
	default:
		return errors.New("action must be install, start, stop, remove, or status")
	}
}

func configure(args []string) error {
	flags := flag.NewFlagSet("configure", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	name := flags.String("service-user", "", "non-root service user")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *name == "" {
		return errors.New("configure requires --service-user USER")
	}
	account, err := user.Lookup(*name)
	if err != nil {
		return errors.New("service user does not exist")
	}
	uid, e1 := strconv.Atoi(account.Uid)
	gid, e2 := strconv.Atoi(account.Gid)
	if e1 != nil || e2 != nil || uid <= 0 || gid <= 0 || !filepath.IsAbs(account.HomeDir) {
		return errors.New("service user must have non-root numeric identity and absolute home")
	}
	group, err := user.LookupGroupId(account.Gid)
	if err != nil || group.Name == "" {
		return errors.New("service group does not exist")
	}
	config := ownerConfig{User: account.Username, Group: group.Name, UID: uid, GID: gid, Home: filepath.Clean(account.HomeDir)}
	if err := validateServiceHome(config); err != nil {
		return err
	}
	configDirectory := filepath.Dir(configPath)
	if err := ensureProtectedConfigDirectory(configDirectory); err != nil {
		return err
	}
	data, _ := json.Marshal(config)
	data = append(data, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(configPath), ".service-owner-*")
	if err != nil {
		return err
	}
	nameTmp := temporary.Name()
	defer os.Remove(nameTmp)
	if temporary.Chmod(0o600) != nil {
		temporary.Close()
		return errors.New("config mode could not be set")
	}
	if _, err = temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err = temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	if err = os.Rename(nameTmp, configPath); err != nil {
		return err
	}
	return validateProtectedPath(configPath, false)
}

func parseJob(action string, args []string) (string, error) {
	if action != "install" && action != "start" && action != "stop" && action != "remove" && action != "status" {
		return "", errors.New("unknown action")
	}
	if len(args) != 2 || args[0] != "--job" || args[1] != "host" && args[1] != "runner" {
		return "", errors.New(action + " requires --job host|runner")
	}
	return args[1], nil
}

func readConfig(path string) (ownerConfig, error) {
	if err := validateProtectedPath(path, false); err != nil {
		return ownerConfig{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ownerConfig{}, err
	}
	var value ownerConfig
	if json.Unmarshal(data, &value) != nil || value.UID <= 0 || value.GID <= 0 || value.User == "" || value.Group == "" || !filepath.IsAbs(value.Home) {
		return ownerConfig{}, errors.New("service owner config is invalid")
	}
	account, err := user.Lookup(value.User)
	if err != nil || account.Uid != strconv.Itoa(value.UID) || account.Gid != strconv.Itoa(value.GID) || filepath.Clean(account.HomeDir) != value.Home {
		return ownerConfig{}, errors.New("service owner config no longer matches the OS account")
	}
	group, err := user.LookupGroupId(strconv.Itoa(value.GID))
	if err != nil || group.Name != value.Group {
		return ownerConfig{}, errors.New("service owner group no longer matches the OS account")
	}
	return value, nil
}

func resolveJob(config ownerConfig, name string) (fixedJob, error) {
	logs := filepath.Join(config.Home, "Library", "Logs")
	if name == "host" {
		program := filepath.Join(config.Home, "Library", "Application Support", "Agent VMM Host", "current", "Agent VMM Host.app", "Contents", "MacOS", "agent-vmm-host")
		app := filepath.Dir(filepath.Dir(filepath.Dir(program)))
		arguments := []string{"--cpus", "8", "--memory", "12884901888", "--state-size", "68719476736", "--bundle", filepath.Join(app, "Contents", "Resources", "Guest"), "--instance", filepath.Join(config.Home, "Library", "Application Support", "AgentVMM", "instance"), "--serial-log", filepath.Join(logs, "AgentVMM", "serial.log"), "--operator-socket", filepath.Join(config.Home, "Library", "Application Support", "AgentVMM", "operator.sock"), "--remote-compute-socket", filepath.Join(config.Home, "Library", "Application Support", "AgentVMM", "remote-compute.sock"), "--database", filepath.Join(config.Home, "Library", "Application Support", "AgentVMM", "host.sqlite"), "--session-directory", filepath.Join(config.Home, "Library", "Application Support", "AgentVMM", "sessions"), "--data-worker", filepath.Join(app, "Contents", "Helpers", "agent-vmm-data"), "--disable-personal-mesh-pairing"}
		return fixedJob{name: "host", label: hostLabel, program: program, plist: "/Library/LaunchDaemons/" + hostLabel + ".plist", stdout: filepath.Join(logs, "AgentVMM", "host.stdout.log"), stderr: filepath.Join(logs, "AgentVMM", "host.stderr.log"), arguments: arguments}, nil
	}
	program := filepath.Join(config.Home, ".bridge-for-teams", "bin", "bft-runner")
	state := filepath.Join(config.Home, ".bridge-for-teams", "state")
	return fixedJob{name: "runner", label: runnerLabel, program: program, plist: "/Library/LaunchDaemons/" + runnerLabel + ".plist", stdout: filepath.Join(state, "runner.out.log"), stderr: filepath.Join(state, "runner.err.log")}, nil
}

func installJob(config ownerConfig, job fixedJob) error {
	// The executable is service-user managed, so root validates it as a regular,
	// non-symlink file but never executes it. launchd drops identity first.
	if err := validateServiceProgram(config, job.program); err != nil {
		return err
	}
	loaded, _, _, observeErr := observeLaunchctl(job.label)
	if observeErr != nil {
		return errors.New("fixed job state could not be read")
	}
	if loaded {
		return errors.New("fixed job must be stopped before its definition is replaced")
	}
	if err := validateProtectedPath("/Library/LaunchDaemons", false); err != nil {
		return errors.New("launchd definition directory is not protected")
	}
	definition := renderPlist(config, job)
	temporary, err := os.CreateTemp("/Library/LaunchDaemons", ".agent-vmm-job-*")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
	if temporary.Chmod(0o644) != nil {
		temporary.Close()
		return errors.New("service definition mode could not be set")
	}
	if _, err = temporary.Write(definition); err != nil {
		temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	if err = os.Rename(path, job.plist); err != nil {
		return err
	}
	return runLaunchctl("bootstrap", "system", job.plist)
}

func renderPlist(config ownerConfig, job fixedJob) []byte {
	if job.name == "host" {
		return renderHostPlist(config, job)
	}
	values := append([]string{job.program}, job.arguments...)
	var arguments strings.Builder
	for _, value := range values {
		arguments.WriteString("<string>" + xmlEscape(value) + "</string>")
	}
	path := fixedRunnerPath(config.Home)
	working := filepath.Join(config.Home, ".bridge-for-teams")
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>Label</key><string>%s</string><key>EnvironmentVariables</key><dict><key>HOME</key><string>%s</string><key>PATH</key><string>%s</string></dict><key>ProgramArguments</key><array>%s</array><key>UserName</key><string>%s</string><key>GroupName</key><string>%s</string><key>WorkingDirectory</key><string>%s</string><key>StandardOutPath</key><string>%s</string><key>StandardErrorPath</key><string>%s</string><key>SoftResourceLimits</key><dict><key>NumberOfFiles</key><integer>65536</integer></dict><key>HardResourceLimits</key><dict><key>NumberOfFiles</key><integer>524288</integer></dict><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>`, job.label, xmlEscape(config.Home), xmlEscape(path), arguments.String(), xmlEscape(config.User), xmlEscape(config.Group), xmlEscape(working), xmlEscape(job.stdout), xmlEscape(job.stderr)))
}

func fixedRunnerPath(home string) string {
	parts := []string{filepath.Join(home, ".bridge-for-teams", "bin"), filepath.Join(home, ".local", "bin"), filepath.Join(home, ".npm-global", "bin"), filepath.Join(home, ".yarn", "bin"), filepath.Join(home, ".bun", "bin"), filepath.Join(home, ".volta", "bin"), filepath.Join(home, ".asdf", "shims"), "/opt/homebrew/bin", "/Applications/LibreOffice.app/Contents/MacOS", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}
	return strings.Join(parts, ":")
}

func renderHostPlist(config ownerConfig, job fixedJob) []byte {
	values := append([]string{job.program}, job.arguments...)
	var arguments strings.Builder
	for _, value := range values {
		arguments.WriteString("    <string>" + xmlEscape(value) + "</string>\n")
	}
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>ProgramArguments</key>
  <array>
%s  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ProcessType</key>
  <string>Background</string>
  <key>ExitTimeOut</key>
  <integer>90</integer>
  <key>StandardOutPath</key>
  <string>%s</string>
  <key>StandardErrorPath</key>
  <string>%s</string>
	<key>UserName</key>
	<string>%s</string>
	<key>GroupName</key>
	<string>%s</string>
</dict>
</plist>
`, job.label, arguments.String(), xmlEscape(job.stdout), xmlEscape(job.stderr), xmlEscape(config.User), xmlEscape(config.Group)))
}

func xmlEscape(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;")
	return replacer.Replace(value)
}

func validateServiceProgram(config ownerConfig, program string) error {
	relative, err := filepath.Rel(config.Home, program)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("fixed service program is outside the configured home")
	}
	current := filepath.Clean(config.Home)
	parts := append([]string{"."}, strings.Split(relative, string(filepath.Separator))...)
	for index, part := range parts {
		if part != "." {
			current = filepath.Join(current, part)
		}
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return errors.New("fixed service program path is unavailable")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 && int(stat.Uid) != config.UID || info.Mode()&0o022 != 0 || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("fixed service program path is controlled by another identity (component=%s owner=%d mode=%s expected=%d)", current, stat.Uid, info.Mode(), config.UID)
		}
		if index == len(parts)-1 && (!info.Mode().IsRegular() || info.Mode()&0o111 == 0 || int(stat.Uid) != config.UID) {
			return errors.New("fixed service program is not an executable owned by the configured service user")
		}
	}
	return nil
}

func validateServiceHome(config ownerConfig) error {
	for current := filepath.Clean(config.Home); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return errors.New("service home path is unavailable")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 && int(stat.Uid) != config.UID || info.Mode()&0o022 != 0 || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("service home path is controlled by another identity")
		}
		if current == "/" {
			break
		}
	}
	return nil
}

func startJob(job fixedJob) error {
	loaded, running, _, err := observeLaunchctl(job.label)
	if err != nil || !loaded {
		return errors.New("fixed job is not loaded; install it before start")
	}
	if running {
		return nil
	}
	return runLaunchctl("kickstart", "system/"+job.label)
}

func startConfiguredJob(config ownerConfig, job fixedJob) error {
	if err := validateServiceProgram(config, job.program); err != nil {
		return err
	}
	return startJob(job)
}

func stopJob(config ownerConfig, job fixedJob) error {
	loaded, _, pid, err := observeLaunchctl(job.label)
	if err != nil {
		return errors.New("fixed job state could not be read")
	}
	if !loaded {
		return nil
	}
	stateDisk := ""
	if job.name == "host" {
		stateDisk = filepath.Join(config.Home, "Library", "Application Support", "AgentVMM", "instance", "state.raw")
		busy, checkErr := stateDiskBusy(stateDisk)
		if checkErr != nil {
			return errors.New("state disk attachment could not be checked")
		}
		if busy {
			return errors.New("VM state disk is still attached; complete the service-user VM stop first")
		}
	}
	if err := runLaunchctl("bootout", "system/"+job.label); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for pid > 0 && processAlive(pid) {
		if time.Now().After(deadline) {
			return errors.New("fixed service process did not exit")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if stateDisk != "" {
		busy, checkErr := stateDiskBusy(stateDisk)
		if checkErr != nil || busy {
			return errors.New("VM state disk was not released after service stop")
		}
	}
	return nil
}

func tolerateMissing(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "No such process") {
		return nil
	}
	return err
}

func validateProtectedExecutable(resolve func() (string, error)) error {
	path, err := resolve()
	if err != nil {
		return err
	}
	return validateProtectedPath(path, true)
}

func ensureProtectedConfigDirectory(path string) error {
	if err := validateProtectedPath(filepath.Dir(path), false); err != nil {
		return errors.New("service owner config parent is not protected")
	}
	if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := validateProtectedPath(path, false); err != nil {
		return errors.New("service owner config directory is not protected")
	}
	return nil
}
func validateProtectedPath(path string, executable bool) error {
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || info.Mode()&0o022 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("root-owned path is replaceable")
	}
	if executable && info.Mode()&0o111 == 0 {
		return errors.New("executor is not executable")
	}
	for parent := filepath.Dir(clean); parent != "/"; parent = filepath.Dir(parent) {
		info, err = os.Lstat(parent)
		if err != nil {
			return err
		}
		stat, ok = info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 || info.Mode()&0o022 != 0 || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("root-owned parent is replaceable")
		}
	}
	return nil
}
