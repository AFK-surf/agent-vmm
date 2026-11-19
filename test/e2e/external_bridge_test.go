//go:build darwin && e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExternalSettingsBridgeE2E(t *testing.T) {
	config := os.Getenv("AGENT_VMM_EXTERNAL_E2E_CONFIG")
	if config == "" {
		t.Skip("requires an external settings fixture")
	}
	app := os.Getenv("AGENT_VMM_EXTERNAL_E2E_APP")
	archive, err := url.Parse(os.Getenv("AGENT_VMM_EXTERNAL_E2E_ARCHIVE_URL"))
	if app == "" || err != nil || archive.Scheme != "https" || archive.Host == "" {
		t.Fatal("set AGENT_VMM_EXTERNAL_E2E_APP and AGENT_VMM_EXTERNAL_E2E_ARCHIVE_URL")
	}

	// Keep UNIX socket paths within the Darwin length limit.
	home, err := os.MkdirTemp("/private/tmp", "avm-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	label := "com.afk-surf." + filepath.Base(home) + "-host"
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	target := filepath.Join(home, "Library/Application Support/Agent VMM Host/current/Agent VMM Host.app")
	helper := filepath.Join(target, "Contents/Helpers/agent-vmm-lifecycle")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "/usr/bin/ditto", app, target).CombinedOutput(); err != nil {
		t.Fatalf("copy fixture app: %v: %s", err, output)
	}
	probe := exec.CommandContext(ctx, helper, "status", "--service-type", "agent")
	probe.Env = append(os.Environ(), "AGENT_VMM_E2E_HOME=", "AGENT_VMM_E2E_HOST_LABEL="+label)
	if output, err := probe.CombinedOutput(); err == nil || !strings.Contains(string(output), "requires a canonical AGENT_VMM_E2E_HOME") {
		t.Fatal("fixture requires an e2e-isolation lifecycle helper")
	}

	t.Setenv("AGENT_VMM_E2E_HOME", home)
	t.Setenv("AGENT_VMM_E2E_HOST_LABEL", label)
	t.Cleanup(func() {
		for _, job := range []string{label, strings.TrimSuffix(label, "-host") + "-carrier"} {
			_ = exec.Command("/bin/launchctl", "bootout", domain+"/"+job).Run()
		}
	})
	if _, err := runLifecycle(ctx, helper, []string{"install", "--request-id", "external-settings-install"}, ""); err != nil {
		t.Fatal(err)
	}

	commands := config + ".commands"
	if err := os.MkdirAll(commands, 0700); err != nil {
		t.Fatal(err)
	}
	serial := filepath.Join(home, "Library/Logs/AgentVMM/serial.log")
	if err := writeJSON(config+".host-ready", map[string]string{"archive_url": archive.String(), "serial_log": serial}); err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("settings fixture exceeded fifteen minutes")
		case <-ticker.C:
		}
		if _, err := os.Stat(config + ".stop"); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(commands)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".request") {
				continue
			}
			path := filepath.Join(commands, entry.Name())
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var request struct {
				Args  []string `json:"args"`
				Stdin string   `json:"stdin"`
			}
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Fatal(err)
			}
			result, err := runFixtureCommand(ctx, helper, home, domain, label, request.Args, request.Stdin)
			response := struct {
				Result json.RawMessage `json:"result"`
				Error  string          `json:"error,omitempty"`
			}{Result: result}
			if err != nil {
				response.Error = err.Error()
			}
			if err := writeJSON(strings.TrimSuffix(path, ".request")+".response", response); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func runFixtureCommand(ctx context.Context, helper, home, domain, label string, args []string, input string) (json.RawMessage, error) {
	if len(args) == 0 {
		return nil, errors.New("missing fixture command")
	}
	switch args[0] {
	case "fixture-disconnect":
		// Stop the fixture Host and its connector. The Carrier owns the VM.
		return nil, exec.CommandContext(ctx, "/bin/launchctl", "bootout", domain+"/"+label).Run()
	case "fixture-reconnect":
		plist := filepath.Join(home, "Library/LaunchAgents", label+".plist")
		return nil, exec.CommandContext(ctx, "/bin/launchctl", "bootstrap", domain, plist).Run()
	case "maintenance-status", "status", "operation", "registration-claim", "registration-recover", "registration-enable", "registration-drain", "registration-remove":
		return runLifecycle(ctx, helper, args, input)
	default:
		return nil, fmt.Errorf("unsupported fixture command %q", args[0])
	}
}

func runLifecycle(ctx context.Context, helper string, args []string, input string) (json.RawMessage, error) {
	// The fixture always uses its own launchd domain and application path.
	command := []string{args[0], "--service-type", "agent"}
	for index := 1; index < len(args); index++ {
		arg := args[index]
		name, value, inline := strings.Cut(arg, "=")
		switch name {
		case "--service-user", "--service-app":
			return nil, fmt.Errorf("fixture refuses %s", name)
		case "--service-type":
			if !inline && index+1 < len(args) {
				index++
				value = args[index]
			}
			if value != "agent" && value != "auto" {
				return nil, fmt.Errorf("fixture refuses service type %q", value)
			}
		default:
			command = append(command, arg)
		}
	}
	timeout, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	child := exec.CommandContext(timeout, helper, command...)
	child.Stdin = strings.NewReader(input)
	raw, err := child.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return nil, fmt.Errorf("%s: %w: %s", args[0], err, exit.Stderr)
		}
		return nil, err
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%s returned invalid JSON", args[0])
	}
	return raw, nil
}

func writeJSON(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	pending := path + ".tmp"
	if err := os.WriteFile(pending, raw, 0600); err != nil {
		return err
	}
	return os.Rename(pending, path)
}
