//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

const productionHostLabel = "com.afk-surf.agent-vmm-host"

// installChain runs the e2e-tagged lifecycle helpers of real ad-hoc bundles
// against launchd under an isolated label and service home.
type installChain struct {
	t     *testing.T
	home  string
	label string
}

type installChainStatus struct {
	HostJob     string `json:"hostJob"`
	HostLoaded  bool   `json:"hostLoaded"`
	HostRunning bool   `json:"hostRunning"`
	HostHealthy bool   `json:"hostHealthy"`
	HostPID     int    `json:"hostPid"`
}

type installChainReceipt struct {
	RequestID       string `json:"requestId"`
	TargetReleaseID string `json:"targetReleaseId"`
	Stage           string `json:"stage"`
	Outcome         string `json:"outcome"`
	FailureCode     string `json:"failureCode"`
}

// installChainData identifies the preserved Host data without opening it for
// writing: a test marker, the database schema, and the state disk file.
type installChainData struct {
	marker    string
	schema    int
	stateIno  uint64
	stateBorn syscall.Timespec
}

func TestInstallationChainE2E(t *testing.T) {
	if os.Getenv("AGENT_VMM_E2E_INSTALL_CHAIN") != "1" {
		t.Skip("set AGENT_VMM_E2E_INSTALL_CHAIN=1 to run the real launchd installation chain")
	}
	guest := os.Getenv("AGENT_VMM_E2E_BUNDLE")
	if guest == "" {
		t.Fatal("AGENT_VMM_E2E_BUNDLE is required")
	}
	production := productionFacts(t)

	// Unix socket paths under the service home must stay below 104 bytes.
	home, err := os.MkdirTemp("/private/tmp", "avmm-ic-")
	if err != nil {
		t.Fatal(err)
	}
	chain := &installChain{t: t, home: home, label: fmt.Sprintf("%s.e2e-%d", productionHostLabel, os.Getpid())}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Cleanup(func() {
		_ = exec.Command("/bin/launchctl", "bootout", chain.domain()+"/"+chain.label).Run()
	})

	appA := installChainBundle(t, "AGENT_VMM_E2E_INSTALL_CHAIN_OUTPUT_A", guest, "e2e-install-chain-a")
	appB := installChainBundle(t, "AGENT_VMM_E2E_INSTALL_CHAIN_OUTPUT_B", guest, "e2e-install-chain-b")

	var published struct {
		Outcome string `json:"outcome"`
	}
	chain.decode(chain.lifecycle(appA, nil, nil, 2*time.Minute, "publish-host", "--service-type", "agent", "--source-app", appA), &published)
	if published.Outcome != "published" {
		t.Fatalf("publish-host outcome = %q", published.Outcome)
	}
	chain.lifecycle(appA, nil, nil, 20*time.Minute, "install", "--shared-host", "--service-type", "agent",
		"--request-id", "e2e-install-chain-install", "--disable-personal-mesh-pairing")
	initial := chain.healthy(appA, "e2e-install-chain-a")
	if !chain.loaded() {
		t.Fatalf("isolated launchd job %s is not loaded", chain.label)
	}
	marker := filepath.Join(chain.applicationData(), "e2e-install-chain-marker")
	if err := os.WriteFile(marker, []byte(chain.label), 0o600); err != nil {
		t.Fatal(err)
	}
	data := chain.data()
	if data.schema == 0 {
		t.Fatal("installed Host did not create its database")
	}

	// §3.13: a target that cannot open the local schema is refused before any
	// effect. The current Host keeps running.
	refused, err := chain.maintenance(appB, []string{"AGENT_VMM_E2E_SUPPORTED_SCHEMA_MAX=" + strconv.Itoa(data.schema-1)},
		chain.updateRequest("e2e-install-chain-refused", appB, "e2e-install-chain-b"))
	if err == nil || refused.FailureCode != "target_older_than_data" || refused.Outcome != "failed" || refused.Stage != "accepted" {
		t.Fatalf("downgrade maintenance = %+v, %v; want target_older_than_data refusal", refused, err)
	}
	if after := chain.healthy(appA, "e2e-install-chain-a"); after.HostPID != initial.HostPID {
		t.Fatalf("refused maintenance changed Host PID %d to %d", initial.HostPID, after.HostPID)
	}
	chain.requireData(data)

	updated, err := chain.maintenance(appB, nil, chain.updateRequest("e2e-install-chain-update", appB, "e2e-install-chain-b"))
	if err != nil || updated.Outcome != "succeeded" || updated.Stage != "complete" {
		t.Fatalf("update maintenance = %+v, %v", updated, err)
	}
	if after := chain.healthy(appB, "e2e-install-chain-b"); after.HostPID == initial.HostPID {
		t.Fatalf("update kept the old Host process %d", initial.HostPID)
	}
	chain.requireData(data)

	removed, err := chain.maintenance(appB, nil, map[string]any{"version": 1, "requestId": "e2e-install-chain-uninstall",
		"action": "uninstall", "dataPolicy": "preserve", "targetApp": chain.sharedApp(), "confirmSharedHost": true})
	if err != nil || removed.Outcome != "succeeded" {
		t.Fatalf("uninstall maintenance = %+v, %v", removed, err)
	}
	if chain.loaded() {
		t.Fatalf("isolated launchd job %s is still loaded after uninstall", chain.label)
	}
	for _, path := range []string{chain.sharedApp(), chain.plist()} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("%s remains after uninstall: %v", path, err)
		}
	}
	chain.requireData(data)

	if after := productionFacts(t); after != production {
		t.Fatalf("production Host files changed during the test: before %v, after %v", production, after)
	}
}

// installChainBundle packages an ad-hoc development bundle with the e2e
// lifecycle seam, or uses a package output directory named by env.
func installChainBundle(t *testing.T, env, guest, releaseID string) string {
	t.Helper()
	output := os.Getenv(env)
	if output == "" {
		output = t.TempDir()
		repository, err := filepath.Abs("../..")
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", filepath.Join(repository, "build", "host", "package.sh"))
		command.Dir = repository
		command.Env = append(installChainEnvironment(), "OUTPUT_DIRECTORY="+output, "CODE_SIGN_IDENTITY=-",
			"AGENT_VMM_GUEST_BUNDLE="+guest, "AGENT_VMM_RELEASE_ID="+releaseID, "AGENT_VMM_PACKAGE_PHASE=all", "AGENT_VMM_PACKAGE_E2E=1")
		if result, err := command.CombinedOutput(); err != nil {
			t.Fatalf("package %s: %v\n%s", releaseID, err, readTail(result))
		}
	}
	app, err := filepath.EvalSymlinks(filepath.Join(output, "Agent VMM Host.app"))
	if err != nil {
		t.Fatal(err)
	}
	// A helper without the seam would manage the production label and home.
	info, err := exec.Command("go", "version", "-m", filepath.Join(app, "Contents", "Helpers", "agent-vmm-lifecycle")).Output()
	if err != nil || !strings.Contains(string(info), "-tags=e2e") {
		t.Fatalf("%s lifecycle helper is not an e2e build: %v\n%s", app, err, info)
	}
	return app
}

// installChainEnvironment drops variables that would select another label,
// home, schema or a notarized build.
func installChainEnvironment(drop ...string) []string {
	var result []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if slices.Contains(drop, name) {
			continue
		}
		switch name {
		case "AGENT_VMM_E2E_HOST_LABEL", "AGENT_VMM_E2E_HOME", "AGENT_VMM_E2E_SUPPORTED_SCHEMA_MAX", "NOTARYTOOL_PROFILE":
			continue
		}
		result = append(result, entry)
	}
	return result
}

func (chain *installChain) lifecycle(app string, env []string, stdin []byte, timeout time.Duration, args ...string) []byte {
	chain.t.Helper()
	output, err := chain.run(app, env, stdin, timeout, args...)
	if err != nil {
		chain.t.Fatalf("agent-vmm-lifecycle %v: %v", args, err)
	}
	return output
}

func (chain *installChain) run(app string, env []string, stdin []byte, timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	command := exec.CommandContext(ctx, filepath.Join(app, "Contents", "Helpers", "agent-vmm-lifecycle"), args...)
	command.Env = append(installChainEnvironment("HOME"), "HOME="+chain.home, "AGENT_VMM_E2E_HOME="+chain.home, "AGENT_VMM_E2E_HOST_LABEL="+chain.label)
	command.Env = append(command.Env, env...)
	command.Stdin = bytes.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%w\nstdout: %s\nstderr: %s", err, readTail(stdout.Bytes()), readTail(stderr.Bytes()))
	}
	return stdout.Bytes(), nil
}

func (chain *installChain) decode(data []byte, result any) {
	chain.t.Helper()
	if err := json.Unmarshal(data, result); err != nil {
		chain.t.Fatalf("decode %s: %v", data, err)
	}
}

func (chain *installChain) maintenance(app string, env []string, request map[string]any) (installChainReceipt, error) {
	chain.t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		chain.t.Fatal(err)
	}
	output, runErr := chain.run(app, env, data, 30*time.Minute, "maintenance", "--service-type", "agent")
	var receipt installChainReceipt
	if err := json.Unmarshal(output, &receipt); err != nil {
		chain.t.Fatalf("maintenance receipt %q: %v (%v)", output, err, runErr)
	}
	return receipt, runErr
}

func (chain *installChain) updateRequest(id, app, releaseID string) map[string]any {
	chain.t.Helper()
	archive, err := os.ReadFile(filepath.Join(filepath.Dir(app), "Agent-VMM-Host.zip"))
	if err != nil {
		chain.t.Fatal(err)
	}
	digest := sha256.Sum256(archive)
	return map[string]any{"version": 1, "requestId": id, "action": "update", "dataPolicy": "preserve",
		"targetApp": chain.sharedApp(), "sourceApp": app, "targetReleaseId": releaseID,
		"artifactSha256": hex.EncodeToString(digest[:]), "artifactSize": len(archive), "confirmSharedHost": true}
}

// healthy waits for a running, healthy Host of the expected installed release.
func (chain *installChain) healthy(app, releaseID string) installChainStatus {
	chain.t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var status installChainStatus
		output, err := chain.run(app, nil, nil, time.Minute, "status", "--shared-host", "--service-type", "agent")
		if err == nil {
			chain.decode(output, &status)
		}
		if err == nil && status.HostJob == chain.domain()+"/"+chain.label && status.HostLoaded && status.HostRunning && status.HostHealthy && status.HostPID > 0 {
			var inventory struct {
				ReleaseID string `json:"release_id"`
			}
			raw, readErr := os.ReadFile(filepath.Join(chain.sharedApp(), "Contents", "Resources", "release-inventory.json"))
			if readErr != nil || json.Unmarshal(raw, &inventory) != nil || inventory.ReleaseID != releaseID {
				chain.t.Fatalf("installed release = %q, %v; want %s", inventory.ReleaseID, readErr, releaseID)
			}
			return status
		}
		if time.Now().After(deadline) {
			chain.t.Fatalf("Host did not become healthy: %+v, %v", status, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func (chain *installChain) data() installChainData {
	chain.t.Helper()
	marker, err := os.ReadFile(filepath.Join(chain.applicationData(), "e2e-install-chain-marker"))
	if err != nil {
		chain.t.Fatal(err)
	}
	schema, err := store.ReadSchemaVersion(filepath.Join(chain.applicationData(), "host.sqlite"))
	if err != nil {
		chain.t.Fatal(err)
	}
	var state syscall.Stat_t
	if err := syscall.Stat(filepath.Join(chain.applicationData(), "instance", "state.raw"), &state); err != nil {
		chain.t.Fatal(err)
	}
	return installChainData{marker: string(marker), schema: schema, stateIno: state.Ino, stateBorn: state.Birthtimespec}
}

func (chain *installChain) requireData(want installChainData) {
	chain.t.Helper()
	if got := chain.data(); got != want {
		chain.t.Fatalf("Host data changed: got %+v, want %+v", got, want)
	}
}

func (chain *installChain) loaded() bool {
	return exec.Command("/bin/launchctl", "print", chain.domain()+"/"+chain.label).Run() == nil
}

func (chain *installChain) domain() string { return fmt.Sprintf("gui/%d", os.Getuid()) }

func (chain *installChain) sharedApp() string {
	return filepath.Join(chain.home, "Library", "Application Support", "Agent VMM Host", "current", "Agent VMM Host.app")
}

func (chain *installChain) applicationData() string {
	return filepath.Join(chain.home, "Library", "Application Support", "AgentVMM")
}

func (chain *installChain) plist() string {
	return filepath.Join(chain.home, "Library", "LaunchAgents", chain.label+".plist")
}

// productionFacts records metadata of the production Host paths. It never
// reads their contents or calls launchd for the production label.
func productionFacts(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	var facts string
	for _, path := range []string{
		filepath.Join(home, "Library", "LaunchAgents", productionHostLabel+".plist"),
		filepath.Join(home, "Library", "Application Support", "Agent VMM Maintenance", "state.json"),
		filepath.Join(home, "Library", "Application Support", "Agent VMM Host", "current"),
		filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	} {
		if info, err := os.Lstat(path); err == nil {
			facts += fmt.Sprintf("%s:%d:%d;", path, info.Size(), info.ModTime().UnixNano())
		} else {
			facts += path + ":absent;"
		}
	}
	return facts
}

func readTail(data []byte) string {
	const limit = 8 << 10
	if len(data) > limit {
		data = data[len(data)-limit:]
	}
	return string(data)
}
