package reporting

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

type testEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

type caseResult struct {
	Name     string
	Action   string
	Duration float64
}

type runningCase struct {
	startedAt time.Time
}

func Render(repository, bundleDirectory, jsonPath, reportPath string, status int) error {
	manifestBytes, err := os.ReadFile(filepath.Join(bundleDirectory, bundle.ManifestName))
	if err != nil {
		return err
	}
	var manifest bundle.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	manifestHash := sha256.Sum256(manifestBytes)
	cases, peak, coldStart, err := readResults(jsonPath)
	if err != nil {
		return err
	}
	rootfs := manifest.Artifacts["rootfs"]
	result := "PASS"
	if status != 0 {
		result = "FAIL"
	}
	var output strings.Builder
	fmt.Fprintln(&output, "# Agent VMM product E2E report")
	fmt.Fprintf(&output, "\n- Result: `%s`\n", result)
	fmt.Fprintf(&output, "- Revision: `%s`\n", command(repository, "git", "rev-parse", "HEAD"))
	fmt.Fprintf(&output, "- Host: `%s %s`\n", command(repository, "sw_vers", "-productVersion"), runtime.GOARCH)
	fmt.Fprintln(&output, "- VM: 2 vCPU, 4,294,967,296 bytes memory")
	fmt.Fprintf(&output, "- Guest manifest SHA-256: `%s`\n", hex.EncodeToString(manifestHash[:]))
	fmt.Fprintf(&output, "- Rootfs: %d packed bytes, SHA-256 `%s`\n", rootfs.Size, rootfs.SHA256)
	if peak != 0 {
		fmt.Fprintf(&output, "- Parallel E2E peak: %d cases\n", peak)
	}
	if coldStart != 0 {
		fmt.Fprintf(&output, "- Cold VM start: %d ms\n", coldStart)
	}
	fmt.Fprintln(&output, "\n## Case results")
	if len(cases) == 0 {
		fmt.Fprintln(&output, "\nDetailed case timings are produced by `make e2e` in `.build/test-results/e2e/test.json`.")
	} else {
		fmt.Fprintln(&output, "\n| Case | Result | Duration |")
		fmt.Fprintln(&output, "| --- | --- | ---: |")
		for _, item := range cases {
			fmt.Fprintf(&output, "| `%s` | %s | %.2fs |\n", item.Name, strings.ToUpper(item.Action), item.Duration)
		}
	}
	fmt.Fprintln(&output, "\n## Isolation and cleanup assertions")
	fmt.Fprintln(&output, "\nThe suite checks guest network counter propagation, namespace enumeration and volume ownership, mounted-volume deletion guards, project quotas, bounded-log truncation, default cross-namespace denial, selector/port-limited Link rules, Link revocation, filtered public egress, build context/cache/secret isolation, cancellation cleanup, host policy replay and snapshot-first event reconnection after VM restart, forced-stop UNKNOWN/stale semantics, revision-bound and interrupted cleanup recovery, and a final residual namespace scan. The report contains no environment values, build context, secret bytes, or full workload logs.")
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(reportPath, []byte(output.String()), 0o644)
}

func readResults(path string) ([]caseResult, int, int64, error) {
	if path == "" {
		return nil, 0, 0, nil
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	defer file.Close()
	var results []caseResult
	running := make(map[string]runningCase)
	terminal := make(map[string]bool)
	active := 0
	peak := 0
	var coldStart int64
	var latestEvent time.Time
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		var event testEvent
		if json.Unmarshal(scanner.Bytes(), &event) != nil || event.Test == "" {
			continue
		}
		if event.Time.After(latestEvent) {
			latestEvent = event.Time
		}
		if strings.Count(event.Test, "/") >= 1 && event.Action == "run" {
			running[event.Test] = runningCase{startedAt: event.Time}
		}
		if event.Action == "output" {
			if value, ok := metric(event.Output, "E2E_PARALLEL_PEAK="); ok {
				peak = int(value)
			}
			if coldStart == 0 && event.Test == "TestE2E/boot" {
				if value, ok := metric(event.Output, "E2E_COLD_START_MS="); ok {
					coldStart = value
				}
			}
		}
		if strings.HasPrefix(event.Test, "TestE2E/parallel/") {
			switch event.Action {
			case "cont":
				active++
				peak = max(peak, active)
			case "pass", "fail", "skip":
				active--
			}
		}
		if strings.Count(event.Test, "/") >= 1 && (event.Action == "pass" || event.Action == "fail" || event.Action == "skip") {
			terminal[event.Test] = true
			if event.Test != "TestE2E/parallel" {
				results = append(results, caseResult{Name: strings.TrimPrefix(event.Test, "TestE2E/"), Action: event.Action, Duration: event.Elapsed})
			}
		}
	}
	for name, item := range running {
		if terminal[name] {
			continue
		}
		duration := 0.0
		if !item.startedAt.IsZero() && !latestEvent.IsZero() && latestEvent.After(item.startedAt) {
			duration = latestEvent.Sub(item.startedAt).Seconds()
		}
		results = append(results, caseResult{Name: strings.TrimPrefix(name, "TestE2E/"), Action: "interrupted", Duration: duration})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return results, peak, coldStart, scanner.Err()
}

func metric(output, marker string) (int64, bool) {
	index := strings.Index(output, marker)
	if index < 0 {
		return 0, false
	}
	value := strings.TrimSpace(output[index+len(marker):])
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}

func command(directory, name string, arguments ...string) string {
	cmd := exec.Command(name, arguments...)
	cmd.Dir = directory
	value, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(value))
}
