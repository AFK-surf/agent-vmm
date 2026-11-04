//go:build linux

package quota

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// TestProjectQuotaIsAHardIndependentWriteBoundary runs in the Linux integration
// job with a disposable project-quota filesystem. Unit jobs skip it.
func TestProjectQuotaIsAHardIndependentWriteBoundary(t *testing.T) {
	if helper := os.Getenv("AGENT_VMM_QUOTA_TEST_HELPER"); helper != "" {
		directory := helper
		if helper == "direct" || helper == "metadata" || helper == "snapshot" {
			directory = "oci"
		}
		file, err := os.Create(filepath.Join(os.Getenv("AGENT_VMM_QUOTA_TEST_ROOT"), directory, "writer-"+helper))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		chunk := make([]byte, 1<<20)
		limit := 2
		if helper == "oci" || helper == "metadata" {
			limit = 32
		} else if helper == "snapshot" {
			limit = 6
		} else if helper == "direct" {
			limit = 1
		}
		for index := 0; index < limit; index++ {
			_, err = file.Write(chunk)
			if err == nil {
				err = file.Sync()
			}
			if err != nil {
				if (helper == "oci" || helper == "metadata" || helper == "direct") && (errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.ENOSPC)) {
					if writeErr := os.WriteFile(os.Getenv("AGENT_VMM_QUOTA_TEST_RESULT"), []byte("quota"), 0o600); writeErr != nil {
						t.Fatal(writeErr)
					}
					return
				}
				t.Fatal(err)
			}
		}
		if helper == "oci" || helper == "metadata" || helper == "direct" || helper == "snapshot" {
			if err := os.WriteFile(os.Getenv("AGENT_VMM_QUOTA_TEST_RESULT"), []byte("complete"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	root := os.Getenv("AGENT_VMM_QUOTA_TEST_ROOT")
	device := os.Getenv("AGENT_VMM_QUOTA_TEST_DEVICE")
	if root == "" || device == "" {
		t.Skip("project-quota integration filesystem is not configured")
	}
	oci := filepath.Join(root, "oci")
	workloads := []string{filepath.Join(root, "workload-1"), filepath.Join(root, "workload-2"), filepath.Join(root, "workload-3")}
	for _, path := range append([]string{oci}, workloads...) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := Assign(oci, 9000); err != nil {
		t.Fatal(err)
	}
	for index, path := range workloads {
		project := uint32(10001 + index)
		if err := Assign(path, project); err != nil {
			t.Fatal(err)
		}
		if err := SetLimit(device, project, 4<<20); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{filepath.Join(oci, "writer-direct"), filepath.Join(oci, "writer-metadata"), filepath.Join(oci, "writer-snapshot"), filepath.Join(oci, "writer-oci")} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	for index, path := range workloads {
		writer := filepath.Join(path, "writer-workload-"+strconv.Itoa(index+1))
		if err := os.Remove(writer); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	unix.Sync()
	if err := unix.Prctl(unix.PR_CAPBSET_DROP, unix.CAP_SYS_RESOURCE, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	runHelper := func(role string) string {
		resultPath := filepath.Join(root, role+".result")
		_ = os.Remove(resultPath)
		command := exec.Command(os.Args[0], "-test.run=^TestProjectQuotaIsAHardIndependentWriteBoundary$")
		command.Env = append(os.Environ(), "AGENT_VMM_QUOTA_TEST_HELPER="+role, "AGENT_VMM_QUOTA_TEST_RESULT="+resultPath)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s writer: %v\n%s", role, err, output)
		}
		result, _ := os.ReadFile(resultPath)
		return string(result)
	}
	usage, err := Usage(device, 9000)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetLimit(device, 9000, usage); err != nil {
		t.Fatal(err)
	}
	if result := runHelper("direct"); result != "quota" {
		t.Fatalf("direct OCI writer result=%q, want hard quota failure", result)
	}
	afterDirect, err := Usage(device, 9000)
	if err != nil || afterDirect > usage+(64<<10) {
		t.Fatalf("idle OCI quota allowed direct growth: before=%d after=%d err=%v", usage, afterDirect, err)
	}
	if err := SetLimit(device, 9000, afterDirect+(2<<20)); err != nil {
		t.Fatal(err)
	}
	if result := runHelper("metadata"); result != "quota" {
		t.Fatalf("metadata writer result=%q, want hard quota failure", result)
	}
	afterMetadata, err := Usage(device, 9000)
	if err != nil || afterMetadata > afterDirect+(3<<20) {
		t.Fatalf("bounded metadata usage=%d baseline=%d err=%v", afterMetadata, afterDirect, err)
	}
	if err := SetLimit(device, 9000, afterMetadata+(8<<20)); err != nil {
		t.Fatal(err)
	}
	// Snapshot metadata and lower-tree materialization consume the OCI project.
	// The product opens this same ceiling only after a physical I hold exists;
	// this bounded growth can consume that I allowance while workload W projects
	// and the E/M floors remain outside project 9000.
	if result := runHelper("snapshot"); result != "complete" {
		t.Fatalf("snapshot growth result=%q, want physically bounded success", result)
	}
	afterSnapshot, err := Usage(device, 9000)
	if err != nil || afterSnapshot > afterMetadata+(7<<20) || afterSnapshot < afterMetadata+(5<<20) {
		t.Fatalf("snapshot usage=%d baseline=%d err=%v", afterSnapshot, afterMetadata, err)
	}
	if err := SetLimit(device, 9000, afterSnapshot+(8<<20)); err != nil {
		t.Fatal(err)
	}
	if result := runHelper("oci"); result != "quota" {
		t.Fatalf("bounded OCI writer result=%q, want hard quota failure", result)
	}
	if bounded, usageErr := Usage(device, 9000); usageErr != nil || bounded > afterSnapshot+(9<<20) {
		t.Fatalf("bounded OCI usage=%d baseline=%d err=%v", bounded, afterSnapshot, usageErr)
	}
	child := filepath.Join(oci, "inherited")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatal(err)
	}
	if id, err := ProjectID(child); err != nil || id != 9000 {
		t.Fatalf("inherited OCI project=%d err=%v", id, err)
	}
	for index := range workloads {
		runHelper("workload-" + strconv.Itoa(index+1))
	}
}

func TestNextQuotaMissingDeviceIsUnavailable(t *testing.T) {
	_, _, err := Next(filepath.Join(t.TempDir(), "missing-device"), 10000)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("missing quota device became empty usage: %v", err)
	}
}
